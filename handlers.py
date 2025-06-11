import re
import os
import json
import base64
import zipfile
import tempfile
import asyncio
import traceback
from utils import (
    logger, get_db_connection, check_registration, parse_student_data, save_to_db,
    show_student_rating, format_ratings_table, REPLY_KEYBOARD_MARKUP,
    CANCEL_KEYBOARD_MARKUP, INLINE_KEYBOARD_MARKUP, validate_student_id, validate_group_format, validate_student_group, handle_telegram_timeout,
    send_notification_to_users, get_week_type, set_week_type_settings
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from archive_manager import CourseWorkArchiveManager
from datetime import datetime, timedelta

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ КЛАВИАТУРЫ РАСПИСАНИЯ ---
def build_schedule_keyboard(schedule, group, subgroup, week_type, day_type, date_obj=None):
    from telegram import InlineKeyboardButton
    from datetime import datetime
    lessons_data = []
    lesson_buttons = []
    inactive_count = 0
    if not date_obj:
        date_obj = datetime.now()
    week_type_text = "верхняя" if week_type == "UP" else "нижняя"
    if day_type == 'today':
        message = f"\U0001F4C5 Расписание на сегодня ({date_obj.strftime('%d.%m.%Y')})\n"
    else:
        message = f"\U0001F4C5 Расписание на завтра ({date_obj.strftime('%d.%m.%Y')})\n"
    message += f"Группа: {group}, Подгруппа: {subgroup}\n"
    message += f"Неделя: {week_type_text}\n\n"
    for i, lesson in enumerate(schedule, 1):
        if lesson and lesson.strip():
            try:
                data = json.loads(lesson)
                data['number'] = i
                lessons_data.append(data)
                if data.get('type') == 'inactive':
                    inactive_count += 1
                elif data.get('type') == 'window':
                    lesson_buttons.append([InlineKeyboardButton(f"{i}. 🪟 Форточка", callback_data=f'lessoninfo_window_{day_type}_{i}')])
                else:
                    discipline = data.get('discipline', data.get('description', 'Пара'))
                    auditory = data.get('auditory', '')
                    btn_text = f"{i}. {discipline}"
                    if auditory:
                        btn_text += f" ({auditory})"
                    lesson_buttons.append([InlineKeyboardButton(btn_text, callback_data=f'lessoninfo_{day_type}_{i}')])
            except Exception:
                lesson_buttons.append([InlineKeyboardButton(f"{i}. {lesson}", callback_data=f'lessoninfo_unknown_{day_type}_{i}')])
        else:
            inactive_count += 1
    if inactive_count == 5:
        message += "Выходной\n"
        lesson_buttons = []
    else:
        lesson_buttons.append([InlineKeyboardButton("« Назад", callback_data='schedule')])
    return message, lesson_buttons, lessons_data

@handle_telegram_timeout()
async def handle_message(update, context):
    text = update.message.text.strip()
    user_id = update.effective_user.id
    logger.info(f"Получено сообщение от пользователя {user_id}: {text}")

    if context.user_data.get('awaiting_admin_comment'):
        comment = text.strip()
        params = context.user_data.get('edit_comment')
        if not params:
            await update.message.reply_text(
                "Ошибка: параметры для комментария не найдены.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            context.user_data.pop('awaiting_admin_comment', None)
            return
        subgroup = params['subgroup']
        week_type = params['week_type']
        day = params['day']
        slot = params['slot']
        telegram_id = str(update.effective_user.id)
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await update.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            group = result[0]
            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            field = f"{day}_{slot}"
            safe_field = '"' + field.replace('"', '""') + '"'
            cursor.execute(f'SELECT {safe_field} FROM raspisanie WHERE group_full_name=?', (group_full_name,))
            row = cursor.fetchone()
            if row and row[0]:
                try:
                    data = json.loads(row[0])
                except Exception:
                    data = {}
            else:
                data = {}
            data['admin_comment'] = comment
            cursor.execute(f'UPDATE raspisanie SET {safe_field}=? WHERE group_full_name=?', (json.dumps(data, ensure_ascii=False), group_full_name))
            conn.commit()
            await update.message.reply_text(
                f"Комментарий успешно {'удален' if not comment else 'обновлен'}!",
                reply_markup=None
            )
        except Exception as e:
            logger.error(f"Ошибка при сохранении admin_comment: {e}")
            await update.message.reply_text(
                "Ошибка при сохранении комментария.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            if 'conn' in locals():
                conn.close()
        # Очистить флаги
        context.user_data.pop('awaiting_admin_comment', None)
        context.user_data.pop('edit_comment', None)
        # Вернуть к меню редактирования этого слота
        # Эмулируем callback для edit_slot
        class FakeCallbackQuery:
            def __init__(self, user_id, message, subgroup, week_type, day, slot):
                self.data = f'edit_slot_{subgroup}_{week_type}_{day}_{slot}'
                self.message = update.message
                self.from_user = update.effective_user
            async def answer(self):
                pass
        fake_query = FakeCallbackQuery(user_id, update.message, subgroup, week_type, day, slot)
        fake_update = type('FakeUpdate', (), {'callback_query': fake_query, 'effective_user': update.effective_user})()
        await handle_inline_buttons(fake_update, context)
        return

    if text == '🏠 Главное меню':
        logger.info(f"Нажата кнопка '🏠 Главное меню' пользователем {user_id}")
        context.user_data.clear()
        await update.message.reply_text(
            "Вы вернулись в главное меню! Выберите опцию:",
            reply_markup=INLINE_KEYBOARD_MARKUP
        )
        return

    telegram_id = str(update.effective_user.id)
    
    if context.user_data.get('awaiting_student_id'):
        student_id = text
        # Проверяем валидность student_id
        is_valid, error_message = validate_student_id(student_id)
        if not is_valid:
            await update.message.reply_text(
                f"Ошибка: {error_message}\n\nПожалуйста, введите корректный номер студенческого билета или отмените действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT name, student_group FROM students WHERE student_id=?', (student_id,))
        existing_student = cursor.fetchone()
        if existing_student:
            telegram_id = str(update.effective_user.id)
            # Просто обновляем telegram_id в students и course_works
            cursor.execute('UPDATE students SET telegram_id=? WHERE student_id=?', (telegram_id, student_id))
            cursor.execute('UPDATE course_works SET telegram_id=? WHERE student_id=?', (telegram_id, student_id))
            conn.commit()
            conn.close()
            context.user_data.clear()
            await update.message.reply_text(
                f"Ваш Telegram ID был успешно привязан к существующему студенту {existing_student[0]} (группа: {existing_student[1]}).",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn.close()
        context.user_data['temp_student_id'] = student_id
        context.user_data['awaiting_student_id'] = False
        context.user_data['awaiting_group'] = True
        # Получаем список уникальных групп из базы
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT student_group FROM students WHERE student_group IS NOT NULL AND student_group != ""')
        groups = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        # Формируем клавиатуру
        group_keyboard = [[g] for g in sorted(groups)] if groups else []
        from telegram import ReplyKeyboardMarkup
        reply_markup = ReplyKeyboardMarkup(group_keyboard, resize_keyboard=True, one_time_keyboard=True) if group_keyboard else CANCEL_KEYBOARD_MARKUP
        await update.message.reply_text(
            "Введите группу БОЛЬШИМИ РУССКИМИ БУКВАМИ, например ПМР-231, либо выберите из групп, которые уже зарегистрированы в боте:",
            reply_markup=reply_markup
        )
        return

    if context.user_data.get('awaiting_group'):
        student_group = text.upper()
        student_id = context.user_data.get('temp_student_id')
        telegram_id = str(update.effective_user.id)
        
        # Сначала проверяем формат группы
        is_valid_format, format_error = validate_group_format(student_group)
        if not is_valid_format:
            context.user_data.clear()  # Сбрасываем состояние
            context.user_data['awaiting_student_id'] = True  # Возвращаем к вводу student_id
            await update.message.reply_text(
                f"Ошибка: {format_error}\n\nПожалуйста, введите номер студенческого билета или отмените действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return

        # Проверяем существование студента и его группу
        name, grades, subjects, course_works = parse_student_data(student_id)
        if name == "Unknown":
            context.user_data.clear()  # Сбрасываем состояние
            context.user_data['awaiting_student_id'] = True  # Возвращаем к вводу student_id
            await update.message.reply_text(
                "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или сервер VUZ2 не отвечает. Попробуйте позже.\n\nПожалуйста, введите номер студенческого билета или отмените действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return
            
        # Проверяем соответствие студента группе
        is_valid_group, group_error = validate_student_group(student_id, student_group)
        if not is_valid_group:
            context.user_data.clear()  # Сбрасываем состояние
            context.user_data['awaiting_student_id'] = True  # Возвращаем к вводу student_id
            await update.message.reply_text(
                f"Ошибка: {group_error}\n\nПожалуйста, введите номер студенческого билета или отмените действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return

        conn = get_db_connection()
        cursor = conn.cursor()
        # Проверяем, есть ли студент с таким student_id
        cursor.execute('SELECT name FROM students WHERE student_id=?', (student_id,))
        existing_student = cursor.fetchone()
        if existing_student:
            # Просто обновляем telegram_id в students и course_works
            cursor.execute('UPDATE students SET telegram_id=? WHERE student_id=?', (telegram_id, student_id))
            cursor.execute('UPDATE course_works SET telegram_id=? WHERE student_id=?', (telegram_id, student_id))
            conn.commit()
            conn.close()
            context.user_data.clear()
            await update.message.reply_text(
                f"Ваш Telegram ID был успешно привязан к существующему студенту {existing_student[0]}.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        # Сообщаем пользователю о начале процесса
        if not context.user_data.get('registration_in_progress'):
            context.user_data['registration_in_progress'] = True
            await update.message.reply_text(
                "Идет регистрация, пожалуйста, подождите... Это может занять до минуты.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        # Парсим только если еще не парсили для этого студента в этой сессии
        if 'temp_parsed_student_id' in context.user_data and context.user_data['temp_parsed_student_id'] == student_id:
            name = context.user_data['temp_name']
            grades = context.user_data['temp_grades']
            subjects = context.user_data['temp_subjects']
            course_works = context.user_data['temp_course_works']
        else:
            name, grades, subjects, course_works = parse_student_data(student_id)
            context.user_data['temp_name'] = name
            context.user_data['temp_grades'] = grades
            context.user_data['temp_subjects'] = subjects
            context.user_data['temp_course_works'] = course_works
            context.user_data['temp_parsed_student_id'] = student_id
        if name == "Unknown":
            context.user_data.pop('registration_in_progress', None)
            await update.message.reply_text(
                "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или сервер VUZ2 не отвечает. Попробуйте позже.\n\nВы можете отменить действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            context.user_data.clear()
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT COUNT(*) FROM students WHERE student_group=?', (student_group,))
            group_exists = cursor.fetchone()[0] > 0
            is_admin = not group_exists

            save_to_db(
                student_id=student_id,
                name=name,
                grades=grades,
                subjects=subjects,
                telegram_id=telegram_id,
                student_group=student_group,
                is_admin=is_admin
            )
            # Сохраняем курсовые работы
            for cw in course_works:
                from utils import save_course_work_to_db
                save_course_work_to_db(
                    student_id=student_id,
                    name=name,
                    telegram_id=telegram_id,
                    student_group=student_group,
                    discipline=cw.get('discipline'),
                    file_path=cw.get('file_path'),
                    semester=cw.get('semester')
                )
            context.user_data.pop('registration_in_progress', None)
            context.user_data.clear()
            
            # Уведомляем суперадминов о новом пользователе
            notification_text = (
                "🆕 <b>Новый пользователь в боте!</b>\n\n"
                f"• Имя: {name}\n"
                f"• Группа: {student_group}\n"
                f"• Student ID: {student_id}\n"
                f"• Telegram ID: {telegram_id}"
            )
            await notify_superadmins(context.application, notification_text)
            
            await update.message.reply_text(
                f"Регистрация завершена! Вы {'стали администратором' if is_admin else 'добавлены в'} группу {student_group}.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        except Exception as e:
            context.user_data.pop('registration_in_progress', None)
            logger.error(f"Error saving group: {e}")
            await update.message.reply_text("Произошла ошибка при регистрации.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()
        return

    if context.user_data.get('awaiting_admin_student_id'):
        student_id = text
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT student_group, is_admin FROM students WHERE telegram_id=?', (telegram_id,))
            admin_data = cursor.fetchone()
            if not admin_data or not admin_data[1]:
                await update.message.reply_text(
                    "Только администратор группы может выполнять это действие.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                context.user_data.clear()
                return
                
            admin_group = admin_data[0]
            
            cursor.execute('SELECT name, student_group FROM students WHERE student_id=?', (student_id,))
            student_data = cursor.fetchone()
            
            if not student_data:
                name, grades, subjects, course_works = parse_student_data(student_id, telegram_id="added by admin", student_group=admin_group)
                if name == "Unknown":
                    await update.message.reply_text(
                        "Не удалось получить данные по номеру студенческого билета. Проверьте правильность введенного номера. Возможно сервер VUZ2 не отвечает. Попробуйте позже.\n\nВы можете отменить действие командой /cancel.",
                        reply_markup=CANCEL_KEYBOARD_MARKUP
                    )
                    return
                save_to_db(student_id, name, grades, subjects, telegram_id="added by admin", student_group=admin_group)
                await update.message.reply_text(
                    f"Студент {name} добавлен в группу {admin_group}!",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
            else:
                name, student_group = student_data
                if student_group != admin_group:
                    await update.message.reply_text(
                        f"Студент {name} находится в другой группе ({student_group}).",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                else:
                    await update.message.reply_text(
                        f"Студент {name} уже является членом группы {admin_group}.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
        except Exception as e:
            logger.error(f"Error processing admin action: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении действия.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()
            context.user_data.clear()
        return

    if context.user_data.get('awaiting_add_admin_id'):
        student_id = text
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT student_group, is_admin FROM students WHERE telegram_id=?', (telegram_id,))
            admin_data = cursor.fetchone()
            if not admin_data or not admin_data[1]:
                await update.message.reply_text(
                    "Только администратор группы может выполнять это действие.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                context.user_data.clear()
                return
            admin_group = admin_data[0]
            cursor.execute('SELECT name, student_group, is_admin FROM students WHERE student_id=?', (student_id,))
            student_data = cursor.fetchone()
            if not student_data:
                name, grades, subjects, course_works = parse_student_data(student_id, telegram_id="added by admin", student_group=admin_group)
                if name == "Unknown":
                    await update.message.reply_text(
                        "Не удалось получить данные по номеру студенческого билета. Проверьте правильность введенного номера. Возможно сервер VUZ2 не отвечает. Попробуйте позже.\n\nВы можете отменить действие командой /cancel.",
                        reply_markup=CANCEL_KEYBOARD_MARKUP
                    )
                    context.user_data.clear()
                    return
                save_to_db(student_id, name, grades, subjects, telegram_id="added by admin", student_group=admin_group, is_admin=True)
                
                # Уведомляем суперадминов о новом администраторе
                notification_text = (
                    "🆕 <b>Новый администратор группы!</b>\n\n"
                    f"• Имя: {name}\n"
                    f"• Группа: {admin_group}\n"
                    f"• Student ID: {student_id}\n"
                    f"• Назначен администратором группы"
                )
                await notify_superadmins(context.application, notification_text)
                
                await update.message.reply_text(
                    f"Пользователь {name} добавлен как администратор группы {admin_group}!",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
            else:
                name, student_group, is_admin_flag = student_data
                if student_group != admin_group:
                    await update.message.reply_text(
                        f"Пользователь {name} находится в другой группе ({student_group}).",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                elif is_admin_flag:
                    await update.message.reply_text(
                        f"Пользователь {name} уже является администратором группы {admin_group}.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                else:
                    cursor.execute('UPDATE students SET is_admin=1 WHERE student_id=?', (student_id,))
                    conn.commit()
                    await update.message.reply_text(
                        f"Пользователь {name} назначен администратором группы {admin_group}.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
        except Exception as e:
            logger.error(f"Error processing add admin action: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении действия.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()
            context.user_data.clear()
        return

    if context.user_data.get('awaiting_add_student_id'):
        student_id = text
        # Проверяем валидность student_id
        is_valid, error_message = validate_student_id(student_id)
        if not is_valid:
            await update.message.reply_text(
                f"Ошибка: {error_message}\n\nПожалуйста, введите корректный номер студенческого билета или отмените действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT student_group, is_admin FROM students WHERE telegram_id=?', (telegram_id,))
            admin_data = cursor.fetchone()
            if not admin_data or not admin_data[1]:
                await update.message.reply_text(
                    "Только администратор группы может выполнять это действие.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                context.user_data.clear()
                return
            admin_group = admin_data[0]
            cursor.execute('SELECT name, student_group FROM students WHERE student_id=?', (student_id,))
            student_data = cursor.fetchone()
            if not student_data:
                name, grades, subjects, course_works = parse_student_data(student_id, telegram_id="added by admin", student_group=admin_group)
                if name == "Unknown":
                    await update.message.reply_text(
                        "Не удалось получить данные по номеру студенческого билета. Проверьте правильность введенного номера. Возможно сервер VUZ2 не отвечает. Попробуйте позже.\n\nВы можете отменить действие командой /cancel.",
                        reply_markup=CANCEL_KEYBOARD_MARKUP
                    )
                    context.user_data.clear()
                    return
                save_to_db(student_id, name, grades, subjects, telegram_id="added by admin", student_group=admin_group)
                from utils import save_course_work_to_db
                for cw in course_works:
                    save_course_work_to_db(
                        student_id=student_id,
                        name=name,
                        telegram_id="added by admin",
                        student_group=admin_group,
                        discipline=cw.get('discipline'),
                        file_path=cw.get('file_path'),
                        semester=cw.get('semester')
                    )
                
                # Уведомляем суперадминов о новом пользователе, добавленном админом
                notification_text = (
                    "🆕 <b>Новый пользователь добавлен администратором!</b>\n\n"
                    f"• Имя: {name}\n"
                    f"• Группа: {admin_group}\n"
                    f"• Student ID: {student_id}\n"
                    f"• Добавлен администратором группы"
                )
                await notify_superadmins(context.application, notification_text)
                
                await update.message.reply_text(
                    f"Студент {name} добавлен в группу {admin_group}!\n\nВведите следующий номер студенческого билета или /cancel для выхода.",
                    reply_markup=CANCEL_KEYBOARD_MARKUP
                )
                # context.user_data['awaiting_add_student_id'] оставляем True для продолжения цикла
                return
            else:
                name, student_group = student_data
                if student_group != admin_group:
                    await update.message.reply_text(
                        f"Студент {name} находится в другой группе ({student_group}).\n\nВведите следующий номер студенческого билета или /cancel для выхода.",
                        reply_markup=CANCEL_KEYBOARD_MARKUP
                    )
                    return
                else:
                    await update.message.reply_text(
                        f"Студент {name} уже является членом группы {admin_group}.\n\nВведите следующий номер студенческого билета или /cancel для выхода.",
                        reply_markup=CANCEL_KEYBOARD_MARKUP
                    )
                    return
        except Exception as e:
            logger.error(f"Error processing add student action: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении действия.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()
        return

    if context.user_data.get('awaiting_superadmin_student_id'):
        student_id = update.message.text.strip()
        # Проверяем валидность student_id
        is_valid, error_message = validate_student_id(student_id)
        if not is_valid:
            await update.message.reply_text(
                f"Ошибка: {error_message}\n\nПожалуйста, введите корректный номер студенческого билета или отмените действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return

        context.user_data['temp_superadmin_student_id'] = student_id
        context.user_data['awaiting_superadmin_student_id'] = False
        context.user_data['awaiting_superadmin_group'] = True
        # Получаем список уникальных групп
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT student_group FROM students WHERE student_group IS NOT NULL AND student_group != ""')
        groups = [row[0] for row in cursor.fetchall() if row[0]]
        conn.close()
        group_keyboard = [[g] for g in sorted(groups)] if groups else []
        from telegram import ReplyKeyboardMarkup
        reply_markup = ReplyKeyboardMarkup(group_keyboard, resize_keyboard=True, one_time_keyboard=True) if group_keyboard else CANCEL_KEYBOARD_MARKUP
        await update.message.reply_text(
            "Введите группу БОЛЬШИМИ РУССКИМИ БУКВАМИ, например ПМР-231, либо выберите из уже существующих:",
            reply_markup=reply_markup
        )
        return
    if context.user_data.get('awaiting_superadmin_group'):
        student_group = update.message.text.strip().upper()
        student_id = context.user_data.get('temp_superadmin_student_id')
        # Сообщаем пользователю о начале процесса
        if not context.user_data.get('superadmin_registration_in_progress'):
            context.user_data['superadmin_registration_in_progress'] = True
            await update.message.reply_text(
                "Идет регистрация пользователя, пожалуйста, подождите... Это может занять до минуты.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        # Парсим только если еще не парсили для этого студента в этой сессии
        if 'temp_superadmin_parsed_student_id' in context.user_data and context.user_data['temp_superadmin_parsed_student_id'] == student_id:
            name = context.user_data['temp_superadmin_name']
            grades = context.user_data['temp_superadmin_grades']
            subjects = context.user_data['temp_superadmin_subjects']
            course_works = context.user_data['temp_superadmin_course_works']
        else:
            name, grades, subjects, course_works = parse_student_data(student_id)
            context.user_data['temp_superadmin_name'] = name
            context.user_data['temp_superadmin_grades'] = grades
            context.user_data['temp_superadmin_subjects'] = subjects
            context.user_data['temp_superadmin_course_works'] = course_works
            context.user_data['temp_superadmin_parsed_student_id'] = student_id
        if name == "Unknown":
            context.user_data.pop('superadmin_registration_in_progress', None)
            await update.message.reply_text(
                "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или сервер VUZ2 не отвечает. Попробуйте позже.\n\nВы можете отменить действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            context.user_data.clear()
            return
        try:
            save_to_db(
                student_id=student_id,
                name=name,
                grades=grades,
                subjects=subjects,
                telegram_id="added_by_superadmin",
                student_group=student_group,
                is_admin=False
            )
            for cw in course_works:
                from utils import save_course_work_to_db
                save_course_work_to_db(
                    student_id=student_id,
                    name=name,
                    telegram_id="added_by_superadmin",
                    student_group=student_group,
                    discipline=cw.get('discipline'),
                    file_path=cw.get('file_path'),
                    semester=cw.get('semester')
                )
            
            # Уведомляем других суперадминов о новом пользователе
            notification_text = (
                "🆕 <b>Новый пользователь добавлен суперадминистратором!</b>\n\n"
                f"• Имя: {name}\n"
                f"• Группа: {student_group}\n"
                f"• Student ID: {student_id}"
            )
            await notify_superadmins(context.application, notification_text)
            
            context.user_data.pop('superadmin_registration_in_progress', None)
            await update.message.reply_text(
                f"Пользователь {name} успешно добавлен в группу {student_group}.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        except Exception as e:
            context.user_data.pop('superadmin_registration_in_progress', None)
            logger.error(f"Ошибка при добавлении пользователя суперадмином (user_id: {update.effective_user.id}): {e}")
            await update.message.reply_text(
                "Произошла ошибка при добавлении пользователя.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        context.user_data.clear()
        return

    if context.user_data.get('awaiting_title'):
        if len(text) > 50:
            await update.message.reply_text(
                "❌ Заголовок слишком длинный. Пожалуйста, сократите его до 50 символов.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return
        context.user_data['title'] = text
        context.user_data['awaiting_title'] = False
        context.user_data['awaiting_content'] = True
        await update.message.reply_text(
            "Введите текст объявления:\n\n"
            "Вы можете отменить создание объявления командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    if context.user_data.get('awaiting_content'):
        context.user_data['content'] = text
        context.user_data['awaiting_content'] = False
        context.user_data['awaiting_contacts'] = True
        await update.message.reply_text(
            "Введите контактные данные (например, Telegram, email или телефон):\n\n"
            "Вы можете отменить создание объявления командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    if context.user_data.get('awaiting_contacts'):
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем student_id пользователя
            cursor.execute('SELECT student_id FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await update.message.reply_text(
                    "❌ Ошибка: пользователь не найден.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                context.user_data.clear()
                return

            student_id = result[0]
            is_anon = context.user_data.get('announcement_type') == 'create_anon'
            title = context.user_data.get('title')
            content = context.user_data.get('content')
            contacts = text

            # Сохраняем объявление с текущим временем
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('''
                INSERT INTO blackmarket (student_id, is_anon, title, content, contacts, publication_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (student_id, is_anon, title, content, contacts, current_time))
            conn.commit()

            # Получаем ID только что созданного объявления
            announcement_id = cursor.lastrowid

            # Отправляем уведомления пользователям
            cursor.execute('''
                SELECT telegram_id 
                FROM students 
                WHERE blackmarket_announcements = 1 
                AND telegram_id IS NOT NULL 
                AND telegram_id != ? 
                AND telegram_id != "added by admin"
                AND telegram_id != "added_by_superadmin"
            ''', (telegram_id,))
            users_to_notify = cursor.fetchall()

            logger.info(f"Найдено {len(users_to_notify)} пользователей для уведомления о новом объявлении")

            # Формируем текст уведомления
            preview_length = 200
            content_preview = content[:preview_length] + "..." if len(content) > preview_length else content
            
            # Получаем информацию об авторе
            author_info = "🕵️ Анонимно" if is_anon else f"👤 {name} ({student_group})"
            
            notification = (
                "🔔 <b>НОВОЕ ОБЪЯВЛЕНИЕ НА BLACK MARKET!</b> 🏪\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📌 <b>{title}</b>\n\n"
                f"👥 <b>Автор:</b> {author_info}\n"
                f"📞 <b>Контакты:</b> {contacts}\n"
                f"⏰ <b>Опубликовано:</b> {current_time}\n\n"
                f"📝 <b>Описание:</b>\n{content_preview}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "👇 Нажмите кнопку ниже, чтобы посмотреть полное объявление"
            )

            # Отправляем уведомления с простым ID объявления
            success_count = 0
            for (user_telegram_id,) in users_to_notify:
                try:
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("👁 Посмотреть", callback_data=f'view_{announcement_id}')
                    ]])
                    
                    # Проверяем, что telegram_id является числом
                    try:
                        user_telegram_id_int = int(user_telegram_id)
                    except (ValueError, TypeError):
                        logger.error(f"Некорректный telegram_id: {user_telegram_id}")
                        continue

                    await context.application.bot.send_message(
                        chat_id=user_telegram_id_int,
                        text=notification,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
                    success_count += 1
                    logger.info(f"Уведомление успешно отправлено пользователю {user_telegram_id}")
                except Exception as e:
                    if "Forbidden: bot was blocked by the user" in str(e):
                        logger.warning(f"Бот заблокирован пользователем {user_telegram_id}")
                    elif "chat not found" in str(e):
                        logger.warning(f"Чат не найден для пользователя {user_telegram_id}")
                    else:
                        logger.error(f"Ошибка при отправке уведомления пользователю {user_telegram_id}: {str(e)}")

            logger.info(f"Уведомления отправлены успешно: {success_count} из {len(users_to_notify)}")

            # Возвращаем пользователя в меню черного рынка
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("« Вернуться в Black Market", callback_data='black_market')
            ]])
            await update.message.reply_text(
                "✅ Объявление успешно создано!",
                reply_markup=keyboard
            )
            context.user_data.clear()

        except Exception as e:
            logger.error(f"Ошибка при создании объявления: {e}")
            await update.message.reply_text(
                "❌ Произошла ошибка при создании объявления.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            context.user_data.clear()
        finally:
            conn.close()
        return

    # Обработка ввода информации о дисциплине
    if context.user_data.get('editing_discipline'):
        editing_data = context.user_data['editing_discipline']
        disc_num = editing_data['number']
        step = editing_data['step']
        
        if step == 'discipline_name':
            editing_data['discipline'] = text
            editing_data['step'] = 'lector_name'
            await update.message.reply_text(
                "Введите полное имя преподавателя (многие студенты с трудом запоминают имена, вводите полное имя):",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return
            
        elif step == 'lector_name':
            editing_data['lector_name'] = text
            editing_data['step'] = 'auditory'
            await update.message.reply_text(
                "Введите аудиторию:",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return
            
        elif step == 'auditory':
            editing_data['auditory'] = text
            # Сохраняем данные в базу сразу после аудитории
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                # Получаем группу администратора
                cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (user_id,))
                result = cursor.fetchone()
                if not result:
                    await update.message.reply_text(
                        "Ошибка: группа не найдена.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                    return
                group = result[0]
                # Создаем JSON с данными дисциплины
                discipline_data = {
                    'discipline': editing_data['discipline'],
                    'lector_name': editing_data['lector_name'],
                    'auditory': editing_data['auditory']
                }
                # Обновляем данные в базе
                cursor.execute(f'''
                    UPDATE disciplines 
                    SET disc_{disc_num}=? 
                    WHERE group_name=?
                ''', (json.dumps(discipline_data), group))
                conn.commit()
                # Очищаем данные редактирования
                context.user_data.pop('editing_discipline', None)
                # После сохранения сразу возвращаем к настройке списка дисциплин
                # (имитируем нажатие кнопки 'Назад')
                keyboard = []
                cursor.execute('SELECT * FROM disciplines WHERE group_name=?', (group,))
                disciplines = cursor.fetchone()
                cursor.execute('PRAGMA table_info(disciplines)')
                columns = {row[1]: idx for idx, row in enumerate(cursor.fetchall())}
                if disciplines:
                    for i in range(1, 31):
                        disc_field = f'disc_{i}'
                        disc_data = disciplines[columns[disc_field]] if disc_field in columns else None
                        if disc_data:
                            try:
                                disc_info = json.loads(disc_data)
                                if disc_info.get('inactive'):
                                    button_text = f"{i}. inactive"
                                else:
                                    button_text = f"{i}. {disc_info.get('discipline', 'Не задано')}"
                            except json.JSONDecodeError:
                                button_text = f"{i}. Ошибка данных"
                        else:
                            button_text = f"{i}. Не задано"
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=f'edit_disc_{i}')])
                else:
                    for i in range(1, 31):
                        keyboard.append([InlineKeyboardButton(f"{i}. Не задано", callback_data=f'edit_disc_{i}')])
                keyboard.append([InlineKeyboardButton("« Назад", callback_data='schedule')])
                await update.message.reply_text(
                    "✅ Информация о дисциплине успешно сохранена!\n\nВыберите номер дисциплины для редактирования:",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e:
                logger.error(f"Ошибка при сохранении информации о дисциплине: {e}")
                await update.message.reply_text(
                    "Произошла ошибка при сохранении информации о дисциплине.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
            finally:
                conn.close()
            return

    # Обработка ввода расписания
    if context.user_data.get('awaiting_schedule_input'):
        schedule_data = context.user_data.get('editing_schedule', {})
        if not schedule_data:
            await update.message.reply_text(
                "Ошибка: данные для редактирования не найдены.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return

        # Разбиваем введенное расписание на строки
        lessons = text.split('\n')
        lessons = [lesson.strip() for lesson in lessons if lesson.strip()]
        
        # Проверяем формат ввода
        valid_lessons = []
        for lesson in lessons:
            if not lesson.startswith(('1.', '2.', '3.', '4.', '5.')):
                await update.message.reply_text(
                    "Неверный формат ввода. Каждая строка должна начинаться с номера пары (1-5) и точки.",
                    reply_markup=CANCEL_KEYBOARD_MARKUP
                )
                return
            valid_lessons.append(lesson[2:].strip())  # Убираем номер и точку

        # Заполняем пустыми значениями отсутствующие пары
        while len(valid_lessons) < 5:
            valid_lessons.append('')

        # Сохраняем расписание в базу
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (user_id,))
            group_result = cursor.fetchone()
            if not group_result:
                await update.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return

            group = group_result[0]
            day = schedule_data['day']
            subgroup = schedule_data['subgroup']
            week_type = schedule_data['week_type']

            # Проверяем существование записи
            cursor.execute('''
                SELECT 1 FROM raspisanie 
                WHERE group_full_name=? AND subgroup=? AND week_type=?
            ''', (group, subgroup, week_type))
            exists = cursor.fetchone()

            if exists:
                # Обновляем существующее расписание
                updates = []
                values = []
                for i, lesson in enumerate(valid_lessons, 1):
                    updates.append(f"{day}_{i}=?")
                    values.append(lesson)
                values.extend([group, subgroup, week_type])
                
                query = f'''
                    UPDATE raspisanie 
                    SET {', '.join(updates)}
                    WHERE group_full_name=? AND subgroup=? AND week_type=?
                '''
                cursor.execute(query, values)
            else:
                # Создаем новую запись
                columns = [f"{day}_{i}" for i in range(1, 6)]
                placeholders = ['?'] * 5
                values = valid_lessons + [group, subgroup, week_type]
                
                query = f'''
                    INSERT INTO raspisanie (
                        {', '.join(columns)},
                        group_full_name, subgroup, week_type
                    ) VALUES ({', '.join(placeholders)}, ?, ?, ?)
                '''
                cursor.execute(query, values)

            conn.commit()
            
            # Очищаем данные ожидания ввода
            context.user_data.pop('awaiting_schedule_input', None)
            context.user_data.pop('editing_schedule', None)
            
            # Отправляем сообщение об успешном сохранении
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='edit_schedule')]])
            await update.message.reply_text(
                "✅ Расписание успешно сохранено!",
                reply_markup=keyboard
            )
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении расписания: {e}")
            await update.message.reply_text(
                "Произошла ошибка при сохранении расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
    return

@handle_telegram_timeout()
async def handle_inline_buttons(update, context):
    query = update.callback_query
    if not query:
        logger.error("No callback_query in update")
        return
    await query.answer()

    callback_data = query.data
    if not callback_data:
        logger.error("No callback_data received")
        return

    user_id = update.effective_user.id
    logger.info(f"Нажата inline кнопка {callback_data} пользователем {user_id}")
    telegram_id = str(update.effective_user.id)

    # --- Вытаскиваем is_superadmin ---
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT student_id, student_group, is_admin, is_superadmin FROM students WHERE telegram_id=?', (telegram_id,))
        student_row = cursor.fetchone()
        if student_row:
            student_id, student_group, is_admin, is_superadmin = student_row
        else:
            student_id, student_group, is_admin, is_superadmin = None, None, 0, 0
    except Exception as e:
        logger.error(f"Ошибка при получении профиля: {e}")
        student_id, student_group, is_admin, is_superadmin = None, None, 0, 0
    finally:
        conn.close()
    is_registered = student_id is not None

    if not is_registered:
        await update.callback_query.message.reply_text(
            "Вы не зарегистрированы. Введите номер студенческого билета:\n\nВы можете отменить действие командой /cancel.",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data.clear()
        context.user_data['awaiting_student_id'] = True
        return

    if callback_data == 'my_rating':
        if not is_registered:
            await query.message.reply_text(
                "Вы не зарегистрированы, воспользуйтесь кнопкой Мой Профиль для регистрации в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM students WHERE student_id=?", (student_id,))
            row = cursor.fetchone()
            columns = [desc[0] for desc in cursor.description]
            student_data = dict(zip(columns, row))
            name = student_data.get('name', 'Неизвестно')
            message = format_ratings_table(name, student_data)
            await query.message.reply_text(message, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
        except Exception as e:
            logger.error(f"Database error: {e}")
            await query.message.reply_text("Произошла ошибка при получении данных.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()

    elif callback_data == 'group':
        if not is_registered:
            await query.message.reply_text(
                "Вы не зарегистрированы, воспользуйтесь кнопкой Мой Профиль для регистрации в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT student_id, name FROM students WHERE student_group=? ORDER BY name', (student_group,))
            students = cursor.fetchall()
            keyboard = [[InlineKeyboardButton(name, callback_data=f"student_{student_id}")] for student_id, name in students]
            if is_admin:
                keyboard.append([InlineKeyboardButton("Добавить студента", callback_data='add_student')])
            await query.message.reply_text(
                f"Студенты вашей группы ({student_group}):",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Database error in group handler (user_id: {update.effective_user.id}): {e}")
            await query.message.reply_text("Произошла ошибка при обработке запроса.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()

    elif callback_data == 'disciplines':
        if not is_registered:
            await query.message.reply_text(
                "Вы не зарегистрированы, воспользуйтесь кнопкой Мой Профиль для регистрации в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM students WHERE student_id=?", (student_id,))
            row = cursor.fetchone()
            if not row:
                await query.message.reply_text(
                    "Данные студента не найдены.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            columns = [desc[0] for desc in cursor.description]
            student_data = dict(zip(columns, row))
            disciplines = []
            for col in columns:
                if "(модуль" in col and student_data[col] != "не изучает":
                    disc_name = col.split(' (модуль')[0].strip()
                    if disc_name and disc_name not in disciplines:
                        disciplines.append(disc_name)
            if not disciplines:
                await query.message.reply_text(
                    "Вы не изучаете ни одной дисциплины.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            # Use short keys for callback_data
            discipline_map = {}
            keyboard = []
            for idx, disc in enumerate(sorted(disciplines)):
                key = f"d{idx}"
                discipline_map[key] = disc
                keyboard.append([InlineKeyboardButton(disc, callback_data=f"discipline_{key}")])
            context.user_data['discipline_map'] = discipline_map
            await query.message.reply_text(
                "Ваши дисциплины:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Database error in disciplines handler (user_id: {update.effective_user.id}): {e}")
            await query.message.reply_text("Произошла ошибка при обработке запроса.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()

    elif callback_data.startswith('discipline_'):
        if not is_registered:
            await query.message.reply_text(
                "Вы не зарегистрированы, воспользуйтесь кнопкой Мой Профиль для регистрации в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        try:
            discipline_key = callback_data[len('discipline_'):]
            logger.info(f"Получен callback: {discipline_key}")
            logger.info(f"Текущий discipline_map: {context.user_data.get('discipline_map', {})}")
            discipline_name = context.user_data.get('discipline_map', {}).get(discipline_key)
            if not discipline_name:
                logger.error(f"Discipline not found in discipline_map for callback_data: {discipline_key}")
                await query.message.reply_text(
                    "Ошибка: дисциплина не найдена. Попробуйте снова.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            conn = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
                result = cursor.fetchone()
                if not result:
                    await query.message.reply_text(
                        "Группа не найдена. Пожалуйста, зарегистрируйтесь заново.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                    return
                student_group = result[0]
                # Проверяем, что нужные столбцы существуют в таблице
                module1_col = f'{discipline_name} (модуль 1)'
                module2_col = f'{discipline_name} (модуль 2)'
                cursor.execute('PRAGMA table_info(students)')
                columns_info = [col[1] for col in cursor.fetchall()]
                def normalize_colname(s):
                    return re.sub(r'\s+', ' ', s.strip().lower())
                norm_module1 = normalize_colname(module1_col)
                norm_module2 = normalize_colname(module2_col)
                module1_col_real = next((c for c in columns_info if normalize_colname(c) == norm_module1), None)
                module2_col_real = next((c for c in columns_info if normalize_colname(c) == norm_module2), None)
                if not module1_col_real or not module2_col_real:
                    await query.message.reply_text(
                        f"Ошибка: не найден столбец для дисциплины '{discipline_name}' в таблице студентов.\nПопробуйте вручную проверить названия столбцов в базе данных.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                    return
                module1_col_sql = f'"{module1_col_real}"'
                module2_col_sql = f'"{module2_col_real}"'
                sql_query = f'SELECT student_id, name, {module1_col_sql}, {module2_col_sql} FROM students WHERE student_group=? ORDER BY name'
                cursor.execute(sql_query, (student_group,))
                students = cursor.fetchall()
                if not students:
                    await query.message.reply_text(
                        "В вашей группе нет студентов, изучающих эту дисциплину.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                    return
                group_data = []
                for student in students:
                    student_id, name, m1, m2 = student
                    grades = {
                        f"{discipline_name} (модуль 1)": m1 if m1 not in ["не изучает", None, "None"] else "-",
                        f"{discipline_name} (модуль 2)": m2 if m2 not in ["не изучает", None, "None"] else "-"
                    }
                    group_data.append((name, grades))
                message = format_ratings_table(discipline_name, group_data, is_group=True)

                # Проверяем наличие курсовых работ по дисциплине (для всех групп)
                cursor.execute('SELECT COUNT(*) FROM course_works WHERE TRIM(LOWER(discipline))=TRIM(LOWER(?))', (discipline_name,))
                cw_count = cursor.fetchone()[0]
                if cw_count > 0:
                    # Добавляем кнопку "Курсовые работы" с коротким ключом
                    keyboard = [
                        [InlineKeyboardButton("Курсовые работы", callback_data=f"courseworks_{discipline_key}")]
                    ]
                    await query.message.reply_text(
                        message,
                        parse_mode='HTML',
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                else:
                    await query.message.reply_text(
                        message,
                        parse_mode='HTML',
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
            except Exception as e:
                logger.error(f"Error displaying discipline ratings (user_id: {update.effective_user.id}): {e}")
                await query.message.reply_text("Произошла ошибка при получении данных.\n\nВы можете вернуться в главное меню командой /cancel.")
            finally:
                if conn:
                    conn.close()
        except Exception as inner_error:
            logger.error(f"Unexpected error in discipline handler: {inner_error}")

    elif callback_data.startswith('courseworks_'):
        # --- Показываем список курсовых работ по дисциплине ---
        discipline_key = callback_data[len('courseworks_'):]
        # Логируем ключ дисциплины
        logger.info(f"courseworks_: discipline_key={discipline_key}")
        # Получаем название дисциплины по ключу из user_data
        discipline_name = context.user_data.get('discipline_map', {}).get(discipline_key)
        logger.info(f"courseworks_: discipline_name={discipline_name}")
        if not discipline_name:
            logger.error(f"courseworks_: Не найдено название дисциплины по ключу {discipline_key}. discipline_map={context.user_data.get('discipline_map', {})}")
            await query.message.reply_text(
                "Ошибка: дисциплина не найдена. Попробуйте снова.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем все курсовые работы по дисциплине (без фильтра по группе)
            cursor.execute('SELECT name, discipline, file_path, semester, student_group FROM course_works WHERE TRIM(LOWER(discipline))=TRIM(LOWER(?))', (discipline_name,))
            course_works = cursor.fetchall()
            logger.info(f"courseworks_: найдено {len(course_works)} курсовых работ по дисциплине {discipline_name}")
            if not course_works:
                await query.message.reply_text(
                    "Курсовые работы по этой дисциплине не найдены.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            buttons = []
            coursework_map = {}
            for idx, (name, discipline, file_path, semester, student_group) in enumerate(course_works, 1):
                # Получаем только имя архива без папки
                filename = os.path.basename(file_path)
                btn_text = filename
                cw_key = f"cw{idx}"
                norm_file_path = os.path.normpath(file_path) if file_path else file_path
                coursework_map[cw_key] = norm_file_path
                logger.info(f"courseworks_: добавлен coursework_map[{cw_key}]={norm_file_path}")
                buttons.append([InlineKeyboardButton(btn_text, callback_data=f"getcw_{cw_key}")])
            # Кнопка для скачивания всех работ архивом
            buttons.append([InlineKeyboardButton("Скачать все архивом", callback_data=f"getcwzip_{discipline_key}")])
            # Сохраняем map в user_data
            context.user_data['coursework_map'] = coursework_map
            logger.info(f"courseworks_: coursework_map={coursework_map}")
            await query.message.reply_text(
                f"<b>Курсовые работы по дисциплине {discipline_name}:</b>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception as e:
            logger.error(f"Ошибка при получении курсовых работ (user_id: {update.effective_user.id}): {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении курсовых работ.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
    elif callback_data.startswith('getcw_'):
        # --- Отправка отдельной курсовой работы ---
        cw_key = callback_data[len('getcw_'):]
        # Получаем путь к файлу из сохранённой map
        file_path = context.user_data.get('coursework_map', {}).get(cw_key)
        norm_file_path = os.path.normpath(file_path) if file_path else file_path
        logger.info(f"getcw_: cw_key={cw_key}, file_path={norm_file_path}")
        # Проверяем, существует ли файл физически
        file_exists = norm_file_path and os.path.isfile(norm_file_path)
        logger.info(f"getcw_: file_exists={file_exists}")
        if not norm_file_path or not file_exists:
            logger.error(f"getcw_: Файл не найден. coursework_map={context.user_data.get('coursework_map', {})}")
            await query.message.reply_text(
                "Ошибка: файл не найден. Попробуйте снова.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        try:
            with open(norm_file_path, 'rb') as f:
                logger.info(f"getcw_: отправка файла {norm_file_path}")
                await query.message.reply_document(f, filename=os.path.basename(norm_file_path))
        except Exception as e:
            logger.error(f"Ошибка при отправке файла {norm_file_path}: {e}")
            await query.message.reply_text(
                "Ошибка при отправке файла.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )

    elif callback_data.startswith('getcwzip_'):
        # --- Отправка архива всех курсовых работ по дисциплине ---
        discipline_key = callback_data[len('getcwzip_'):]
        logger.info(f"getcwzip_: discipline_key={discipline_key}")
        discipline_name = context.user_data.get('discipline_map', {}).get(discipline_key)
        logger.info(f"getcwzip_: discipline_name={discipline_name}")
        
        if not discipline_name:
            logger.error(f"getcwzip_: Не найдено название дисциплины по ключу {discipline_key}. discipline_map={context.user_data.get('discipline_map', {})}")
            await query.message.reply_text(
                "Ошибка: дисциплина не найдена. Попробуйте снова.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return

        # Отправляем сообщение о начале процесса
        status_message = await query.message.reply_text(
            "⏳ Подготовка архива курсовых работ...\n"
            "Пожалуйста, подождите и не нажимайте другие кнопки."
        )

        try:
            # Инициализируем менеджер архивов
            archive_manager = CourseWorkArchiveManager()
            
            # Получаем или создаем архив
            archive_paths, is_updated, info_message = await archive_manager.get_or_create_archive(discipline_name)
            
            if not archive_paths:
                await status_message.edit_text(info_message, reply_markup=REPLY_KEYBOARD_MARKUP)
                return

            # Получаем все части архива из базы данных
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT archive_parts FROM course_work_archives WHERE discipline=?', (discipline_name,))
                result = cursor.fetchone()
                if result and result[0]:
                    try:
                        archive_paths = json.loads(result[0])
                    except json.JSONDecodeError:
                        logger.error(f"Ошибка при декодировании JSON для archive_parts: {result[0]}")
                        archive_paths = []

            # Обновляем статус
            total_parts = len(archive_paths)
            if total_parts > 1:
                await status_message.edit_text(
                    f"{info_message}\n"
                    "📤 Загрузка архивов в Telegram..."
                )
            else:
                await status_message.edit_text(
                    f"{info_message}\n"
                    "📤 Загрузка архива в Telegram..."
                )

            # Проверяем размер каждого архива перед отправкой
            for i, archive_path in enumerate(archive_paths, 1):
                if not os.path.exists(archive_path):
                    logger.error(f"Файл архива не найден: {archive_path}")
                    continue

                file_size = os.path.getsize(archive_path)
                if file_size > 50 * 1024 * 1024:  # 50MB
                    logger.error(f"Архив {archive_path} превышает лимит Telegram (размер: {file_size/1024/1024:.2f}MB)")
                    continue

                if total_parts > 1:
                    await status_message.edit_text(
                        f"{info_message}\n"
                        f"📤 Загрузка части {i} из {total_parts}..."
                    )
                    logger.info(f"Начинаем отправку части {i} из {total_parts} для дисциплины '{discipline_name}'")

                try:
                    with open(archive_path, 'rb') as f:
                        filename = os.path.basename(archive_path)
                        caption = "✅ Архив курсовых работ успешно загружен!"
                        if total_parts > 1:
                            caption = f"✅ Часть {i} из {total_parts} архива курсовых работ"
                        logger.info(f"Отправка файла {filename} (часть {i} из {total_parts})")
                        await query.message.reply_document(
                            f,
                            filename=filename,
                            caption=caption
                        )
                        logger.info(f"Успешно отправлен файл {filename} (часть {i} из {total_parts})")
                        # Небольшая пауза между отправкой частей
                        if i < total_parts:
                            await asyncio.sleep(1)
                except Exception as e:
                    logger.error(f"Ошибка при отправке архива {archive_path}: {e}")
                    if "Request Entity Too Large" in str(e):
                        await query.message.reply_text(
                            f"❌ Часть {i} архива слишком большая для отправки через Telegram.\n"
                            "Пожалуйста, обратитесь к администратору."
                        )
                    continue
            
            # Если было несколько частей, отправляем итоговое сообщение
            if total_parts > 1:
                logger.info(f"Все части архива для '{discipline_name}' успешно отправлены")
                await query.message.reply_text(
                    "✅ Все доступные части архива успешно загружены!\n"
                    "📝 Для распаковки скачайте все части и используйте архиватор."
                )

            # Удаляем статусное сообщение
            await status_message.delete()

        except Exception as e:
            logger.error(f"Ошибка при работе с архивом курсовых работ (user_id: {update.effective_user.id}): {e}")
            error_message = "❌ Произошла ошибка при подготовке архива."
            if "Request Entity Too Large" in str(e):
                error_message = (
                    "❌ Архив слишком большой для отправки через Telegram.\n"
                    "Пожалуйста, попробуйте скачать работы по отдельности."
                )
            await status_message.edit_text(
                error_message,
                reply_markup=REPLY_KEYBOARD_MARKUP
            )

    elif callback_data == 'settings':
        keyboard = []
        keyboard.append([InlineKeyboardButton("ℹ️ Информация о профиле", callback_data='profile_info')])
        keyboard.append([InlineKeyboardButton("🔔 Настройка уведомлений", callback_data='notifications_menu')])
        keyboard.append([InlineKeyboardButton("🏪 Black Market", callback_data='black_market')])
        keyboard.append([InlineKeyboardButton("Установить подгруппу", callback_data='set_subgroup')])
        
        if is_admin:
            keyboard.append([InlineKeyboardButton("👥 Добавить админа", callback_data='add_admin')])
        
        if is_superadmin:
            keyboard.append([InlineKeyboardButton("📅 Задать тип недели", callback_data='set_week_type')])
            keyboard.append([InlineKeyboardButton("➕ Добавить пользователя другой группы", callback_data='add_other_group_user')])
            keyboard.append([InlineKeyboardButton("📢 Отправить системное уведомление", callback_data='send_notification')])
            keyboard.append([InlineKeyboardButton("📋 Получить лог бота", callback_data='get_bot_log')])
        
        await query.message.reply_text(
            "⚙️ Настройки\n"
            "Выберите нужный пункт меню:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    elif callback_data == 'set_subgroup':
        # Клавиатура выбора подгруппы
        subgroup_keyboard = [
            [InlineKeyboardButton("Подгруппа 1", callback_data='choose_subgroup_1')],
            [InlineKeyboardButton("Подгруппа 2", callback_data='choose_subgroup_2')],
            [InlineKeyboardButton("« Назад", callback_data='settings')]
        ]
        await query.message.reply_text(
            "Выберите вашу подгруппу:",
            reply_markup=InlineKeyboardMarkup(subgroup_keyboard)
        )
        return

    elif callback_data.startswith('choose_subgroup_'):
        # Обработка выбора подгруппы
        chosen = callback_data.split('_')[-1]
        if chosen not in ('1', '2'):
            await query.message.reply_text("Ошибка: некорректный выбор подгруппы.")
            return
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE students SET subgroup=? WHERE telegram_id=?', (chosen, telegram_id))
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при обновлении подгруппы: {e}")
            await query.message.reply_text("Ошибка при сохранении подгруппы. Попробуйте позже.")
            return
        finally:
            conn.close()
        await query.message.reply_text(f"Ваша подгруппа успешно установлена: {chosen}")
        return

    elif callback_data == 'schedule':
        keyboard = []
        keyboard.append([InlineKeyboardButton("📅 Расписание На Сегодня", callback_data='schedule_today')])
        keyboard.append([InlineKeyboardButton("📅 Расписание На Завтра", callback_data='schedule_tomorrow')])
        keyboard.append([InlineKeyboardButton("📅 Расписание На Неделю", callback_data='schedule_week')])
        
        if is_admin:
            keyboard.append([InlineKeyboardButton("✏️ Редактировать Расписание", callback_data='edit_schedule')])
            keyboard.append([InlineKeyboardButton("📚 Задать список дисциплин", callback_data='setup_disciplines')])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data='settings')])
        
        await query.message.reply_text(
            "📅 Расписание\n"
            "Выберите действие:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    elif callback_data == 'setup_disciplines':
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            
            # Получаем список дисциплин для группы
            cursor.execute('SELECT * FROM disciplines WHERE group_name=?', (group,))
            disciplines = cursor.fetchone()
            
            # Получаем имена колонок
            cursor.execute('PRAGMA table_info(disciplines)')
            columns = {row[1]: idx for idx, row in enumerate(cursor.fetchall())}
            
            keyboard = []
            if disciplines:
                for i in range(1, 31):
                    disc_field = f'disc_{i}'
                    disc_data = disciplines[columns[disc_field]] if disc_field in columns else None
                    if disc_data:
                        try:
                            disc_info = json.loads(disc_data)
                            if disc_info.get('inactive'):
                                button_text = f"{i}. inactive"
                            else:
                                button_text = f"{i}. {disc_info.get('discipline', 'Не задано')}"
                        except json.JSONDecodeError:
                            button_text = f"{i}. Ошибка данных"
                    else:
                        button_text = f"{i}. Не задано"
                    keyboard.append([InlineKeyboardButton(button_text, callback_data=f'edit_disc_{i}')])
            else:
                # Создаем новую запись для группы
                cursor.execute('INSERT INTO disciplines (group_name) VALUES (?)', (group,))
                conn.commit()
                for i in range(1, 31):
                    keyboard.append([InlineKeyboardButton(f"{i}. Не задано", callback_data=f'edit_disc_{i}')])
            
            keyboard.append([InlineKeyboardButton("« Назад", callback_data='schedule')])
            
            await query.message.reply_text(
                "📚 Настройка списка дисциплин\n"
                "Выберите номер дисциплины для редактирования:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка дисциплин: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении списка дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('edit_disc_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        disc_num = callback_data.split('_')[2]
        keyboard = [
            [InlineKeyboardButton("✏️ Настроить", callback_data=f'setup_disc_{disc_num}')],
            [InlineKeyboardButton("❌ Сделать неактивной", callback_data=f'deactivate_disc_{disc_num}')],
            [InlineKeyboardButton("« Назад", callback_data='setup_disciplines')]
        ]
        
        await query.message.reply_text(
            f"Выберите действие для дисциплины {disc_num}:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    elif callback_data.startswith('deactivate_disc_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        disc_num = callback_data.split('_')[2]
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            
            # Обновляем статус дисциплины
            cursor.execute(f'''
                UPDATE disciplines 
                SET disc_{disc_num}=? 
                WHERE group_name=?
            ''', (json.dumps({'inactive': True}), group))
            conn.commit()
            
            await query.message.reply_text(
                "✅ Дисциплина помечена как неактивная",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='setup_disciplines')]])
            )
            
        except Exception as e:
            logger.error(f"Ошибка при деактивации дисциплины: {e}")
            await query.message.reply_text(
                "Произошла ошибка при деактивации дисциплины.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('setup_disc_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        disc_num = callback_data.split('_')[2]
        context.user_data['editing_discipline'] = {
            'number': disc_num,
            'step': 'discipline_name'
        }
        
        await query.message.reply_text(
            "Введите название дисциплины:",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data == 'schedule_today':
        # Получаем расписание на сегодня
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу пользователя
            cursor.execute('SELECT student_group, subgroup FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group, subgroup = result
            if not subgroup:
                subgroup = 1  # По умолчанию первая подгруппа
                
            # Определяем текущий день недели
            weekday = datetime.now().strftime('%A').lower()
            
            # Получаем тип недели из глобальной переменной
            week_type = get_week_type()
            
            # Получаем расписание
            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            cursor.execute(f'''
                SELECT {weekday}_1, {weekday}_2, {weekday}_3, {weekday}_4, {weekday}_5
                FROM raspisanie 
                WHERE group_full_name=?
            ''', (group_full_name,))
            schedule = cursor.fetchone()
            
            if not schedule:
                await query.message.reply_text(
                    "Расписание на сегодня не найдено.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
                return
                
            message, lesson_buttons, lessons_data = build_schedule_keyboard(schedule, group, subgroup, week_type, 'today')
            if lesson_buttons:
                await query.message.reply_text(
                    message,
                    reply_markup=InlineKeyboardMarkup(lesson_buttons)
                )
            else:
                await query.message.reply_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
            context.user_data['lessons_today'] = lessons_data
            
        except Exception as e:
            logger.error(f"Ошибка при получении расписания: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'schedule_tomorrow':
        # Получаем расписание на завтра
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу пользователя
            cursor.execute('SELECT student_group, subgroup FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return

            group, subgroup = result
            if not subgroup:
                subgroup = 1  # По умолчанию первая подгруппа

            now = datetime.now()
            today_weekday = now.strftime('%A').lower()
            tomorrow = now + timedelta(days=1)

            # Если сегодня воскресенье, показать расписание на понедельник противоположной недели
            if today_weekday == 'sunday':
                weekday = 'monday'
                week_type = get_week_type()
                # Переключаем на противоположный тип недели
                week_type = "DOWN" if week_type == "UP" else "UP"
                # Для корректного отображения даты передаем дату следующего понедельника
                days_until_monday = (7 - now.weekday()) % 7 or 7
                next_monday = now + timedelta(days=days_until_monday)
                date_obj = next_monday
            else:
                weekday = tomorrow.strftime('%A').lower()
                week_type = get_week_type()
                date_obj = tomorrow

            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            cursor.execute(f'''
                SELECT {weekday}_1, {weekday}_2, {weekday}_3, {weekday}_4, {weekday}_5
                FROM raspisanie 
                WHERE group_full_name=?
            ''', (group_full_name,))
            schedule = cursor.fetchone()

            if not schedule:
                await query.message.reply_text(
                    "Расписание на завтра не найдено.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
                return

            message, lesson_buttons, lessons_data = build_schedule_keyboard(schedule, group, subgroup, week_type, 'tomorrow', date_obj=date_obj)
            if lesson_buttons:
                await query.message.reply_text(
                    message,
                    reply_markup=InlineKeyboardMarkup(lesson_buttons)
                )
            else:
                await query.message.reply_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
            context.user_data['lessons_tomorrow'] = lessons_data

        except Exception as e:
            logger.error(f"Ошибка при получении расписания: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('lessoninfo_today_') or callback_data.startswith('lessoninfo_window_today_'):
        num = int(callback_data.rsplit('_', 1)[-1])
        lessons = context.user_data.get('lessons_today', [])
        if callback_data.startswith('lessoninfo_window_today_'):
            await query.message.reply_text("Форточка это промежуток между парами. Используй его с пользой. Посиди отдохни, подумай как ты докатился до такой жизни.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule_today')]]))
        elif 0 < num <= len(lessons):
            data = lessons[num-1]
            discipline = data.get('discipline', data.get('description', 'Пара'))
            auditory = data.get('auditory', '—')
            lecturer = data.get('lector_name') or data.get('lecturer', '—')
            comment = data.get('admin_comment') if 'admin_comment' in data else data.get('comment', '')
            msg = f"<b>{discipline}</b>\nАудитория: {auditory}\nПреподаватель: {lecturer}"
            if comment:
                msg += f"\nКомментарий: {comment}"
            await query.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule_today')]]))
        return
    elif callback_data.startswith('lessoninfo_tomorrow_') or callback_data.startswith('lessoninfo_window_tomorrow_'):
        num = int(callback_data.rsplit('_', 1)[-1])
        lessons = context.user_data.get('lessons_tomorrow', [])
        if callback_data.startswith('lessoninfo_window_tomorrow_'):
            await query.message.reply_text("Форточка это промежуток между парами. Используй его с пользой. Посиди отдохни, подумай как ты докатился до такой жизни.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule_tomorrow')]]))
        elif 0 < num <= len(lessons):
            data = lessons[num-1]
            discipline = data.get('discipline', data.get('description', 'Пара'))
            auditory = data.get('auditory', '—')
            lecturer = data.get('lector_name') or data.get('lecturer', '—')
            comment = data.get('admin_comment') if 'admin_comment' in data else data.get('comment', '')
            msg = f"<b>{discipline}</b>\nАудитория: {auditory}\nПреподаватель: {lecturer}"
            if comment:
                msg += f"\nКомментарий: {comment}"
            await query.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule_tomorrow')]]))
        return
    elif callback_data == 'schedule_week':
        # Получаем расписание на неделю
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу пользователя
            cursor.execute('SELECT student_group, subgroup FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group, subgroup = result
            if not subgroup:
                subgroup = 1  # По умолчанию первая подгруппа
                
            # Получаем тип недели
            week_type = get_week_type()
            
            # Получаем расписание на всю неделю
            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            cursor.execute('''
                SELECT * FROM raspisanie 
                WHERE group_full_name=?
            ''', (group_full_name,))
            schedule = cursor.fetchone()
            
            if not schedule:
                await query.message.reply_text(
                    "Расписание на неделю не найдено.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
                return
                
            # Форматируем расписание
            week_type_text = "верхняя" if week_type == "UP" else "нижняя"
            message = f"📅 Расписание на неделю ({week_type_text})\n"
            message += f"Группа: {group}, Подгруппа: {subgroup}\n\n"
            
            days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
            day_columns = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

            columns = [desc[0] for desc in cursor.description]
            schedule_dict = dict(zip(columns, schedule))

            for day_name, day_col in zip(days, day_columns):
                lessons = []
                active_lessons = []
                inactive_count = 0
                for i in range(1, 6):
                    lesson = schedule_dict.get(f"{day_col}_{i}")
                    if lesson and lesson.strip():
                        try:
                            data = json.loads(lesson)
                            if data.get('type') == 'inactive':
                                inactive_count += 1
                            elif data.get('type') == 'window':
                                active_lessons.append(f"{i}. 🪟 Форточка")
                            else:
                                active_lessons.append(f"{i}. {data.get('discipline', data.get('description', 'Пара'))}")
                        except Exception:
                            active_lessons.append(f"{i}. {lesson}")
                    else:
                        inactive_count += 1
                message += f"\n{day_name}:\n"
                if inactive_count == 5:
                    message += "Выходной\n"
                else:
                    message += "\n".join(active_lessons) + "\n"

            # Разбиваем сообщение на части, если оно слишком длинное
            if len(message) > 4096:
                parts = [message[i:i+4096] for i in range(0, len(message), 4096)]
                for part in parts[:-1]:
                    await query.message.reply_text(part)
                await query.message.reply_text(
                    parts[-1],
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
            else:
                await query.message.reply_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='schedule')]])
                )
            
        except Exception as e:
            logger.error(f"Ошибка при получении расписания: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'edit_schedule':
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        keyboard = [
            [InlineKeyboardButton("1️⃣ Подгруппа 1 (верхняя неделя)", callback_data='edit_schedule_1_UP')],
            [InlineKeyboardButton("1️⃣ Подгруппа 1 (нижняя неделя)", callback_data='edit_schedule_1_DOWN')],
            [InlineKeyboardButton("2️⃣ Подгруппа 2 (верхняя неделя)", callback_data='edit_schedule_2_UP')],
            [InlineKeyboardButton("2️⃣ Подгруппа 2 (нижняя неделя)", callback_data='edit_schedule_2_DOWN')],
            [InlineKeyboardButton("« Назад", callback_data='schedule')]
        ]
        
        await query.message.reply_text(
            "✏️ Редактирование расписания\n"
            "Выберите подгруппу и тип недели для редактирования:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    elif callback_data.startswith('edit_schedule_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return

        # Парсим параметры из callback_data (например: edit_schedule_1_UP)
        parts = callback_data.split('_')
        subgroup = parts[2]
        week_type = parts[3]
        logger.info(f"Редактирование расписания: подгруппа {subgroup}, тип недели {week_type}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            logger.info(f"Получено group_full_name: {group_full_name}")
            
            # Получаем текущее расписание
            cursor.execute('''
                SELECT * FROM raspisanie 
                WHERE group_full_name=?
            ''', (group_full_name,))
            schedule = cursor.fetchone()
            logger.info(f"Найдено расписание: {bool(schedule)}")
            
            # Получаем имена колонок
            cursor.execute('PRAGMA table_info(raspisanie)')
            columns = {row[1]: idx for idx, row in enumerate(cursor.fetchall())}
            logger.info(f"Получены колонки таблицы raspisanie: {list(columns.keys())}")
            
            # Создаем клавиатуру с кнопками для каждого дня и пары
            keyboard = []
            days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            
            for day in days:
                day_buttons = []
                for i in range(1, 6):
                    field = f"{day}_{i}"
                    button_text = field
                    if schedule and field in columns:
                        value = schedule[columns[field]]
                        if value:
                            try:
                                data = json.loads(value)
                                if data.get('type') == 'inactive':
                                    button_text = "❌ Неактивно"
                                elif data.get('type') == 'window':
                                    button_text = "🪟 Форточка"
                                else:
                                    button_text = data.get('discipline', field)
                            except json.JSONDecodeError:
                                logger.error(f"Ошибка парсинга JSON для {field}: {value}")
                                button_text = field
                    
                    day_buttons.append(InlineKeyboardButton(
                        button_text, 
                        callback_data=f'edit_slot_{subgroup}_{week_type}_{day}_{i}'
                    ))
                keyboard.append(day_buttons)
            
            keyboard.append([InlineKeyboardButton("« Назад", callback_data='edit_schedule')])
            logger.info(f"Создана клавиатура с {len(keyboard)-1} строками по {len(keyboard[0])} кнопок")
            
            await query.message.reply_text(
                f"📅 Редактирование расписания\n"
                f"Подгруппа: {subgroup}\n"
                f"Неделя: {'верхняя' if week_type == 'UP' else 'нижняя'}\n\n"
                f"Выберите слот для редактирования:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Ошибка при отображении слотов расписания: {str(e)}\n{traceback.format_exc()}")
            await query.message.reply_text(
                "Произошла ошибка при отображении расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('edit_slot_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Парсим параметры (например: edit_slot_1_UP_monday_1)
        try:
            parts = callback_data.split('_')
            subgroup = parts[2]
            week_type = parts[3]
            day = parts[4]
            slot = parts[5]
            logger.info(f"Редактирование слота: подгруппа {subgroup}, тип недели {week_type}, день {day}, слот {slot}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге callback_data '{callback_data}': {str(e)}")
            await query.message.reply_text(
                "Произошла ошибка при обработке команды.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        
        keyboard = [
            [InlineKeyboardButton("✏️ Задать пару", callback_data=f'set_lesson_{subgroup}_{week_type}_{day}_{slot}')],
            [InlineKeyboardButton("🪟 Форточка", callback_data=f'set_window_{subgroup}_{week_type}_{day}_{slot}')],
            [InlineKeyboardButton("❌ Сделать неактивной", callback_data=f'set_inactive_{subgroup}_{week_type}_{day}_{slot}')],
            [InlineKeyboardButton("💬 Добавить комментарий", callback_data=f'set_comment_{subgroup}_{week_type}_{day}_{slot}')],
            [InlineKeyboardButton("« Назад", callback_data=f'edit_schedule_{subgroup}_{week_type}')]
        ]
        
        await query.message.reply_text(
            f"Выберите действие для слота {day}_{slot}:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    elif callback_data.startswith('set_comment_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        try:
            parts = callback_data.split('_')
            subgroup = parts[2]
            week_type = parts[3]
            day = parts[4]
            slot = parts[5]
            context.user_data['edit_comment'] = {
                'subgroup': subgroup,
                'week_type': week_type,
                'day': day,
                'slot': slot
            }
            await query.message.reply_text(
                f"Введите комментарий для {day}_{slot} (или отправьте пустое сообщение, чтобы удалить комментарий):",
                reply_markup=None
            )
            context.user_data['awaiting_admin_comment'] = True
        except Exception as e:
            logger.error(f"Ошибка при начале ввода комментария: {e}")
            await query.message.reply_text(
                "Произошла ошибка при попытке добавить комментарий.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        return
    elif callback_data.startswith('set_lesson_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Парсим параметры
        try:
            parts = callback_data.split('_')
            subgroup = parts[2]
            week_type = parts[3]
            day = parts[4]
            slot = parts[5]
            logger.info(f"Выбор дисциплины для слота: подгруппа {subgroup}, тип недели {week_type}, день {day}, слот {slot}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге callback_data '{callback_data}': {str(e)}")
            await query.message.reply_text(
                "Произошла ошибка при обработке команды.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            
            # Получаем список активных дисциплин для группы
            cursor.execute('SELECT * FROM disciplines WHERE group_name=?', (group,))
            disciplines = cursor.fetchone()
            
            if not disciplines:
                await query.message.reply_text(
                    "Сначала необходимо задать список дисциплин для группы.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("« Назад", callback_data=f'edit_slot_{subgroup}_{week_type}_{day}_{slot}')
                    ]])
                )
                return
            
            # Получаем имена колонок
            cursor.execute('PRAGMA table_info(disciplines)')
            columns = {row[1]: idx for idx, row in enumerate(cursor.fetchall())}
            
            # Создаем клавиатуру с активными дисциплинами
            keyboard = []
            for i in range(1, 31):
                disc_field = f'disc_{i}'
                if disc_field in columns and disciplines[columns[disc_field]]:
                    try:
                        disc_data = json.loads(disciplines[columns[disc_field]])
                        if not disc_data.get('inactive'):
                            keyboard.append([InlineKeyboardButton(
                                disc_data.get('discipline', 'Без названия'),
                                callback_data=f'assign_lesson_{subgroup}_{week_type}_{day}_{slot}_{i}'
                            )])
                    except json.JSONDecodeError:
                        continue
            
            keyboard.append([InlineKeyboardButton("« Назад", callback_data=f'edit_slot_{subgroup}_{week_type}_{day}_{slot}')])
            
            await query.message.reply_text(
                "Выберите дисциплину для этого слота:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Ошибка при выборе дисциплины: {e}")
            await query.message.reply_text(
                "Произошла ошибка при выборе дисциплины.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('assign_lesson_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Парсим параметры
        try:
            parts = callback_data.split('_')
            subgroup = parts[2]
            week_type = parts[3]
            day = parts[4]
            slot = parts[5]
            disc_num = parts[6]
            logger.info(f"Назначение дисциплины {disc_num} для слота: подгруппа {subgroup}, тип недели {week_type}, день {day}, слот {slot}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге callback_data '{callback_data}': {str(e)}")
            await query.message.reply_text(
                "Произошла ошибка при обработке команды.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            
            # Получаем информацию о дисциплине
            cursor.execute(f'SELECT disc_{disc_num} FROM disciplines WHERE group_name=?', (group,))
            disc_data = cursor.fetchone()
            if not disc_data or not disc_data[0]:
                await query.message.reply_text(
                    "Ошибка: дисциплина не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            
            discipline_info = json.loads(disc_data[0])
            
            # Проверяем существование записи в расписании
            cursor.execute('SELECT 1 FROM raspisanie WHERE group_full_name=?', (group_full_name,))
            exists = cursor.fetchone()
            
            if exists:
                # Обновляем существующую запись
                cursor.execute(f'''
                    UPDATE raspisanie 
                    SET {day}_{slot}=? 
                    WHERE group_full_name=?
                ''', (json.dumps(discipline_info), group_full_name))
            else:
                # Создаем новую запись
                cursor.execute(f'''
                    INSERT INTO raspisanie (group_full_name, {day}_{slot})
                    VALUES (?, ?)
                ''', (group_full_name, json.dumps(discipline_info)))
            
            conn.commit()
            
            await query.message.reply_text(
                "✅ Дисциплина успешно назначена",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("« Назад", callback_data=f'edit_schedule_{subgroup}_{week_type}')
                ]])
            )
            
        except Exception as e:
            logger.error(f"Ошибка при назначении дисциплины: {e}")
            await query.message.reply_text(
                "Произошла ошибка при назначении дисциплины.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('set_window_') or callback_data.startswith('set_inactive_'):
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования расписания.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Парсим параметры
        try:
            parts = callback_data.split('_')
            action = parts[1]  # window или inactive
            subgroup = parts[2]
            week_type = parts[3]
            day = parts[4]
            slot = parts[5]
            logger.info(f"Установка статуса {action} для слота: подгруппа {subgroup}, тип недели {week_type}, день {day}, слот {slot}")
        except Exception as e:
            logger.error(f"Ошибка при парсинге callback_data '{callback_data}': {str(e)}")
            await query.message.reply_text(
                "Произошла ошибка при обработке команды.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            group_full_name = f"{group}_sub{subgroup}_{week_type}"
            
            # Подготавливаем данные в зависимости от действия
            if action == 'window':
                data = {
                    'type': 'window',
                    'description': 'Форточка (перерыв между парами)'
                }
                status_text = "форточкой (перерыв)"
            else:  # inactive
                data = {
                    'type': 'inactive',
                    'description': 'Неактивная пара (нет занятий)'
                }
                status_text = "неактивной (нет занятий)"
            
            # Проверяем существование записи в расписании
            cursor.execute('SELECT 1 FROM raspisanie WHERE group_full_name=?', (group_full_name,))
            exists = cursor.fetchone()
            
            if exists:
                # Обновляем существующую запись
                cursor.execute(f'''
                    UPDATE raspisanie 
                    SET {day}_{slot}=? 
                    WHERE group_full_name=?
                ''', (json.dumps(data), group_full_name))
            else:
                # Создаем новую запись
                cursor.execute(f'''
                    INSERT INTO raspisanie (group_full_name, {day}_{slot})
                    VALUES (?, ?)
                ''', (group_full_name, json.dumps(data)))
            
            conn.commit()
            
            await query.message.reply_text(
                f"✅ Пара помечена как {status_text}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("« Назад", callback_data=f'edit_schedule_{subgroup}_{week_type}')
                ]])
            )
            
        except Exception as e:
            logger.error(f"Ошибка при изменении статуса пары: {e}")
            await query.message.reply_text(
                "Произошла ошибка при изменении статуса пары.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'edit_disciplines':
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для редактирования дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем группу администратора
            cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if not result:
                await query.message.reply_text(
                    "Ошибка: группа не найдена.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            group = result[0]
            
            # Получаем список дисциплин для группы
            cursor.execute('SELECT discipline_name, short_name FROM disciplines WHERE group_full_name=?', (group,))
            disciplines = cursor.fetchall()
            
            keyboard = []
            for discipline, short_name in disciplines:
                display_name = f"{discipline} ({short_name})" if short_name else discipline
                keyboard.append([InlineKeyboardButton(display_name, callback_data=f'edit_discipline_{discipline}')])
            
            keyboard.append([InlineKeyboardButton("➕ Добавить дисциплину", callback_data='add_discipline')])
            keyboard.append([InlineKeyboardButton("« Назад", callback_data='edit_schedule')])
            
            await query.message.reply_text(
                "📚 Список дисциплин\n"
                "Выберите дисциплину для редактирования или добавьте новую:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Ошибка при получении списка дисциплин: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении списка дисциплин.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'profile_info':
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT name, student_group, is_admin, is_superadmin, student_id, notifications FROM students WHERE telegram_id=?', (telegram_id,))
            user_row = cursor.fetchone()
            if user_row:
                name, group, is_admin, is_superadmin, student_id_val, notifications = user_row
                status = "Суперадмин" if is_superadmin else ("Админ группы" if is_admin else "Студент")
                notifications_status = "включены" if notifications else "отключены"
                
                # Для суперадмина добавляем статистику пользователей
                users_stats = ""
                if is_superadmin:
                    cursor.execute('SELECT COUNT(*) FROM students')
                    total_users = cursor.fetchone()[0]
                    cursor.execute('SELECT COUNT(*) FROM students WHERE telegram_id IS NOT NULL AND telegram_id != "added by admin"')
                    active_users = cursor.fetchone()[0]
                    users_stats = f"\n\n<b>Статистика пользователей</b>\nВсего пользователей: {total_users}\nАктивных пользователей: {active_users}"
                
                # Ищем всех администраторов группы
                cursor.execute('SELECT name FROM students WHERE student_group=? AND is_admin=1', (group,))
                admin_rows = cursor.fetchall()
                admin_info = "\nAdmin_list:"
                for (admin_name,) in admin_rows:
                    admin_info += f"\n• {admin_name}"
                
                # --- Блок с информацией для связи ---
                admin_help_block = (
                    "\n\n"
                    "<b>Обратная связь</b>\n"
                    "Если вы:\n"
                    "• Нашли ошибку или баг в работе бота\n"
                    "• Есть идеи и предложения по улучшению функционала\n"
                    "• Хотите стать администратором своей группы\n\n"
                    "Свяжитесь с нами:\n"
                    "📧 Email: 6fcag3vsaoag@mail.ru\n"
                    "📱 Telegram: <a href='https://t.me/bycard1'>@bycard1</a>\n"
                )
                profile_text = (
                    f"📚 Сайт Бота: <a href='https://6fcag3vsaoag.github.io/brumarks/'>6fcag3vsaoag.github.io</a>\n\n\n"
                    f"<b>Ваш профиль</b>\n"
                    f"Name: {name}\n"
                    f"Group: {group}\n"
                    f"Student_ID: {student_id_val}\n"
                    f"Status: {status}\n"
                    f"Уведомления: {notifications_status}"
                    f"{admin_info}"
                    f"{users_stats}"
                    f"{admin_help_block}\n\n"
                )
            else:
                profile_text = "Профиль не найден. Зарегистрируйтесь через кнопку Мой Профиль."

            # Добавляем кнопку возврата
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("« Назад", callback_data="settings")
            ]])
            
            await query.message.reply_text(
                profile_text,
                parse_mode='HTML',
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при получении профиля: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при получении информации о профиле.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'notifications_menu':
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("Системные уведомления", callback_data='notification_settings')],
            [InlineKeyboardButton("Уведомления Black Market", callback_data='blackmarket_notifications')],
            [InlineKeyboardButton("« Назад", callback_data='settings')]
        ])
        await query.message.reply_text(
            "🔔 Выберите тип уведомлений для настройки:",
            reply_markup=keyboard
        )
        return

    elif callback_data == 'blackmarket_notifications':
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Включить", callback_data='blackmarket_notifications_on'),
                InlineKeyboardButton("❌ Отключить", callback_data='blackmarket_notifications_off')
            ],
            [InlineKeyboardButton("« Назад", callback_data='notifications_menu')]
        ])
        await query.message.reply_text(
            "🔔 Настройка уведомлений Black Market\n\n"
            "Хотите ли вы получать уведомления о новых объявлениях?",
            reply_markup=keyboard
        )
        return

    elif callback_data in ['blackmarket_notifications_on', 'blackmarket_notifications_off']:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            new_value = 1 if callback_data == 'blackmarket_notifications_on' else 0
            cursor.execute('UPDATE students SET blackmarket_announcements=? WHERE telegram_id=?', (new_value, telegram_id))
            conn.commit()
            status = "включены" if new_value else "отключены"
            back_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("« Назад", callback_data="notifications_menu")
            ]])
            await query.message.reply_text(
                f"✅ Настройки сохранены!\n"
                f"Уведомления Black Market {status}.",
                reply_markup=back_keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при обновлении настроек уведомлений Black Market: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при сохранении настроек.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'black_market':
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Получаем все активные объявления
            cursor.execute('''
                SELECT bm.id, bm.title 
                FROM blackmarket bm 
                ORDER BY bm.publication_time DESC
            ''')
            announcements = cursor.fetchall()
            
            keyboard = []
            for announcement_id, title in announcements:
                keyboard.append([InlineKeyboardButton(title, callback_data=f'view_{announcement_id}')])
            
            # Проверяем, может ли пользователь создавать объявления
            cursor.execute('SELECT blackmarket_allowed FROM students WHERE telegram_id=?', (telegram_id,))
            result = cursor.fetchone()
            if result and result[0] == 1:
                keyboard.append([InlineKeyboardButton("📝 Создать объявление", callback_data='create_announcement')])
            
            keyboard.append([InlineKeyboardButton("« Назад", callback_data='settings')])
            
            if not keyboard:  # Если нет объявлений и нет прав на создание
                keyboard = [[InlineKeyboardButton("« Назад", callback_data='settings')]]
                await query.message.reply_text(
                    "🏪 Black Market\n\n"
                    "В данный момент нет доступных объявлений.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await query.message.reply_text(
                    "🏪 Black Market\n\n"
                    "Здесь вы можете просмотреть актуальные объявления или создать своё.",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        except Exception as e:
            logger.error(f"Ошибка при открытии Black Market: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при загрузке объявлений.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('view_'):
        try:
            announcement_id = int(callback_data.split('_')[1])
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT bm.student_id, bm.is_anon, bm.title, bm.content, bm.contacts, bm.publication_time,
                       s.name, s.student_group, s.telegram_id as author_telegram_id
                FROM blackmarket bm 
                JOIN students s ON bm.student_id = s.student_id 
                WHERE bm.id = ?
            ''', (announcement_id,))
            announcement = cursor.fetchone()
            
            if not announcement:
                await query.message.reply_text(
                    "❌ Объявление не найдено.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='black_market')]])
                )
                return
                
            # Распаковываем данные
            student_id, is_anon, title, content, contacts, pub_time, author_name, author_group, author_telegram_id = announcement
            
            # Формируем текст объявления
            author_text = "🕵️ Анонимно" if is_anon else f"👤 {author_name} ({author_group})"
            message_text = (
                f"<b>{title}</b>\n\n"
                f"Автор: {author_text}\n"
                f"Контакты: {contacts}\n\n"
                f"Содержание:\n{content}\n\n"
                f"Опубликовано: {pub_time}"
            )
            
            # Формируем клавиатуру
            keyboard = []
            
            # Проверяем права на удаление
            if telegram_id == author_telegram_id or is_superadmin:
                if is_superadmin:
                    keyboard.append([
                        InlineKeyboardButton("🗑 Удалить", callback_data=f'del_{announcement_id}'),
                        InlineKeyboardButton("⛔️ Удалить и заблокировать", callback_data=f'delblock_{announcement_id}')
                    ])
                else:
                    keyboard.append([InlineKeyboardButton("🗑 Удалить", callback_data=f'del_{announcement_id}')])
            
            keyboard.append([InlineKeyboardButton("« Назад", callback_data='black_market')])
            
            await query.message.reply_text(
                message_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Ошибка при просмотре объявления: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при загрузке объявления.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            if 'conn' in locals():
                conn.close()
        return

    elif callback_data.startswith('del_'):
        if callback_data.startswith('delblock_'):
            return
        try:
            announcement_id = int(callback_data.split('_')[1])
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Да, удалить", callback_data=f'confirm_del_{announcement_id}'),
                    InlineKeyboardButton("❌ Нет, отменить", callback_data=f'view_{announcement_id}')
                ]
            ])
            await query.message.reply_text(
                "⚠️ Вы уверены, что хотите удалить это объявление?",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при подготовке удаления: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        return

    elif callback_data.startswith('delblock_'):
        try:
            announcement_id = int(callback_data.split('_')[1])
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Да, удалить и заблокировать", callback_data=f'confirm_delblock_{announcement_id}'),
                    InlineKeyboardButton("❌ Нет, отменить", callback_data=f'view_{announcement_id}')
                ]
            ])
            await query.message.reply_text(
                "⚠️ Вы уверены, что хотите удалить объявление и заблокировать пользователя?",
                reply_markup=keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при подготовке удаления с блокировкой: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        return

    elif callback_data.startswith('confirm_del_'):
        try:
            announcement_id = int(callback_data.split('_')[2])
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Удаляем объявление
            cursor.execute('DELETE FROM blackmarket WHERE id=?', (announcement_id,))
            conn.commit()
            
            await query.message.reply_text(
                "✅ Объявление успешно удалено!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='black_market')]])
            )
        except Exception as e:
            logger.error(f"Ошибка при удалении объявления: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при удалении объявления.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            if 'conn' in locals():
                conn.close()
        return

    elif callback_data.startswith('confirm_delblock_'):
        try:
            announcement_id = int(callback_data.split('_')[2])
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Получаем student_id из объявления
            cursor.execute('SELECT student_id FROM blackmarket WHERE id=?', (announcement_id,))
            result = cursor.fetchone()
            if result:
                student_id = result[0]
                
                # Блокируем пользователя
                cursor.execute('UPDATE students SET blackmarket_allowed=0 WHERE student_id=?', (student_id,))
                # Удаляем объявление
                cursor.execute('DELETE FROM blackmarket WHERE id=?', (announcement_id,))
                conn.commit()
                
                await query.message.reply_text(
                    "✅ Объявление удалено и пользователь заблокирован!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='black_market')]])
                )
            else:
                await query.message.reply_text(
                    "❌ Объявление не найдено.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
        except Exception as e:
            logger.error(f"Ошибка при удалении объявления и блокировке: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при удалении объявления и блокировке пользователя.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            if 'conn' in locals():
                conn.close()
        return

    elif callback_data == 'create_announcement':
        rules_text = (
            "📜 <b>Правила размещения объявлений:</b>\n\n"
            "0. не оскорблять других пользователей\n"
            "1. не продавать и не покупать запрещенные кодексом РБ и РФ товары и услуги\n"
            "2. Запрещена реклама запрещенных товаров и услуг\n"
            "3. Желательно, что бы обьявление было связано с универом и было хоть кому-то из студентов быть интересно. Для продажи гаража есть куфар.\n"
            "4. Помните, что вы подписывали бумагу о том, что запрещено прибегать к помощи третьих лиц при написании курсовых работ\n"
            "5. Администрация оставляет за собой право удалять объявления. Причем вы можете вообще лишиться возможности их публиковать\n\n"
            "Вы согласны с правилами?"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Да", callback_data='accept_rules'),
                InlineKeyboardButton("❌ Нет", callback_data='black_market')
            ]
        ])
        await query.message.reply_text(rules_text, parse_mode='HTML', reply_markup=keyboard)
        return

    elif callback_data == 'accept_rules':
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("👤 Показать имя и группу", callback_data='create_public'),
                InlineKeyboardButton("🕵️ Анонимно", callback_data='create_anon')
            ],
            [InlineKeyboardButton("« Отмена", callback_data='black_market')]
        ])
        await query.message.reply_text(
            "Как вы хотите разместить объявление?",
            reply_markup=keyboard
        )
        return

    elif callback_data in ['create_public', 'create_anon']:
        context.user_data['announcement_type'] = callback_data
        context.user_data['creating_announcement'] = True
        await query.message.reply_text(
            "Введите короткий информативный заголовок (до 50 символов):\n\n"
            "Вы можете отменить создание объявления командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_title'] = True
        return

    elif callback_data == 'send_notification':
        if not is_superadmin:
            await query.message.reply_text(
                "Только суперадминистратор может отправлять системные уведомления.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Запрашиваем подтверждение
        confirm_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Да, отправить", callback_data="confirm_send_notification"),
            InlineKeyboardButton("❌ Отмена", callback_data="settings")
        ]])
        
        await query.message.reply_text(
            "⚠️ <b>Внимание!</b>\n\n"
            "Вы уверены, что хотите отправить системное уведомление всем пользователям бота?\n"
            "Это действие нельзя отменить.",
            parse_mode='HTML',
            reply_markup=confirm_keyboard
        )
        return
        
    elif callback_data == 'confirm_send_notification':
        if not is_superadmin:
            await query.message.reply_text(
                "Только суперадминистратор может отправлять системные уведомления.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Создаем клавиатуру для возврата в меню
        back_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Вернуться в меню", callback_data="settings")
        ]])
            
        # Отправляем сообщение о начале процесса
        status_message = await query.message.reply_text(
            "⏳ Отправка уведомлений...\n"
            "Пожалуйста, подождите.",
            reply_markup=back_keyboard
        )
        
        try:
            success, success_count, fail_count = await send_notification_to_users(context.application)
            if success:
                total = success_count + fail_count
                await status_message.edit_text(
                    f"✅ Уведомления отправлены!\n\n"
                    f"📊 Статистика:\n"
                    f"• Успешно: {success_count}\n"
                    f"• Не удалось: {fail_count}\n"
                    f"• Всего получателей: {total}",
                    reply_markup=back_keyboard
                )
            else:
                await status_message.edit_text(
                    "❌ Произошла ошибка при отправке уведомлений.\n"
                    "Пожалуйста, попробуйте позже.",
                    reply_markup=back_keyboard
                )
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомлений: {e}")
            await status_message.edit_text(
                "❌ Произошла ошибка при отправке уведомлений.\n"
                "Пожалуйста, попробуйте позже.",
                reply_markup=back_keyboard
            )
        return

    elif callback_data == 'get_bot_log':
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        try:
            with open('bot.log', 'rb') as f:
                await query.message.reply_document(f, filename='bot.log')
        except Exception as e:
            logger.error(f"Ошибка при отправке лога: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при получении лога.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        return

    elif callback_data == 'notification_settings':
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Включить", callback_data='notifications_on'),
                InlineKeyboardButton("❌ Отключить", callback_data='notifications_off')
            ],
            [InlineKeyboardButton("« Назад", callback_data='notifications_menu')]
        ])
        await query.message.reply_text(
            "🔔 Настройка системных уведомлений\n\n"
            "Хотите ли вы получать уведомления о новых оценках и обновлениях?",
            reply_markup=keyboard
        )
        return

    elif callback_data in ['notifications_on', 'notifications_off']:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            new_value = 1 if callback_data == 'notifications_on' else 0
            cursor.execute('UPDATE students SET notifications=? WHERE telegram_id=?', (new_value, telegram_id))
            conn.commit()
            status = "включены" if new_value else "отключены"
            back_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("« Назад", callback_data="notifications_menu")
            ]])
            await query.message.reply_text(
                f"✅ Настройки сохранены!\n"
                f"Системные уведомления {status}.",
                reply_markup=back_keyboard
            )
        except Exception as e:
            logger.error(f"Ошибка при обновлении настроек уведомлений: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при сохранении настроек.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data == 'send_notification':
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        context.user_data['awaiting_notification'] = True
        await query.message.reply_text(
            "Введите текст уведомления:\n\n"
            "Вы можете отменить отправку командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data == 'notify_all':
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        context.user_data['notification_type'] = 'all'
        context.user_data['awaiting_notification'] = True
        await query.message.reply_text(
            "Введите текст уведомления для всех пользователей:\n\n"
            "Вы можете отменить отправку командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data == 'notify_group':
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT DISTINCT student_group FROM students WHERE student_group IS NOT NULL ORDER BY student_group')
            groups = cursor.fetchall()
            keyboard = []
            for (group,) in groups:
                keyboard.append([InlineKeyboardButton(group, callback_data=f'notify_group_{group}')])
            keyboard.append([InlineKeyboardButton("« Назад", callback_data='send_notification')])
            await query.message.reply_text(
                "Выберите группу для отправки уведомления:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Ошибка при получении списка групп: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении списка групп.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
        return

    elif callback_data.startswith('notify_group_'):
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        group = callback_data[len('notify_group_'):]
        context.user_data['notification_type'] = 'group'
        context.user_data['notification_group'] = group
        context.user_data['awaiting_notification'] = True
        await query.message.reply_text(
            f"Введите текст уведомления для группы {group}:\n\n"
            "Вы можете отменить отправку командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data == 'add_student':
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        context.user_data['awaiting_add_student_id'] = True
        await query.message.reply_text(
            "Введите номер студенческого билета студента, которого хотите добавить:\n\n"
            "Вы можете отменить действие командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data == 'add_admin':
        if not is_admin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        context.user_data['awaiting_add_admin_id'] = True
        await query.message.reply_text(
            "Введите номер студенческого билета пользователя, которого хотите сделать администратором:\n\n"
            "Вы можете отменить действие командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data == 'add_other_group_user':
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        context.user_data['awaiting_superadmin_student_id'] = True
        await query.message.reply_text(
            "Введите номер студенческого билета пользователя:\n\n"
            "Вы можете отменить действие командой /cancel",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    elif callback_data.startswith('student_'):
        student_id = callback_data.split('_')[1]
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM students WHERE student_id=?", (student_id,))
            row = cursor.fetchone()
            if not row:
                await query.message.reply_text(
                    "Студент не найден.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            columns = [desc[0] for desc in cursor.description]
            student_data = dict(zip(columns, row))
            name = student_data.get('name', 'Неизвестно')
            message = format_ratings_table(name, student_data)
            await query.message.reply_text(message, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
        except Exception as e:
            logger.error(f"Database error: {e}")
            await query.message.reply_text("Произошла ошибка при получении данных.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()
        return

    elif callback_data == 'set_week_type':
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        # Получаем текущие настройки из базы данных
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM bot_settings WHERE key=?', ('week_type',))
            result = cursor.fetchone()
            settings = json.loads(result[0]) if result else {'current_type': 'UP', 'auto_switch': True}
            
        current_type = settings['current_type']
        auto_switch = settings.get('auto_switch', True)
        last_change = settings.get('last_change', 'Неизвестно')
        
        keyboard = [
            [
                InlineKeyboardButton("⬆️ Задать верхнюю", callback_data='set_week_up'),
                InlineKeyboardButton("⬇️ Задать нижнюю", callback_data='set_week_down')
            ],
            [InlineKeyboardButton("« Назад", callback_data='settings')]
        ]
        
        await query.message.reply_text(
            f"📅 Управление типом недели\n\n"
            f"Текущий тип: {'Верхняя' if current_type == 'UP' else 'Нижняя'}\n"
            f"Авто-переключение: {'Включено' if auto_switch else 'Выключено'}\n"
            f"Последнее изменение: {last_change}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return
        
    elif callback_data in ['set_week_up', 'set_week_down']:
        if not is_superadmin:
            await query.message.reply_text(
                "У вас нет прав для выполнения этого действия.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        new_type = 'UP' if callback_data == 'set_week_up' else 'DOWN'
        set_week_type_settings(new_type=new_type)
        
        keyboard = [
            [
                InlineKeyboardButton("⬆️ Задать верхнюю", callback_data='set_week_up'),
                InlineKeyboardButton("⬇️ Задать нижнюю", callback_data='set_week_down')
            ],
            [InlineKeyboardButton("« Назад", callback_data='settings')]
        ]
        
        await query.message.reply_text(
            f"✅ Тип недели успешно изменен!\n\n"
            f"Текущий тип недели: {'Верхняя' if new_type == 'UP' else 'Нижняя'}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

@handle_telegram_timeout()
async def settings_menu(update, context):
    """Показывает меню настроек для суперадмина"""
    if not is_superadmin(update.effective_user.id):
        await update.message.reply_text("У вас нет доступа к этому разделу.")
        return

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM bot_settings WHERE key=?', ('week_type',))
        result = cursor.fetchone()
        settings = json.loads(result[0]) if result else {'current_type': 'UP', 'auto_switch': True}

    current_type = settings['current_type']
    auto_switch = settings.get('auto_switch', True)

    keyboard = [
        [
            InlineKeyboardButton("📅 Тип недели: " + ("Верхняя" if current_type == 'UP' else "Нижняя"), 
                               callback_data='toggle_week_type')
        ],
        [
            InlineKeyboardButton("🔄 Авто-переключение: " + ("Вкл." if auto_switch else "Выкл."), 
                               callback_data='toggle_auto_switch')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "⚙️ Настройки типа недели:\n\n"
        f"Текущий тип: {'Верхняя' if current_type == 'UP' else 'Нижняя'}\n"
        f"Авто-переключение: {'Включено' if auto_switch else 'Выключено'}",
        reply_markup=reply_markup
    )

@handle_telegram_timeout()
async def handle_settings_callback(update, context):
    """Обрабатывает нажатия на кнопки в меню настроек"""
    query = update.callback_query
    await query.answer()

    if not is_superadmin(query.from_user.id):
        await query.message.reply_text("У вас нет доступа к этому действию.")
        return

    if query.data == 'toggle_week_type':
        current_settings = set_week_type_settings(
            new_type='DOWN' if get_week_type() == 'UP' else 'UP'
        )
        new_type = current_settings['current_type']
        auto_switch = current_settings['auto_switch']

    elif query.data == 'toggle_auto_switch':
        current_settings = set_week_type_settings(
            auto_switch=not json.loads(
                get_db_connection().execute(
                    'SELECT value FROM bot_settings WHERE key=?', 
                    ('week_type',)
                ).fetchone()[0]
            )['auto_switch']
        )
        new_type = current_settings['current_type']
        auto_switch = current_settings['auto_switch']

    # Обновляем клавиатуру
    keyboard = [
        [
            InlineKeyboardButton("📅 Тип недели: " + ("Верхняя" if new_type == 'UP' else "Нижняя"), 
                               callback_data='toggle_week_type')
        ],
        [
            InlineKeyboardButton("🔄 Авто-переключение: " + ("Вкл." if auto_switch else "Выкл."), 
                               callback_data='toggle_auto_switch')
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.message.edit_text(
        "⚙️ Настройки типа недели:\n\n"
        f"Текущий тип: {'Верхняя' if new_type == 'UP' else 'Нижняя'}\n"
        f"Авто-переключение: {'Включено' if auto_switch else 'Выключено'}",
        reply_markup=reply_markup
    )