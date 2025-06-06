import re
import os
import json
import zipfile
import tempfile
import asyncio
from utils import (
    logger, get_db_connection, check_registration, parse_student_data, save_to_db,
    show_student_rating, format_ratings_table, REPLY_KEYBOARD_MARKUP,
    CANCEL_KEYBOARD_MARKUP, INLINE_KEYBOARD_MARKUP, validate_student_id, validate_group_format, validate_student_group, handle_telegram_timeout,
    send_notification_to_users
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from archive_manager import CourseWorkArchiveManager

@handle_telegram_timeout()
async def handle_message(update, context):
    text = update.message.text.strip()
    user_id = update.effective_user.id
    logger.info(f"Получено сообщение от пользователя {user_id}: {text}")

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

    # Убираем универсальное сообщение 'Пожалуйста, используйте кнопки меню...'
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
        # Получаем подробную информацию о пользователе
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
        except Exception as e:
            logger.error(f"Ошибка при получении профиля (user_id: {update.effective_user.id}): {e}")
            profile_text = "Ошибка при получении профиля."
        finally:
            conn.close()
        await query.message.reply_text(profile_text, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
        # Кнопки управления
        keyboard = []
        keyboard.append([InlineKeyboardButton("Настройка системных уведомлений", callback_data='notification_settings')])
        if is_admin:
            keyboard.append([InlineKeyboardButton("Добавить админа", callback_data='add_admin')])
        if is_superadmin:
            keyboard.append([InlineKeyboardButton("Добавить пользователя другой группы", callback_data='add_other_group_user')])
            keyboard.append([InlineKeyboardButton("Отправить системное уведомление", callback_data='send_notification')])
            keyboard.append([InlineKeyboardButton("Получить лог бота", callback_data='get_bot_log')])
        if keyboard:
            await query.message.reply_text(
                "Настройки:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        return

    elif callback_data == 'notification_settings':
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Включить", callback_data='notifications_on'),
                InlineKeyboardButton("❌ Отключить", callback_data='notifications_off')
            ]
        ])
        await query.message.reply_text(
            "🔔 Настройка уведомлений\n\n"
            "Хотите ли вы получать уведомления о важных обновлениях функционала бота?\n\n"
            "• Информация об улучшениях\n"
            "• Уведомления о технических работах\n"
            "• Важные объявления",
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
                InlineKeyboardButton("Вернуться в профиль", callback_data="settings")
            ]])
            await query.message.reply_text(
                f"✅ Настройки сохранены!\n"
                f"Уведомления {status}.",
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

    elif callback_data == 'add_other_group_user':
        if not is_superadmin:
            await query.message.reply_text(
                "Только суперадминистратор может добавлять пользователей в другие группы.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        await query.message.reply_text(
            "Введите номер студенческого билета пользователя, которого хотите добавить в любую группу:\n\nВы можете отменить действие командой /cancel.",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_superadmin_student_id'] = True
        return
    elif callback_data == 'add_admin':
        if not is_registered or not is_admin:
            await query.message.reply_text(
                "Только администратор группы может добавлять новых администраторов.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        await query.message.reply_text(
            "Введите номер студенческого билета нового администратора:\n\nВы можете отменить действие командой /cancel.",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_add_admin_id'] = True
        return
    elif callback_data == 'add_student':
        if not is_admin:
            await query.message.reply_text(
                "Только администратор группы может добавлять студентов.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        await query.message.reply_text(
            "Введите номер студенческого билета:\n\nВы можете отменить действие командой /cancel.",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_add_student_id'] = True
        return
    elif callback_data.startswith('student_'):
        student_id = callback_data.split('_')[1]
        await show_student_rating(query, student_id)
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
                "Только суперадминистратор может получать лог бота.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        try:
            log_path = 'bot.log'
            if os.path.exists(log_path):
                with open(log_path, 'rb') as f:
                    await query.message.reply_document(
                        f,
                        filename='bot.log',
                        caption="✅ Лог бота"
                    )
            else:
                await query.message.reply_text(
                    "❌ Файл лога не найден.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
        except Exception as e:
            logger.error(f"Ошибка при отправке лога: {e}")
            await query.message.reply_text(
                "❌ Произошла ошибка при получении лога.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        return

    # Обработка других кнопок
    # Просто игнорируем неизвестные callback_data без вывода сообщения
    return

async def notify_superadmins(application, message):
    """Отправляет уведомление всем суперадминам"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT telegram_id FROM students WHERE is_superadmin=1 AND telegram_id IS NOT NULL AND telegram_id != "added by admin"')
        superadmins = cursor.fetchall()
        for (admin_id,) in superadmins:
            try:
                await application.bot.send_message(chat_id=admin_id, text=message, parse_mode='HTML')
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления суперадмину {admin_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при получении списка суперадминов: {e}")
    finally:
        conn.close()
