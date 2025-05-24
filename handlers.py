import re
from utils import (
    logger, get_db_connection, check_registration, parse_student_data, save_to_db,
    show_student_rating, format_ratings_table, REPLY_KEYBOARD_MARKUP,
    CANCEL_KEYBOARD_MARKUP, INLINE_KEYBOARD_MARKUP
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

async def handle_message(update, context):
    text = update.message.text.strip()
    logger.info(f"Received message: {text}")

    if text == '🏠 Главное меню':
        logger.info("Нажата кнопка '🏠 Главное меню'")
        context.user_data.clear()
        await update.message.reply_text(
            "Вы вернулись в главное меню! Выберите опцию:",
            reply_markup=INLINE_KEYBOARD_MARKUP
        )
        return

    telegram_id = str(update.effective_user.id)
    
    if context.user_data.get('awaiting_student_id'):
        student_id = text
        name, grades, subjects = parse_student_data(student_id)
        if name == "Unknown":
            await update.message.reply_text(
                "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или сервер VUZ2 не отвечает. Попробуйте позже.\n\nВы можете отменить действие командой /cancel.",
                reply_markup=CANCEL_KEYBOARD_MARKUP
            )
            return
        context.user_data['temp_student_id'] = student_id
        context.user_data['temp_name'] = name
        context.user_data['temp_grades'] = grades
        context.user_data['temp_subjects'] = subjects
        context.user_data['awaiting_student_id'] = False
        context.user_data['awaiting_group'] = True
        await update.message.reply_text(
            "Введите название вашей группы (например, ПМР-231):\n\nВы можете отменить действие командой /cancel.",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        return

    if context.user_data.get('awaiting_group'):
        student_group = text.upper()
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT COUNT(*) FROM students WHERE student_group=?', (student_group,))
            group_exists = cursor.fetchone()[0] > 0
            is_admin = not group_exists

            save_to_db(
                student_id=context.user_data['temp_student_id'],
                name=context.user_data['temp_name'],
                grades=context.user_data['temp_grades'],
                subjects=context.user_data['temp_subjects'],
                telegram_id=telegram_id,
                student_group=student_group,
                is_admin=is_admin
            )

            context.user_data.clear()
            await update.message.reply_text(
                f"Регистрация завершена! Вы {'стали администратором' if is_admin else 'добавлены в'} группу {student_group}.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        except Exception as e:
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
                name, grades, subjects = parse_student_data(student_id)
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

    await update.message.reply_text(
        "Пожалуйста, используйте кнопки меню.\n\nВы можете вернуться в главное меню командой /cancel.",
        reply_markup=REPLY_KEYBOARD_MARKUP
    )

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

    logger.info(f"Inline button pressed: {callback_data}")
    telegram_id = str(update.effective_user.id)

    is_registered, student_data = await check_registration(telegram_id)
    student_id, student_group, is_admin = student_data if student_data else (None, None, False)

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
            logger.error(f"Database error in group handler: {e}")
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
            sanitized_disciplines = []
            for disc in sorted(disciplines):
                safe_disc = re.sub(r'[^\w\s-]', '', disc).strip()
                safe_disc = re.sub(r'\s+', '_', safe_disc)
                callback_data = f"discipline_{safe_disc}"
                encoded = callback_data.encode('utf-8')
                while len(encoded) > 63 and len(safe_disc) > 5:
                    safe_disc = safe_disc[:-1]
                    callback_data = f"discipline_{safe_disc}"
                    encoded = callback_data.encode('utf-8')
                if safe_disc and len(encoded) <= 64:
                    sanitized_disciplines.append((disc, safe_disc))
                    logger.info(f"Discipline: {disc} -> Sanitized: {safe_disc}, bytes: {len(encoded)}")
                else:
                    logger.warning(f"Пропущена дисциплина: {disc} (слишком длинное имя)")
            if not sanitized_disciplines:
                await query.message.reply_text(
                    "Не удалось загрузить дисциплины. Проверьте их названия.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            context.user_data.pop('discipline_map', None)
            context.user_data['discipline_map'] = {
                f"discipline_{safe_disc}": disc for disc, safe_disc in sanitized_disciplines
            }
            keyboard = [
                [InlineKeyboardButton(disc, callback_data=f"discipline_{safe_disc}")]
                for disc, safe_disc in sanitized_disciplines
            ]
            await query.message.reply_text(
                "Ваши дисциплины:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Database error in disciplines handler: {e}")
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
            discipline_key = callback_data
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
                module1_col = f'"{discipline_name} (модуль 1)"'
                module2_col = f'"{discipline_name} (модуль 2)"'
                sql_query = f'SELECT student_id, name, {module1_col}, {module2_col} FROM students WHERE student_group=? ORDER BY name'
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
                await query.message.reply_text(message, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
            except Exception as e:
                logger.error(f"Error displaying discipline ratings: {e}")
                await query.message.reply_text("Произошла ошибка при получении данных.\n\nВы можете вернуться в главное меню командой /cancel.")
            finally:
                if conn:
                    conn.close()
        except Exception as inner_error:
            logger.error(f"Unexpected error in discipline handler: {inner_error}")

    elif callback_data.startswith('student_'):
        student_id = callback_data.split('_')[1]
        await show_student_rating(query, student_id)

    elif callback_data == 'settings':
        keyboard = []
        if is_admin:
            keyboard.append([InlineKeyboardButton("Добавить админа", callback_data='add_admin')])
        await query.message.reply_text(
            "Настройки:",
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else REPLY_KEYBOARD_MARKUP
        )

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
        context.user_data['awaiting_admin_student_id'] = True

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
        context.user_data['awaiting_admin_student_id'] = True

    elif callback_data == 'cancel_registration':
        context.user_data.clear()
        await query.message.reply_text(
            "Действие отменено. Вы вернулись в главное меню! Выберите опцию:",
            reply_markup=INLINE_KEYBOARD_MARKUP
        )