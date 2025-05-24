import re
from utils import (
    logger, get_db_connection, check_registration, parse_student_data, save_to_db,
    show_student_rating, format_ratings_table, REPLY_KEYBOARD_MARKUP,
    CANCEL_KEYBOARD_MARKUP, INLINE_KEYBOARD_MARKUP
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

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
            logger.error(f"Error processing add student action: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении действия.\n\nВы можете вернуться в главное меню командой /cancel.")
        finally:
            conn.close()
            context.user_data.clear()
        return

    if context.user_data.get('awaiting_superadmin_student_id'):
        student_id = update.message.text.strip()
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
            context.user_data.pop('superadmin_registration_in_progress', None)
            await update.message.reply_text(
                f"Пользователь {name} успешно добавлен в группу {student_group}.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        except Exception as e:
            context.user_data.pop('superadmin_registration_in_progress', None)
            logger.error(f"Ошибка при добавлении пользователя суперадмином: {e}")
            await update.message.reply_text(
                "Произошла ошибка при добавлении пользователя.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        context.user_data.clear()
        return

    # Убираем универсальное сообщение 'Пожалуйста, используйте кнопки меню...'
    return

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
                logger.error(f"Error displaying discipline ratings: {e}")
                await query.message.reply_text("Произошла ошибка при получении данных.\n\nВы можете вернуться в главное меню командой /cancel.")
            finally:
                if conn:
                    conn.close()
        except Exception as inner_error:
            logger.error(f"Unexpected error in discipline handler: {inner_error}")

    elif callback_data.startswith('courseworks_'):
        # Показываем список курсовых работ по дисциплине
        discipline_key = callback_data[len('courseworks_'):]
        discipline_name = context.user_data.get('discipline_map', {}).get(discipline_key)
        if not discipline_name:
            await query.message.reply_text(
                "Ошибка: дисциплина не найдена. Попробуйте снова.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Показываем все курсовые по дисциплине (без фильтра по группе)
            cursor.execute('SELECT name, discipline, file_path, semester, student_group FROM course_works WHERE TRIM(LOWER(discipline))=TRIM(LOWER(?))', (discipline_name,))
            course_works = cursor.fetchall()
            if not course_works:
                await query.message.reply_text(
                    "Курсовые работы по этой дисциплине не найдены.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
            buttons = []
            coursework_map = {}
            for idx, (name, discipline, file_path, semester, student_group) in enumerate(course_works, 1):
                filename = file_path.split('/')[-1]
                # Обрезаем ФИО (группа) и имя файла, чтобы кнопка всегда помещалась
                fio_group = f"{name} ({student_group})"
                if len(fio_group) > 25:
                    fio_group = fio_group[:22] + '...'
                filename_short = filename
                if len(filename_short) > 30:
                    filename_short = filename_short[:27] + '...'
                btn_text = f"{fio_group}\n{filename_short}"
                cw_key = f"cw{idx}"
                coursework_map[cw_key] = file_path
                buttons.append([InlineKeyboardButton(btn_text, callback_data=f"getcw_{cw_key}")])
            # Кнопка для скачивания всех работ архивом (реализация архивации потребуется отдельно)
            buttons.append([InlineKeyboardButton("Скачать все архивом", callback_data=f"getcwzip_{discipline_key}")])
            # Сохраняем map в user_data
            context.user_data['coursework_map'] = coursework_map
            await query.message.reply_text(
                f"<b>Курсовые работы по дисциплине {discipline_name}:</b>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        except Exception as e:
            logger.error(f"Ошибка при получении курсовых работ: {e}")
            await query.message.reply_text(
                "Произошла ошибка при получении курсовых работ.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        finally:
            conn.close()
    elif callback_data.startswith('getcw_'):
        # Отправка отдельной курсовой работы
        cw_key = callback_data[len('getcw_'):]
        file_path = context.user_data.get('coursework_map', {}).get(cw_key)
        if not file_path:
            await query.message.reply_text(
                "Ошибка: файл не найден. Попробуйте снова.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        # TODO: реализовать отправку файла пользователю
        await query.message.reply_text(
            f"Файл: {file_path} (отправка файла будет реализована)",
            reply_markup=REPLY_KEYBOARD_MARKUP
        )

    elif callback_data.startswith('getcwzip_'):
        # Заглушка: отправка архива всех курсовых работ по дисциплине
        discipline_key = callback_data[len('getcwzip_'):]
        # TODO: реализовать сбор и отправку архива
        await query.message.reply_text(
            f"Архив курсовых работ по дисциплине {discipline_key} (отправка архива будет реализована)",
            reply_markup=REPLY_KEYBOARD_MARKUP
        )
    elif callback_data == 'settings':
        # Получаем подробную информацию о пользователе
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT name, student_group, is_admin, is_superadmin, student_id, telegram_id FROM students WHERE telegram_id=?', (telegram_id,))
            user_row = cursor.fetchone()
            if user_row:
                name, group, is_admin, is_superadmin, student_id_val, user_telegram_id = user_row
                status = "Суперадмин" if is_superadmin else ("Админ группы" if is_admin else "Студент")
                # Ищем администратора группы
                cursor.execute('SELECT name, telegram_id FROM students WHERE student_group=? AND is_admin=1', (group,))
                admin_row = cursor.fetchone()
                if admin_row:
                    admin_name, admin_telegram_id = admin_row
                    admin_info = f"\n<b>Администратор группы:</b> {admin_name} (Telegram ID: {admin_telegram_id})"
                else:
                    admin_info = "\n<b>Администратор группы:</b> не найден"
                profile_text = (
                    f"<b>Ваш профиль</b>\n"
                    f"ФИО: {name}\n"
                    f"Группа: {group}\n"
                    f"ID: {student_id_val}\n"
                    f"Статус: {status}"
                    f"{admin_info}"
                )
            else:
                profile_text = "Профиль не найден. Зарегистрируйтесь через кнопку Мой Профиль."
        except Exception as e:
            logger.error(f"Ошибка при получении профиля: {e}")
            profile_text = "Ошибка при получении профиля."
        finally:
            conn.close()
        await query.message.reply_text(profile_text, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
        # Кнопки управления
        keyboard = []
        if is_admin:
            keyboard.append([InlineKeyboardButton("Добавить админа", callback_data='add_admin')])
        if is_superadmin:
            keyboard.append([InlineKeyboardButton("Добавить пользователя другой группы", callback_data='add_other_group_user')])
        if keyboard:
            await query.message.reply_text(
                "Настройки:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
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

    # Обработка других кнопок
    # Просто игнорируем неизвестные callback_data без вывода сообщения
    return
