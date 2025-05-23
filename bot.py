import sqlite3
import requests
from bs4 import BeautifulSoup
import datetime
import json
import logging
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, BotCommand
import uuid
import re
from telegram.error import NetworkError

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration from config.json
with open('config.json') as config_file:
    config = json.load(config_file)

# Define the inline keyboard for the main menu
INLINE_KEYBOARD = [
    [
        InlineKeyboardButton("Мой Рейтинг", callback_data='my_rating'),
        InlineKeyboardButton("Группа", callback_data='group')
    ],
    [
        InlineKeyboardButton("Дисциплины", callback_data='disciplines'),
        InlineKeyboardButton("Мой Профиль", callback_data='settings')
    ]
]
INLINE_KEYBOARD_MARKUP = InlineKeyboardMarkup(INLINE_KEYBOARD)

# Define the persistent reply keyboard with the "В начало" button
REPLY_KEYBOARD = [['В начало']]
REPLY_KEYBOARD_MARKUP = ReplyKeyboardMarkup(REPLY_KEYBOARD, resize_keyboard=True, one_time_keyboard=False)

# Define cancel keyboard for registration
CANCEL_KEYBOARD = [[InlineKeyboardButton("Отмена", callback_data='cancel_registration')]]
CANCEL_KEYBOARD_MARKUP = InlineKeyboardMarkup(CANCEL_KEYBOARD)

def get_db_connection():
    return sqlite3.connect('students.db')

def get_all_subjects_from_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(students)")
    columns = cursor.fetchall()
    
    conn.close()
    
    subjects = []
    for col in columns:
        col_name = col[1]
        if col_name not in ['student_id', 'name', 'update_date', 'telegram_id', 'student_group', 'is_admin', 'backup_telegram_ids', 'last_parsed_time']:
            subject = col_name.split(' (модуль')[0]
            if subject not in subjects:
                subjects.append(subject)
    
    return subjects

def update_db_structure(new_subjects):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(students)")
    current_columns = [col[1] for col in cursor.fetchall()]
    
    for subject in new_subjects:
        for module in [' (модуль 1)', ' (модуль 2)']:
            col_name = f'"{subject}{module}"'
            if col_name not in current_columns:
                cursor.execute(f'ALTER TABLE students ADD COLUMN {col_name} TEXT DEFAULT "не изучает"')
    
    conn.commit()
    conn.close()

def get_subjects(soup):
    table = soup.find('table', id='user')
    if not table:
        return []
    rows = table.find_all('tr')
    if not rows or len(rows) < 1:
        return []
    headers = [header.text.strip() for header in rows[0].find_all('th')]
    return headers[1:-1] if len(headers) > 2 else []

def parse_student_data(student_id):
    url = f"http://vuz2.bru.by/rate/{student_id}/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')

        data_box = soup.find('div', class_='box data')
        if data_box:
            name_tag = data_box.find('h1')
            full_name = name_tag.text.strip() if name_tag else "Unknown"
        else:
            full_name = "Unknown"

        last_name = full_name.split()[0] if full_name != "Unknown" else "Unknown"

        subjects = get_subjects(soup)
        if not subjects:
            return last_name, {}, subjects

        table = soup.find('table', id='user')
        rows = table.find_all('tr')

        module1_row = next((row for row in rows if row.find('td') and row.find('td').text.strip() == '1-ый модуль'), None)
        module2_row = next((row for row in rows if row.find('td') and row.find('td').text.strip() == '2-ой модуль'), None)

        grades = {}
        if module1_row:
            module1_cells = module1_row.find_all('td')
            if len(module1_cells) > 1:
                for i, subject in enumerate(subjects):
                    grade = module1_cells[i + 1].text.strip() if i + 1 < len(module1_cells) else '-'
                    grades[f"{subject} (модуль 1)"] = int(grade) if grade.isdigit() else None
        if module2_row:
            module2_cells = module2_row.find_all('td')
            if len(module2_cells) > 1:
                for i, subject in enumerate(subjects):
                    grade = module2_cells[i + 1].text.strip() if i + 1 < len(module2_cells) else '-'
                    grades[f"{subject} (модуль 2)"] = int(grade) if grade.isdigit() else None

        return last_name, grades, subjects
    except Exception as e:
        logger.error(f"Error parsing student data for ID {student_id}: {e}")
        return "Unknown", {}, []

def save_to_db(student_id, name, grades, subjects, telegram_id=None, student_group=None, is_admin=False):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    parse_time = datetime.datetime.now().isoformat()
    
    data = {
        'student_id': student_id,
        'name': name,
        'update_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'last_parsed_time': parse_time
    }
    
    if telegram_id:
        data['telegram_id'] = telegram_id
    
    if student_group is not None:
        data['student_group'] = student_group
    if is_admin:
        data['is_admin'] = 1
    
    db_subjects = get_all_subjects_from_db()
    new_subjects = [s for s in subjects if s not in db_subjects]
    
    if new_subjects:
        update_db_structure(new_subjects)
    
    cursor.execute("PRAGMA table_info(students)")
    for col in cursor.fetchall():
        col_name = col[1]
        if col_name in data:
            continue
        if "(модуль" in col_name:
            subject = col_name.split(' (модуль')[0]
            if subject in subjects:
                data[col_name] = grades.get(col_name, None)
            else:
                data[col_name] = "не изучает"
    
    columns = [f'"{k}"' for k in data.keys()]
    query = f"""
    INSERT INTO students ({','.join(columns)}) 
    VALUES ({','.join(['?']*len(data))})
    ON CONFLICT(student_id) DO UPDATE SET {','.join([f'"{k}"=?' for k in data.keys()])}
    """
    
    cursor.execute(query, list(data.values())*2)
    conn.commit()
    conn.close()

async def check_registration(telegram_id):
    """
    Проверяет, зарегистрирован ли пользователь в базе данных.
    Возвращает: (is_registered, student_data), где student_data = (student_id, student_group, is_admin) или None.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT student_id, student_group, is_admin FROM students WHERE telegram_id=?', (telegram_id,))
        result = cursor.fetchone()
        return bool(result), result
    except Exception as e:
        logger.error(f"Error checking registration: {e}")
        return False, None
    finally:
        conn.close()

async def start_command(update, context):
    telegram_id = str(update.effective_user.id)
    log_event("start_command", telegram_id, message="User started bot")
    
    await update.message.reply_text(
        "Добро пожаловать! Я помогу вам следить за вашей успеваемостью.",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

async def menu_command(update, context):
    telegram_id = str(update.effective_user.id)
    log_event("menu_command", telegram_id, message="User requested menu")
    
    await update.message.reply_text(
        "Выберите действие:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

async def handle_start_button(update, context):
    logger.info("В начало reply button pressed")
    context.user_data.clear()
    await update.message.reply_text(
        "Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

async def show_student_rating(update, student_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT * FROM students WHERE student_id=?", (student_id,))
        row = cursor.fetchone()
        if not row:
            await update.message.reply_text("Студент не найден.")
            return
            
        columns = [desc[0] for desc in cursor.description]
        student_data = dict(zip(columns, row))
        name = student_data.get('name', 'Неизвестно')
        
        message = format_ratings_table(name, student_data)
        
        if hasattr(update, 'message'):
            await update.message.reply_text(message, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
        else:
            await update.reply_text(message, parse_mode='HTML', reply_markup=REPLY_KEYBOARD_MARKUP)
        
    except Exception as e:
        logger.error(f"Error showing student rating: {e}")
        if hasattr(update, 'message'):
            await update.message.reply_text("Произошла ошибка при получении данных.")
        else:
            await update.reply_text("Произошла ошибка при получении данных.")
    finally:
        conn.close()

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

    telegram_id = str(update.effective_user.id)
    log_event("button_click", telegram_id, message=f"Button clicked: {callback_data}")

    # Clear registration state before processing new command
    #context.user_data.clear()

    is_registered, student_data = await check_registration(telegram_id)
    student_id, student_group, is_admin = student_data if student_data else (None, None, False)

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
            await query.message.reply_text("Произошла ошибка при получении данных.")
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
            await query.message.reply_text("Произошла ошибка при обработке запроса.")
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

            # Удаляем старое значение, если есть
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
            await query.message.reply_text("Произошла ошибка при обработке запроса.")
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
                await query.message.reply_text("Произошла ошибка при получении данных.")
            finally:
                if conn:
                    conn.close()
        except Exception as inner_error:
            logger.error(f"Unexpected error in discipline handler: {inner_error}")

    elif callback_data.startswith('student_'):
        student_id = callback_data.split('_')[1]
        await show_student_rating(query, student_id)

    elif callback_data == 'settings':
        keyboard = [
            [InlineKeyboardButton("Регистрация", callback_data='register')],
            [InlineKeyboardButton("Сбросить настройки", callback_data='reset_settings')]
        ]
        if is_admin:
            keyboard.append([InlineKeyboardButton("Добавить админа", callback_data='add_admin')])
        await query.message.reply_text(
            "Настройки:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif callback_data == 'register':
        await query.message.reply_text(
            "Введите номер студенческого билета:",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_student_id'] = True

    elif callback_data == 'reset_settings':
        if not is_registered:
            await query.message.reply_text(
                "Вы не зарегистрированы в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
            
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM students WHERE student_group=? AND is_admin=1", (student_group,))
            admin_count = cursor.fetchone()[0]
            
            if is_admin and admin_count <= 1:
                await query.message.reply_text(
                    "Вы не можете сбросить настройки, так как являетесь единственным администратором группы.",
                    reply_markup=REPLY_KEYBOARD_MARKUP
                )
                return
                
            cursor.execute("UPDATE students SET telegram_id='added by admin', is_admin=0 WHERE telegram_id=?", (telegram_id,))
            conn.commit()
            context.user_data.clear()
            await query.message.reply_text(
                "Ваши настройки успешно сброшены. Вы больше не привязаны к боту.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
        except Exception as e:
            logger.error(f"Error resetting settings: {e}")
            await query.message.reply_text("Произошла ошибка при сбросе настроек.")
        finally:
            conn.close()

    elif callback_data == 'add_admin':
        if not is_registered or not is_admin:
            await query.message.reply_text(
                "Только администратор группы может добавлять новых администраторов.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        await query.message.reply_text(
            "Введите номер студенческого билета нового администратора:",
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
            "Введите номер студенческого билета:",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_admin_student_id'] = True

    elif callback_data == 'cancel_registration':
        context.user_data.clear()
        await query.message.reply_text(
            "Действие отменено. Выберите опцию:",
            reply_markup=INLINE_KEYBOARD_MARKUP
        )

def format_discipline_name(name, max_length=25):
    name = name.split(' (модуль')[0].strip()
    if len(name) > max_length:
        return name[:max_length-3] + '...'
    return name.ljust(max_length)

def format_ratings_table(name, data, is_group=False):
    if not is_group:
        disciplines = {}
        for key, value in data.items():
            if "(модуль" in str(key) and value not in ["не изучает", None, "None"]:
                parts = str(key).split(' (модуль ')
                if len(parts) == 2:
                    disc_name = parts[0]
                    module = parts[1].replace(')', '')
                    if disc_name not in disciplines:
                        disciplines[disc_name] = {'1': '-', '2': '-'}
                    disciplines[disc_name][module] = value

        table = f"<pre>Рейтинг {name}:\n"
        table += "="*45 + "\n"
        table += "Дисциплина".ljust(25) + " | М1 | М2\n"
        table += "-"*45 + "\n"
        
        for disc_name in sorted(disciplines.keys()):
            grades = disciplines[disc_name]
            formatted_name = format_discipline_name(disc_name)
            m1 = str(grades['1']) if '1' in grades and grades['1'] != '-' else ' -'
            m2 = str(grades['2']) if '2' in grades and grades['2'] != '-' else ' -'
            table += f"{formatted_name} | {m1.rjust(2)} | {m2.rjust(2)}\n"
        
        table += "</pre>"
        return table
    else:
        table = f"<pre>Успеваемость по дисциплине {name}:\n"
        table += "="*45 + "\n"
        table += "Студент".ljust(25) + " | М1 | М2\n"
        table += "-"*45 + "\n"
        
        for student_name, grades in data:
            m1 = str(grades[f"{name} (модуль 1)"]) if grades[f"{name} (модуль 1)"] != "-" else " -"
            m2 = str(grades[f"{name} (модуль 2)"]) if grades[f"{name} (модуль 2)"] != "-" else " -"
            formatted_name = student_name[:22] + "..." if len(student_name) > 25 else student_name.ljust(25)
            table += f"{formatted_name} | {m1.rjust(2)} | {m2.rjust(2)}\n"
        
        table += "</pre>"
        return table

def format_grade_changes_table(changes):
    if not changes:
        return "<pre>Нет изменений в рейтинге.</pre>"
    table = "<pre>Изменения в рейтинге:\n"
    table += "="*70 + "\n"
    table += "Дисциплина".ljust(40) + " | Модуль | Было | Стало\n"
    table += "-"*70 + "\n"
    for subject, old_grade, new_grade in changes:
        # subject: "Название (модуль X)"
        if "(модуль" in subject:
            disc, mod = subject.rsplit(' (модуль ', 1)
            mod = mod.replace(")", "")
        else:
            disc, mod = subject, "?"
        disc = disc[:37] + "..." if len(disc) > 40 else disc.ljust(40)
        old = str(old_grade) if old_grade not in [None, "None"] else "-"
        new = str(new_grade) if new_grade not in [None, "None"] else "-"
        table += f"{disc} |  {mod.ljust(6)}| {old.rjust(5)} | {new.rjust(5)}\n"
    table += "</pre>"
    return table

async def handle_message(update, context):
    text = update.message.text.strip()
    telegram_id = str(update.effective_user.id)
    log_event("message", telegram_id, message=text)

    if text == 'В начало':
        await handle_start_button(update, context)
        return

    if context.user_data.get('awaiting_student_id'):
        student_id = text
        name, grades, subjects = parse_student_data(student_id)
        if name == "Unknown":
            await update.message.reply_text(
                "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или попробуйте позже.",
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
            "Введите название вашей группы (например, ПМР-231):",
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
            await update.message.reply_text("Произошла ошибка при регистрации.")
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
                        "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или попробуйте позже.",
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
                    cursor.execute('UPDATE students SET is_admin=1 WHERE student_id=?', (student_id,))
                    conn.commit()
                    await update.message.reply_text(
                        f"Студент {name} назначен администратором группы {admin_group}!",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
        except Exception as e:
            logger.error(f"Error processing admin action: {e}")
            await update.message.reply_text("Произошла ошибка при выполнении действия.")
        finally:
            conn.close()
            context.user_data.clear()
        return

    await update.message.reply_text(
        "Пожалуйста, используйте кнопки меню.",
        reply_markup=REPLY_KEYBOARD_MARKUP
    )

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS students (
            student_id TEXT PRIMARY KEY,
            name TEXT,
            update_date TEXT,
            telegram_id TEXT,
            student_group TEXT,
            is_admin INTEGER DEFAULT 0,
            backup_telegram_ids TEXT DEFAULT '[]',
            last_parsed_time TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            event_type TEXT,
            user_id TEXT,
            student_id TEXT,
            message TEXT,
            data TEXT,
            FOREIGN KEY (student_id) REFERENCES students(student_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS grade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id TEXT,
            subject TEXT,
            module TEXT,
            old_grade TEXT,
            new_grade TEXT,
            timestamp TEXT,
            FOREIGN KEY (student_id) REFERENCES students(student_id)
        )
    ''')
    
    conn.commit()
    conn.close()

def log_event(event_type, user_id=None, student_id=None, message=None, data=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO logs (timestamp, event_type, user_id, student_id, message, data)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            datetime.datetime.now().isoformat(),
            event_type,
            user_id,
            student_id,
            message,
            json.dumps(data) if data else None
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"Error logging event: {e}")
    finally:
        conn.close()

def log_grade_change(student_id, subject, module, old_grade, new_grade):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO grade_history (student_id, subject, module, old_grade, new_grade, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            student_id,
            subject,
            module,
            old_grade,
            new_grade,
            datetime.datetime.now().isoformat()
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"Error logging grade change: {e}")
    finally:
        conn.close()

def get_student_grade_history(student_id, subject=None, module=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = '''
            SELECT * FROM grade_history 
            WHERE student_id = ?
        '''
        params = [student_id]
        
        if subject:
            query += ' AND subject = ?'
            params.append(subject)
        if module:
            query += ' AND module = ?'
            params.append(module)
            
        query += ' ORDER BY timestamp DESC'
        
        cursor.execute(query, params)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting grade history: {e}")
        return []
    finally:
        conn.close()

def get_last_grade_change(student_id, subject, module):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT * FROM grade_history 
            WHERE student_id = ? AND subject = ? AND module = ?
            ORDER BY timestamp DESC LIMIT 1
        ''', (student_id, subject, module))
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Error getting last grade change: {e}")
        return None
    finally:
        conn.close()

def check_grade_changes(student_id, new_grades):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM students WHERE student_id=?", (student_id,))
        row = cursor.fetchone()
        if not row:
            return []
            
        columns = [desc[0] for desc in cursor.description]
        old_data = dict(zip(columns, row))
        
        changes = []
        for subject in new_grades:
            if "(модуль" in subject:
                old_grade = old_data.get(subject)
                new_grade = new_grades[subject]

                # Приводим к строке для корректного сравнения
                old_str = str(old_grade) if old_grade is not None else ""
                new_str = str(new_grade) if new_grade is not None else ""

                if old_str != new_str and new_str not in ["не изучает", "", "None"]:
                    changes.append((subject, old_grade, new_grade))
                    log_grade_change(
                        student_id,
                        subject.split(' (модуль')[0], 
                        subject.split(' (модуль')[1].replace(')', ''), 
                        old_grade, new_grade
                    )
        
        return changes
    except Exception as e:
        logger.error(f"Error checking grade changes: {e}")
        return []
    finally:
        conn.close()

def get_group_members(student_group):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT student_id, telegram_id FROM students WHERE student_group=?', (student_group,))
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting group members: {e}")
        return []
    finally:
        conn.close()

async def notify_grade_changes(application, student_id, changes):
    if not changes:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT telegram_id, student_group FROM students WHERE student_id=?', (student_id,))
        result = cursor.fetchone()
        if not result:
            return
            
        telegram_id, student_group = result
        
        # Формируем красивую таблицу изменений
        message = format_grade_changes_table(changes)
        
        await application.bot.send_message(chat_id=telegram_id, text=message, parse_mode='HTML')
        
        # Получаем всех одногруппников
        group_members = get_group_members(student_group)
        for member_id, member_telegram_id in group_members:
            if member_id != student_id and member_telegram_id != "added by admin":
                await application.bot.send_message(
                    chat_id=member_telegram_id,
                    text=f"В группе {student_group} есть изменения в рейтинге. Используйте кнопку 'Мой Рейтинг' для просмотра."
                )
    except Exception as e:
        logger.error(f"Error notifying grade changes: {e}")
    finally:
        conn.close()

async def auto_parse_students(application):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Получаем всех студентов, у которых прошло более 4 часов с последнего парсинга
        four_hours_ago = (datetime.datetime.now() - datetime.timedelta(hours=4)).isoformat()
        cursor.execute('''
            SELECT student_id, telegram_id, student_group 
            FROM students 
            WHERE last_parsed_time < ? OR last_parsed_time IS NULL
        ''', (four_hours_ago,))
        
        students = cursor.fetchall()
        for student_id, telegram_id, student_group in students:
            try:
                # Парсим данные студента
                name, grades, subjects = parse_student_data(student_id)
                if name == "Unknown":
                    log_event("parse_error", telegram_id, student_id, "Failed to parse student data")
                    continue
                
                # Проверяем изменения в оценках
                changes = check_grade_changes(student_id, grades)
                
                # Сохраняем новые данные
                save_to_db(student_id, name, grades, subjects, telegram_id, student_group)
                
                # Уведомляем об изменениях
                if changes:
                    await notify_grade_changes(application, student_id, changes)
                    log_event("grade_changes", telegram_id, student_id, 
                             f"Changes detected: {len(changes)} subjects", 
                             {"changes": changes})
                
            except Exception as e:
                logger.error(f"Error processing student {student_id}: {e}")
                log_event("parse_error", telegram_id, student_id, str(e))
                
    except Exception as e:
        logger.error(f"Error in auto_parse_students: {e}")
    finally:
        conn.close()

async def set_bot_commands(application):
    commands = [
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="menu", description="Показать меню")
    ]
    try:
        await application.bot.set_my_commands(commands)
        logger.info("Bot commands set successfully")
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")

async def error_handler(update, context):
    logger.error(f"Exception: {context.error}")

def main():
    init_db()
    application = Application.builder().token(config["TOKEN"]).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(CallbackQueryHandler(handle_inline_buttons))

    application.job_queue.run_once(set_bot_commands, 0)
    
    # Запускаем автоматический парсинг каждые 4 часа
    application.job_queue.run_repeating(auto_parse_students, interval=500, first=10)

    application.add_error_handler(error_handler)

    logger.info("Starting bot polling")
    application.run_polling()

if __name__ == "__main__":
    main()