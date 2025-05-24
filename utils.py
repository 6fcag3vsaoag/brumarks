import sqlite3
import requests
import os
from bs4 import BeautifulSoup
import datetime
import json
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
import re
from functools import wraps
import asyncio

# Logging configuration
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

file_handler = logging.FileHandler('bot.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # Clear any existing handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Load configuration
with open('config.json') as config_file:
    config = json.load(config_file)

# Keyboards
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

REPLY_KEYBOARD = [['🏠 Главное меню']]
REPLY_KEYBOARD_MARKUP = ReplyKeyboardMarkup(REPLY_KEYBOARD, resize_keyboard=True, one_time_keyboard=False)

CANCEL_KEYBOARD = [[InlineKeyboardButton("Отмена", callback_data='cancel_registration')]]
CANCEL_KEYBOARD_MARKUP = InlineKeyboardMarkup(CANCEL_KEYBOARD)

# Directory for course work files
COURSE_WORKS_DIR = 'course_works'
if not os.path.exists(COURSE_WORKS_DIR):
    os.makedirs(COURSE_WORKS_DIR)

# Database functions
def get_db_connection():
    """
    Получение соединения с базой данных SQLite.
    Необходимо закрывать соединение после использования (используйте with).
    """
    return sqlite3.connect('students.db')

def require_registration(async_func):
    @wraps(async_func)
    async def wrapper(update, context, *args, **kwargs):
        telegram_id = str(update.effective_user.id)
        is_registered, student_data = await check_registration(telegram_id)
        if not is_registered:
            await update.message.reply_text(
                "Вы не зарегистрированы, воспользуйтесь кнопкой Мой Профиль для регистрации в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        return await async_func(update, context, *args, **kwargs)
    return wrapper

def get_all_subjects_from_db():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(students)")
        columns = cursor.fetchall()
    subjects = []
    for col in columns:
        col_name = col[1]
        if col_name not in ['student_id', 'name', 'update_date', 'telegram_id', 'student_group', 'is_admin', 'backup_telegram_ids', 'last_parsed_time']:
            subject = col_name.split(' (модуль')[0]
            if subject not in subjects:
                subjects.append(subject)
    return subjects

def update_db_structure(new_subjects):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(students)")
        current_columns = [col[1] for col in cursor.fetchall()]
        for subject in new_subjects:
            for module in [' (модуль 1)', ' (модуль 2)']:
                col_name = f'{subject}{module}'
                if col_name not in current_columns:
                    safe_col_name = '"' + col_name.replace('"', '""') + '"'
                    cursor.execute(f'ALTER TABLE students ADD COLUMN {safe_col_name} TEXT DEFAULT "не изучает"')
        conn.commit()

def get_subjects(soup):
    table = soup.find('table', id='user')
    if not table:
        return []
    rows = table.find_all('tr')
    if not rows or len(rows) < 1:
        return []
    headers = [header.text.strip() for header in rows[0].find_all('th')]
    return headers[1:-1] if len(headers) > 2 else []

def download_course_work_file(url, student_id, semester):
    """
    Download a course work file and save it to the course_works directory.
    Returns the local file path or None if download fails.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        original_filename = os.path.basename(url)
        # Create a unique filename to avoid conflicts
        base, ext = os.path.splitext(original_filename)
        unique_filename = f"{base}{ext}"
        file_path = os.path.join(COURSE_WORKS_DIR, unique_filename)
        with open(file_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded course work file: {file_path}")
        return file_path
    except Exception as e:
        logger.error(f"Error downloading course work file from {url}: {e}")
        return None

def save_course_work_to_db(student_id, name, telegram_id, student_group, discipline, file_path, semester):
    """
    Save course work details to the course_works table.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        parsing_time = datetime.datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO course_works (discipline, student_id, telegram_id, name, student_group, semester, file_path, parsing_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (discipline, student_id, telegram_id, name, student_group, semester, file_path, parsing_time))
        conn.commit()
        logger.info(f"Saved course work for student_id {student_id}, discipline {discipline}")

def parse_student_data(student_id, telegram_id=None, student_group=None):
    """
    Parse student data and course works from VUZ2 website.
    Returns: (name, grades, subjects, course_works)
    """
    # Parse student performance data
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
        last_name = full_name.split()[0] if full_name != "Unknown" and len(full_name.split()) > 0 else "Unknown"
        subjects = get_subjects(soup)
        if not subjects:
            grades = {}
        else:
            table = soup.find('table', id='user')
            rows = table.find_all('tr')
            grades = {}
            module_map = {'1-ый модуль': '1', '2-ой модуль': '2'}
            for module_label, module_num in module_map.items():
                module_row = next((row for row in rows if row.find('td') and row.find('td').text.strip() == module_label), None)
                if module_row:
                    module_cells = module_row.find_all('td')
                    if len(module_cells) > 1:
                        for i, subject in enumerate(subjects):
                            grade = module_cells[i + 1].text.strip() if i + 1 < len(module_cells) else '-'
                            grades[f"{subject} (модуль {module_num})"] = int(grade) if grade.isdigit() else None
    except Exception as e:
        logger.error(f"Ошибка при парсинге данных студента для ID {student_id}: {e}")
        return "Unknown", {}, [], []

    # Parse course work data
    course_works = []
    portfolio_url = f"http://vuz2.bru.by/rate/{student_id}/portfolio/1/"
    try:
        response = requests.get(portfolio_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
        portfolio_section = soup.find('div', class_='box data')
        if portfolio_section:
            ul = portfolio_section.find('ul')
            if ul:
                for li in ul.find_all('li'):
                    text = li.text.strip()
                    semester_match = re.search(r'Семестр: (\d+)', text)
                    discipline_match = re.search(r'Дисциплина: ([^\<]+?)(?=\s*\<a|$)', text)
                    if semester_match and discipline_match:
                        semester = semester_match.group(1)
                        discipline = discipline_match.group(1).strip()
                        file_link = li.find('a')
                        if file_link and 'href' in file_link.attrs:
                            file_url = file_link['href']
                            if not file_url.startswith('http'):
                                file_url = f"http://vuz2.bru.by{file_url}"
                            file_path = download_course_work_file(file_url, student_id, semester)
                            if file_path:
                                course_works.append({
                                    'discipline': discipline,
                                    'semester': semester,
                                    'file_path': file_path
                                })
                                # Save to course_works table
                                save_course_work_to_db(
                                    student_id=student_id,
                                    name=full_name,
                                    telegram_id=telegram_id or "added by admin",
                                    student_group=student_group,
                                    discipline=discipline,
                                    file_path=file_path,
                                    semester=semester
                                )
    except Exception as e:
        logger.error(f"Ошибка при парсинге курсовых работ для ID {student_id}: {e}")

    return last_name, grades, subjects, course_works

def save_to_db(student_id, name, grades, subjects, telegram_id=None, student_group=None, is_admin=False):
    with get_db_connection() as conn:
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
        columns = [f'"{k.replace("\"", "\"\"")}"' for k in data.keys()]
        query = f"""
        INSERT INTO students ({','.join(columns)}) 
        VALUES ({','.join(['?']*len(data))})
        ON CONFLICT(student_id) DO UPDATE SET {','.join([f'"{k.replace("\"", "\"\"")}"=?' for k in data.keys()])}
        """
        cursor.execute(query, list(data.values())*2)
        conn.commit()

async def save_to_db_async(*args, **kwargs):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, save_to_db, *args, **kwargs)

async def check_registration(telegram_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT student_id, student_group, is_admin FROM students WHERE telegram_id=?', (telegram_id,))
        result = cursor.fetchone()
        return bool(result), result
    except Exception as e:
        logger.error(f"Ошибка при проверке регистрации: {e}")
        return False, None
    finally:
        conn.close()

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
        logger.error(f"Ошибка при показе рейтинга студента: {e}")
        if hasattr(update, 'message'):
            await update.message.reply_text("Произошла ошибка при получении данных.")
        else:
            await update.reply_text("Произошла ошибка при получении данных.")
    finally:
        conn.close()