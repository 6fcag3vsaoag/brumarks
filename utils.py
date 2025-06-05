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
import functools
from telegram.error import TimedOut, NetworkError
import random
import traceback

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
        file_path = os.path.normpath(file_path)
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
    Skip if the work already exists.
    """
    file_path = os.path.normpath(file_path) if file_path else file_path
    with get_db_connection() as conn:
        cursor = conn.cursor()
        # Проверяем существование записи
        cursor.execute('''
            SELECT 1 FROM course_works 
            WHERE student_id = ? AND discipline = ? AND semester = ?
        ''', (student_id, discipline, semester))
        if cursor.fetchone():
            logger.info(f"Пропускаем сохранение существующей курсовой работы: student_id {student_id}, discipline {discipline}, semester {semester}")
            return
            
        parsing_time = datetime.datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO course_works (discipline, student_id, telegram_id, name, student_group, semester, file_path, parsing_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (discipline, student_id, telegram_id, name, student_group, semester, file_path, parsing_time))
        conn.commit()
        logger.info(f"Saved course work for student_id {student_id}, discipline {discipline}")

def validate_student_id(student_id):
    """
    Проверяет валидность номера студенческого билета.
    Returns: (is_valid, error_message)
    """
    # Проверяем, что student_id является числом
    if not student_id.isdigit():
        return False, "Номер студенческого билета должен содержать только цифры"
    
    # Проверяем длину (обычно 8 цифр)
    if len(student_id) != 8:
        return False, "Номер студенческого билета должен содержать 8 цифр"
    
    return True, None

def validate_group_format(group):
    """
    Проверяет формат названия группы.
    Формат: 2-6 заглавных русских букв, тире, 2-3 цифры
    Returns: (is_valid, error_message)
    """
    pattern = r'^[А-Я]{2,6}-\d{2,3}$'
    if not re.match(pattern, group):
        return False, "Неверный формат группы. Примеры правильного формата: ПМР-231, БИОР-221"
    return True, None

def validate_student_group(student_id, group):
    """
    Проверяет существование студента на сайте VUZ2.
    Returns: (is_valid, error_message)
    """
    url = f"http://vuz2.bru.by/rate/{student_id}/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
        
        # Проверяем наличие сообщения об ошибке
        error_message = soup.find('h2')
        if error_message and "не найден" in error_message.text:
            return False, "Студент не найден в системе VUZ2"
            
        return True, None
    except Exception as e:
        logger.error(f"Ошибка при проверке студента: {e}")
        return False, "Ошибка при проверке студента"

def parse_student_data(student_id, telegram_id=None, student_group=None, skip_existing_course_works=None):
    """
    Parse student data and course works from VUZ2 website.
    Returns: (name, grades, subjects, course_works)
    """
    # Валидация student_id
    is_valid, error_message = validate_student_id(student_id)
    if not is_valid:
        logger.error(f"Невалидный student_id: {student_id} - {error_message}")
        return "Unknown", {}, [], []

    # Parse student performance data
    url = f"http://vuz2.bru.by/rate/{student_id}/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
        
        # Проверяем наличие сообщения об ошибке
        error_message = soup.find('h2')
        if error_message and "не найден" in error_message.text:
            logger.error(f"Студент с номером {student_id} не найден в системе VUZ2")
            return "Unknown", {}, [], []
            
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
                    # Получаем текст до первого <a> (только описание, без имени файла)
                    li_text = ''
                    for content in li.contents:
                        if getattr(content, 'name', None) == 'a':
                            break
                        if isinstance(content, str):
                            li_text += content
                    semester_match = re.search(r'Семестр: (\d+)', li_text)
                    discipline_match = re.search(r'Дисциплина: (.*)', li_text)
                    if semester_match and discipline_match:
                        semester = semester_match.group(1)
                        discipline = discipline_match.group(1).strip()
                        
                        # Проверяем существование курсовой работы
                        if skip_existing_course_works and (discipline, semester) in skip_existing_course_works:
                            logger.info(f"Пропускаем существующую курсовую работу: {discipline}, семестр {semester}")
                            # Добавляем существующую работу в список без скачивания
                            course_works.append({
                                'discipline': discipline,
                                'semester': semester,
                                'file_path': skip_existing_course_works[(discipline, semester)]
                            })
                            continue
                            
                        # Проверяем существование в базе данных
                        with get_db_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute('''
                                SELECT file_path FROM course_works 
                                WHERE student_id = ? AND discipline = ? AND semester = ?
                            ''', (student_id, discipline, semester))
                            existing = cursor.fetchone()
                            if existing:
                                logger.info(f"Пропускаем скачивание существующей курсовой работы: {discipline}, семестр {semester}")
                                # Обновляем telegram_id для существующей записи
                                if telegram_id:
                                    cursor.execute('''
                                        UPDATE course_works 
                                        SET telegram_id = ? 
                                        WHERE student_id = ? AND discipline = ? AND semester = ?
                                    ''', (telegram_id, student_id, discipline, semester))
                                    conn.commit()
                                    logger.info(f"Обновлен telegram_id для курсовой работы: {discipline}, семестр {semester}")
                                course_works.append({
                                    'discipline': discipline,
                                    'semester': semester,
                                    'file_path': existing[0]
                                })
                                continue
                            
                        # Если работа не существует, скачиваем её
                        file_link = li.find('a')
                        if file_link and 'href' in file_link.attrs:
                            file_url = file_link['href']
                            if not file_url.startswith('http'):
                                file_url = f"http://vuz2.bru.by{file_url}"
                            file_path = download_course_work_file(file_url, student_id, semester)
                            if file_path:
                                # Сохраняем информацию о курсовой работе в базу данных
                                save_course_work_to_db(
                                    student_id=student_id,
                                    name=full_name,
                                    telegram_id=telegram_id,
                                    student_group=student_group,
                                    discipline=discipline,
                                    file_path=file_path,
                                    semester=semester
                                )
                                course_works.append({
                                    'discipline': discipline,
                                    'semester': semester,
                                    'file_path': file_path
                                })
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
            'last_parsed_time': parse_time,
            'notifications': 1  # Добавляем поле notifications со значением 1 по умолчанию
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
        columns = ['"{}"'.format(k.replace('"', '""')) for k in data.keys()]
        query = f"""
        INSERT INTO students ({','.join(columns)}) 
        VALUES ({','.join(['?']*len(data))})
        ON CONFLICT(student_id) DO UPDATE SET {','.join(['"{}"=?'.format(k.replace('"', '""')) for k in data.keys()])}
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

async def retry_on_timeout(func, max_retries=3, base_delay=1):
    """Повторяет выполнение функции при таймауте с экспоненциальной задержкой и случайностью"""
    last_error = None
    for attempt in range(max_retries):
        try:
            return await func()
        except (TimedOut, NetworkError) as e:
            last_error = e
            if attempt == max_retries - 1:
                raise last_error
            
            # Экспоненциальная задержка с случайным компонентом
            delay = base_delay * (2 ** attempt)
            jitter = random.uniform(0, min(delay * 0.1, 1.0))  # 10% случайности, но не больше 1 секунды
            total_delay = delay + jitter
            
            logger.warning(
                f"Таймаут при выполнении {func.__name__}, "
                f"попытка {attempt + 1}/{max_retries}. "
                f"Ошибка: {str(e)}. "
                f"Следующая попытка через {total_delay:.1f} сек"
            )
            await asyncio.sleep(total_delay)
    
    # Этот код не должен выполниться, но на всякий случай
    if last_error:
        raise last_error
    raise RuntimeError("Unexpected error in retry_on_timeout")

def handle_telegram_timeout(max_retries=5, base_delay=2):
    """Декоратор для обработки таймаутов в командах и обработчиках"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(update, context, *args, **kwargs):
            async def attempt():
                return await func(update, context, *args, **kwargs)
            try:
                return await retry_on_timeout(attempt, max_retries, base_delay)
            except (TimedOut, NetworkError) as e:
                user_id = update.effective_user.id if update and update.effective_user else "Unknown"
                logger.error(
                    f"Ошибка при обработке запроса от пользователя {user_id}: "
                    f"{type(e).__name__} - {str(e)}"
                )
                if update and hasattr(update, 'message') and update.message:
                    try:
                        await update.message.reply_text(
                            "Извините, произошла ошибка при обработке запроса. "
                            "Пожалуйста, попробуйте еще раз через несколько секунд."
                        )
                    except Exception as reply_error:
                        logger.error(f"Не удалось отправить сообщение об ошибке: {reply_error}")
                elif update and hasattr(update, 'callback_query') and update.callback_query:
                    try:
                        await update.callback_query.answer(
                            "Произошла ошибка. Попробуйте еще раз.",
                            show_alert=True
                        )
                    except Exception as answer_error:
                        logger.error(f"Не удалось отправить ответ на callback: {answer_error}")
                raise
            except Exception as e:
                user_id = update.effective_user.id if update and update.effective_user else "Unknown"
                logger.error(
                    f"Неожиданная ошибка при обработке запроса от пользователя {user_id}: "
                    f"{type(e).__name__} - {str(e)}\n{traceback.format_exc()}"
                )
                raise
        return wrapper
    return decorator

async def safe_send_message(message_obj, text, reply_markup=None, parse_mode=None):
    """Безопасная отправка сообщения с обработкой таймаутов"""
    async def send_attempt():
        return await message_obj.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    try:
        return await retry_on_timeout(send_attempt)
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {str(e)}")
        # В случае критической ошибки пытаемся отправить упрощенное сообщение
        try:
            return await message_obj.reply_text(
                "Произошла ошибка. Пожалуйста, попробуйте позже или используйте /menu для возврата в главное меню."
            )
        except:
            logger.error("Не удалось отправить даже сообщение об ошибке")
            return None

async def safe_edit_message(message_obj, text, reply_markup=None, parse_mode=None):
    """Безопасное редактирование сообщения с обработкой таймаутов"""
    async def edit_attempt():
        return await message_obj.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    try:
        return await retry_on_timeout(edit_attempt)
    except Exception as e:
        logger.error(f"Ошибка при редактировании сообщения: {str(e)}")
        return None

async def send_notification_to_users(application):
    """
    Отправляет системное уведомление всем пользователям, у которых notifications=1
    """
    try:
        # Читаем текст уведомления
        with open('notification.txt', 'r', encoding='utf-8') as f:
            notification_text = f.read()

        # Получаем список пользователей с включенными уведомлениями
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT telegram_id FROM students WHERE notifications=1 AND telegram_id IS NOT NULL AND telegram_id != "added by admin" AND telegram_id != "added_by_superadmin"')
            users = cursor.fetchall()

        # Отправляем уведомление каждому пользователю
        success_count = 0
        fail_count = 0
        for (user_telegram_id,) in users:
            try:
                # Пропускаем невалидные telegram_id
                if not user_telegram_id.isdigit():
                    continue

                await application.bot.send_message(
                    chat_id=int(user_telegram_id),  # Преобразуем в число
                    text=notification_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True  # Отключаем предпросмотр ссылок
                )
                success_count += 1
                await asyncio.sleep(0.1)  # Небольшая задержка между отправками
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления пользователю {user_telegram_id}: {e}")
                fail_count += 1
                continue  # Продолжаем со следующим пользователем

        return True, success_count, fail_count
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомлений: {e}")
        return False, 0, 0