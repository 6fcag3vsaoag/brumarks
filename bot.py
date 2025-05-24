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
from functools import wraps

# Выводим все сообщения INFO и выше в консоль,
# а сообщения WARNING и ERROR также записываем в файл bot.log
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

file_handler = logging.FileHandler('bot.log', encoding='utf-8')
file_handler.setLevel(logging.WARNING)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # Очищаем обработчики, если они уже есть
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Загружаем токен и другие параметры из config.json
with open('config.json') as config_file:
    config = json.load(config_file)

# Основное меню (инлайн-клавиатура)
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

# Постоянная reply-клавиатура с кнопкой "В начало"
REPLY_KEYBOARD = [['В начало']]
REPLY_KEYBOARD_MARKUP = ReplyKeyboardMarkup(REPLY_KEYBOARD, resize_keyboard=True, one_time_keyboard=False)

# Клавиатура для отмены регистрации
CANCEL_KEYBOARD = [[InlineKeyboardButton("Отмена", callback_data='cancel_registration')]]
CANCEL_KEYBOARD_MARKUP = InlineKeyboardMarkup(CANCEL_KEYBOARD)

# =====================
# Работа с базой данных
# =====================
def get_db_connection():
    """
    Получение соединения с базой данных SQLite.
    Необходимо закрывать соединение после использования (используйте with).
    """
    return sqlite3.connect('students.db')

# Декоратор для проверки регистрации пользователя
# Используйте для команд, где требуется регистрация
# Если пользователь не зарегистрирован — выводится сообщение и команда не выполняется
# Пример: @require_registration
#         async def menu_command(...)
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

# Получение списка всех дисциплин из структуры таблицы students
# Исключаются служебные поля, возвращается список уникальных дисциплин
# Используется для динамического обновления структуры БД при появлении новых дисциплин
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

# Добавляет новые столбцы для дисциплин и модулей в таблицу students, если они отсутствуют
# Это позволяет поддерживать актуальную структуру БД при появлении новых предметов
def update_db_structure(new_subjects):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(students)")
        current_columns = [col[1] for col in cursor.fetchall()]
        for subject in new_subjects:
            for module in [' (модуль 1)', ' (модуль 2)']:
                col_name = f'{subject}{module}'
                if col_name not in current_columns:
                    # Экранируем имя столбца для безопасности
                    safe_col_name = '"' + col_name.replace('"', '""') + '"'
                    cursor.execute(f'ALTER TABLE students ADD COLUMN {safe_col_name} TEXT DEFAULT "не изучает"')
        conn.commit()

# Извлекает список дисциплин из HTML-таблицы на странице студента
def get_subjects(soup):
    table = soup.find('table', id='user')
    if not table:
        return []
    rows = table.find_all('tr')
    if not rows or len(rows) < 1:
        return []
    headers = [header.text.strip() for header in rows[0].find_all('th')]
    return headers[1:-1] if len(headers) > 2 else []

# Парсит страницу рейтинга студента по его ID
# Возвращает фамилию, словарь оценок по дисциплинам и список дисциплин
# В случае ошибки возвращает 'Unknown', пустой словарь и пустой список
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
        # Получаем фамилию (или Unknown)
        last_name = full_name.split()[0] if full_name != "Unknown" and len(full_name.split()) > 0 else "Unknown"
        subjects = get_subjects(soup)
        if not subjects:
            return last_name, {}, subjects
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
        return last_name, grades, subjects
    except Exception as e:
        logger.error(f"Ошибка при парсинге данных студента для ID {student_id}: {e}")
        return "Unknown", {}, []

# Сохраняет или обновляет данные студента в базе данных
# Если появляются новые дисциплины, обновляет структуру таблицы
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
        # Экранируем имена столбцов для безопасности
        columns = [f'"{k.replace("\"", "\"\"")}"' for k in data.keys()]
        query = f"""
        INSERT INTO students ({','.join(columns)}) 
        VALUES ({','.join(['?']*len(data))})
        ON CONFLICT(student_id) DO UPDATE SET {','.join([f'"{k.replace("\"", "\"\"")}"=?' for k in data.keys()])}
        """
        cursor.execute(query, list(data.values())*2)
        conn.commit()

# Асинхронная версия сохранения в БД (для использования в async-коде)
import asyncio
async def save_to_db_async(*args, **kwargs):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, save_to_db, *args, **kwargs)

# Проверяет, зарегистрирован ли пользователь в базе данных
# Возвращает: (is_registered, student_data), где student_data = (student_id, student_group, is_admin) или None
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

# Обработчик команды /start
# Показывает главное меню с инлайн-клавиатурой
async def start_command(update, context):
    logger.info("Получена команда /start")
    await update.message.reply_text(
        "Добро пожаловать! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

# Пример использования декоратора require_registration для команд
@require_registration
async def menu_command(update, context):
    # Обработчик команды /menu
    # Показывает главное меню с инлайн-клавиатурой
    logger.info("Получена команда /menu")
    await update.message.reply_text(
        "Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

# Обработчик кнопки "В начало" (reply-клавиатура)
# Очищает пользовательские данные и возвращает главное меню
async def handle_start_button(update, context):
    logger.info("Нажата кнопка 'В начало'")
    context.user_data.clear()
    await update.message.reply_text(
        "Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

# Показывает рейтинг студента по его student_id
# Формирует и отправляет таблицу с оценками
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

def format_discipline_name(name, max_length=25):
    # Форматирует название дисциплины для таблицы (обрезает и выравнивает по ширине)
    name = name.split(' (модуль')[0].strip()
    if len(name) > max_length:
        return name[:max_length-3] + '...'
    return name.ljust(max_length)

def format_ratings_table(name, data, is_group=False):
    # Формирует текстовую таблицу с рейтингом для пользователя или группы
    # Если is_group=False — таблица по дисциплинам пользователя
    # Если is_group=True — таблица по студентам группы по одной дисциплине
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

async def handle_message(update, context):
    # Главный обработчик текстовых сообщений
    # В зависимости от состояния пользователя выполняет регистрацию, добавление студентов/админов или возвращает меню
    text = update.message.text.strip()
    logger.info(f"Received message: {text}")

    if text == 'В начало':
        await handle_start_button(update, context)
        return

    telegram_id = str(update.effective_user.id)
    
    if context.user_data.get('awaiting_student_id'):
        student_id = text
        name, grades, subjects = parse_student_data(student_id)
        if name == "Unknown":
            await update.message.reply_text(
                "Не удалось получить данные по номеру студенческого билета. Проверьте правильность номера или сервет VUZ2 не отвечает. попробуйте позже.",
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
                        "Не удалось получить данные по номеру студенческого билета. Проверьте правильность введенного номера. Возможно сервер VUZ2 не отвечает. попробуйте позже.",
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


async def handle_inline_buttons(update, context):
    # Главный обработчик инлайн-кнопок Telegram-бота
    # В зависимости от callback_data выполняет различные действия:
    # - my_rating: показывает рейтинг пользователя
    # - group: список студентов группы
    # - disciplines: список дисциплин пользователя
    # - discipline_...: рейтинг по дисциплине в группе
    # - student_...: рейтинг конкретного студента
    # - settings: меню настроек
    # - add_admin: добавление администратора
    # - add_student: добавление студента
    # - cancel_registration: отмена регистрации
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

    # Проверяем регистрацию пользователя
    is_registered, student_data = await check_registration(telegram_id)
    student_id, student_group, is_admin = student_data if student_data else (None, None, False)

    # Если пользователь не зарегистрирован, предлагаем пройти регистрацию
    if not is_registered:
        await update.callback_query.message.reply_text(
            "Вы не зарегистрированы. Введите номер студенческого билета:",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data.clear()
        context.user_data['awaiting_student_id'] = True
        return

    # Обработка кнопки "Мой Рейтинг"
    if callback_data == 'my_rating':
        if not is_registered:
            await query.message.reply_text(
                "Вы не зарегистрированы, воспользуйтесь кнопкой Мой Профиль для регистрации в боте.",
                reply_markup=REPLY_KEYBOARD_MARKUP
            )
            return
        # Получаем и показываем рейтинг пользователя
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

    # Обработка кнопки "Группа" — список студентов группы
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
            # Формируем клавиатуру с именами студентов
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

    # Обработка кнопки "Дисциплины" — список дисциплин пользователя
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
            # Собираем список дисциплин, которые изучает студент
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
            # Генерируем callback_data для каждой дисциплины (ограничение 64 байта)
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
            # Сохраняем соответствие callback_data -> название дисциплины в user_data
            context.user_data.pop('discipline_map', None)
            context.user_data['discipline_map'] = {
                f"discipline_{safe_disc}": disc for disc, safe_disc in sanitized_disciplines
            }
            # Формируем клавиатуру с дисциплинами
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

    # Обработка кнопки дисциплины — рейтинг по дисциплине в группе
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
                # Получаем группу пользователя
                cursor.execute('SELECT student_group FROM students WHERE telegram_id=?', (telegram_id,))
                result = cursor.fetchone()
                if not result:
                    await query.message.reply_text(
                        "Группа не найдена. Пожалуйста, зарегистрируйтесь заново.",
                        reply_markup=REPLY_KEYBOARD_MARKUP
                    )
                    return
                student_group = result[0]
                # Формируем SQL-запрос для получения оценок по дисциплине
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
                # Формируем данные для таблицы успеваемости
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

    # Обработка кнопки студента — рейтинг конкретного студента
    elif callback_data.startswith('student_'):
        student_id = callback_data.split('_')[1]
        await show_student_rating(query, student_id)

    # Обработка кнопки "Мой Профиль" (settings)
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
            "Введите номер студенческого билета нового администратора:",
            reply_markup=CANCEL_KEYBOARD_MARKUP
        )
        context.user_data['awaiting_admin_student_id'] = True

    # Обработка кнопки "Добавить студента"
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

    # Обработка кнопки "Отмена регистрации"
    elif callback_data == 'cancel_registration':
        context.user_data.clear()
        await query.message.reply_text(
            "Действие отменено. Выберите опцию:",
            reply_markup=INLINE_KEYBOARD_MARKUP
        )

def init_db():
    # Инициализация базы данных: создание таблицы students, если она не существует
    with get_db_connection() as conn:
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
                last_parsed_time TEXT,
                is_superadmin INTEGER DEFAULT 0
            )
        ''')
        # Создание таблицы course_works, если она не существует, с одинаковыми названиями полей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS course_works (
                discipline TEXT,
                student_id TEXT,
                telegram_id TEXT,
                name TEXT,
                student_group TEXT,
                file_path TEXT,
                parsing_time TEXT
            )
        ''')
        conn.commit()

# Инициализация базы данных при запуске скрипта
init_db()

# Создание и запуск бота
application = Application.builder().token(config['telegram_token']).build()

# Регистрация обработчиков команд и сообщений
application.add_handler(CommandHandler("start", start_command))
application.add_handler(CommandHandler("menu", menu_command))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

# Регистрация обработчика инлайн-кнопок
application.add_handler(CallbackQueryHandler(handle_inline_buttons))

# Запуск бота
if __name__ == "__main__":
    logger.info("Бот запущен.")
    application.run_polling()
