from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram import Update
from telegram.error import NetworkError, TimedOut
from utils import (
    logger, config, get_db_connection, require_registration, REPLY_KEYBOARD_MARKUP,
    INLINE_KEYBOARD_MARKUP, format_ratings_table, show_student_rating, handle_telegram_timeout
)
from handlers import handle_message, handle_inline_buttons
from scheduler import StudentParserScheduler
import asyncio
import signal
import traceback
import sys
import datetime
import random

# Константы для настройки повторных попыток и таймаутов
RETRY_SETTINGS = {
    'max_retries': 10,           # Увеличено с 5 до 10
    'base_delay': 2,            # Уменьшено с 5 до 2 для более быстрых первых попыток
    'max_delay': 600,           # Увеличено с 300 до 600 секунд
    'connection_timeout': 60.0,  # Увеличено с 30 до 60 секунд
    'read_timeout': 60.0,       # Увеличено с 30 до 60 секунд
    'write_timeout': 60.0,      # Увеличено с 30 до 60 секунд
    'connect_attempts': 5,      # Увеличено с 3 до 5
    'polling_timeout': 30,      # Добавлен таймаут для поллинга
    'webhook_max_connections': 40  # Добавлено максимальное количество одновременных подключений
}

# Создаем приложение с настроенными таймаутами
def create_application():
    return (Application.builder()
            .token(config['telegram_token'])
            .connect_timeout(RETRY_SETTINGS['connection_timeout'])
            .read_timeout(RETRY_SETTINGS['read_timeout'])
            .write_timeout(RETRY_SETTINGS['write_timeout'])
            .get_updates_connect_timeout(RETRY_SETTINGS['connection_timeout'])
            .get_updates_read_timeout(RETRY_SETTINGS['read_timeout'])
            .get_updates_write_timeout(RETRY_SETTINGS['write_timeout'])
            .pool_timeout(RETRY_SETTINGS['connection_timeout'])  # Добавлен таймаут пула
            .connection_pool_size(RETRY_SETTINGS['webhook_max_connections'])  # Добавлен размер пула
            .build())

@handle_telegram_timeout()
async def start_command(update, context):
    user_id = update.effective_user.id
    logger.info(f"Получена команда /start от пользователя {user_id}")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Вы вернулись в главное меню! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

@handle_telegram_timeout()
async def cancel_command(update, context):
    user_id = update.effective_user.id
    logger.info(f"Получена команда /cancel от пользователя {user_id}")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Действие отменено. Вы вернулись в главное меню! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

@handle_telegram_timeout()
async def menu_command(update, context):
    user_id = update.effective_user.id
    logger.info(f"Получена команда /menu от пользователя {user_id}")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Вы вернулись в главное меню! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

async def handle_start_button(update, context):
    await start_command(update, context)  # Повторно используем логику /start

def init_db():
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
                is_superadmin INTEGER DEFAULT 0,
                notifications INTEGER DEFAULT 1
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS course_works (
                discipline TEXT,
                student_id TEXT,
                telegram_id TEXT,
                name TEXT,
                student_group TEXT,
                semester INTEGER,
                file_path TEXT,
                parsing_time TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS course_work_archives (
                discipline TEXT PRIMARY KEY,
                archive_parts TEXT DEFAULT '[]',
                last_updated TEXT NOT NULL,
                file_count INTEGER DEFAULT 0,
                total_size INTEGER DEFAULT 0
            )
        ''')
        conn.commit()

# Initialize database
init_db()

# Bot setup with timeouts
application = create_application()

# Initialize scheduler
scheduler = StudentParserScheduler(application)

# Register handlers
application.add_handler(CommandHandler("start", start_command))
application.add_handler(CommandHandler("cancel", cancel_command))
application.add_handler(CommandHandler("menu", menu_command))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
application.add_handler(CallbackQueryHandler(handle_inline_buttons))

def handle_exception(loop, context):
    """Обработчик исключений в event loop"""
    try:
        msg = context.get("exception", context["message"])
        user_id = None
        
        # Безопасное получение user_id
        if hasattr(context.get('future'), '_coro'):
            coro = context.get('future')._coro
            if hasattr(coro, 'cr_frame'):
                locals_dict = coro.cr_frame.f_locals
                if 'update' in locals_dict and hasattr(locals_dict['update'], 'effective_user'):
                    user_id = locals_dict['update'].effective_user.id

        if user_id:
            logger.error(f"Ошибка в event loop для пользователя {user_id}: {msg}")
        else:
            logger.error(f"Ошибка в event loop: {msg}")

        if isinstance(msg, Exception):
            logger.error(f"Тип ошибки: {type(msg).__name__}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    except Exception as e:
        # Если что-то пошло не так в самом обработчике исключений
        logger.error(f"Ошибка в обработчике исключений: {str(e)}")
        logger.error(f"Оригинальное сообщение: {context.get('message', 'Неизвестно')}")
        logger.error(f"Traceback: {traceback.format_exc()}")

async def run_polling(application):
    """Запуск поллинга с обработкой ошибок"""
    retry_count = 0
    connect_attempt = 0
    last_error_time = None
    consecutive_errors = 0
    last_success_time = asyncio.get_event_loop().time()

    while True:
        try:
            logger.info("Запуск поллинга бота...")
            await application.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,  # Игнорируем сообщения, накопившиеся за время простоя
                timeout=RETRY_SETTINGS['polling_timeout']
            )
            logger.info("Поллинг успешно запущен")
            
            # Если поллинг успешно запущен, сбрасываем счетчики
            retry_count = 0
            connect_attempt = 0
            consecutive_errors = 0
            last_error_time = None
            last_success_time = asyncio.get_event_loop().time()
            
            # Ждем бесконечно, пока не произойдет ошибка
            await asyncio.Event().wait()

        except (NetworkError, TimedOut) as e:
            current_time = asyncio.get_event_loop().time()
            consecutive_errors += 1
            
            # Проверяем, нужно ли полностью перезапустить бота
            if consecutive_errors >= 10 or (current_time - last_success_time) > 1800:  # 30 минут без успешного подключения
                logger.error(
                    "Критическое количество ошибок или длительное отсутствие связи. "
                    f"Последнее успешное подключение: {datetime.datetime.fromtimestamp(last_success_time)}"
                )
                # Полная остановка и перезапуск бота
                try:
                    await application.stop()
                    await application.shutdown()
                    logger.info("Бот успешно остановлен, подготовка к перезапуску...")
                    await asyncio.sleep(10)  # Ждем 10 секунд перед перезапуском
                    application = create_application()
                    consecutive_errors = 0
                    connect_attempt = 0
                    retry_count = 0
                    continue
                except Exception as restart_error:
                    logger.error(f"Ошибка при перезапуске бота: {restart_error}")
                    await asyncio.sleep(30)  # Ждем 30 секунд перед следующей попыткой
                    continue
            
            # Сбрасываем счетчик retry_count, если прошло достаточно времени с последней ошибки
            if last_error_time and current_time - last_error_time > RETRY_SETTINGS['max_delay']:
                retry_count = 0
                connect_attempt = 0
            
            last_error_time = current_time
            retry_count += 1
            connect_attempt += 1
            
            # Вычисляем задержку с экспоненциальным ростом и случайным компонентом
            base_delay = RETRY_SETTINGS['base_delay'] * (2 ** retry_count)
            jitter = random.uniform(0, min(base_delay * 0.1, 1.0))  # 10% случайности, но не больше 1 секунды
            delay = min(base_delay + jitter, RETRY_SETTINGS['max_delay'])
            
            error_details = {
                'error_type': type(e).__name__,
                'error_msg': str(e),
                'retry_count': retry_count,
                'connect_attempt': connect_attempt,
                'consecutive_errors': consecutive_errors,
                'next_delay': delay,
                'time_since_last_success': int(current_time - last_success_time)
            }
            
            if connect_attempt >= RETRY_SETTINGS['connect_attempts']:
                logger.error(
                    "Превышено максимальное количество попыток подключения "
                    "(%(connect_attempt)s/%(max_attempts)s). Перезапуск бота...",
                    {'connect_attempt': connect_attempt, 'max_attempts': RETRY_SETTINGS['connect_attempts']}
                )
                # Перезапускаем приложение
                await application.stop()
                await application.shutdown()
                application = create_application()
                connect_attempt = 0
                continue
            
            logger.warning(
                "Сетевая ошибка при поллинге: %(error_type)s - %(error_msg)s "
                "(попытка %(retry_count)s, подключение %(connect_attempt)s/%(max_attempts)s, "
                "последовательных ошибок: %(consecutive_errors)s, "
                "время с последнего успеха: %(time_since_last_success)s сек, "
                "следующая попытка через %(next_delay)s сек)",
                {**error_details, 'max_attempts': RETRY_SETTINGS['connect_attempts']}
            )
            
            await asyncio.sleep(delay)

        except Exception as e:
            logger.error(
                f"Критическая ошибка при поллинге: {type(e).__name__} - {str(e)}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            consecutive_errors += 1
            # Добавляем небольшую задержку перед следующей попыткой
            await asyncio.sleep(RETRY_SETTINGS['base_delay'] * (2 ** min(consecutive_errors, 5)))

async def main():
    """Основная функция запуска бота"""
    try:
        logger.info("Инициализация бота...")
        
        # Получаем текущий event loop
        loop = asyncio.get_running_loop()
        logger.info("Event loop получен успешно")
        
        # Настраиваем обработчик исключений
        loop.set_exception_handler(handle_exception)
        logger.info("Обработчик исключений установлен")
        
        # Запускаем планировщик парсинга в фоновом режиме
        logger.info("Запуск планировщика парсинга...")
        scheduler_task = asyncio.create_task(scheduler.start())
        logger.info("Планировщик парсинга запущен успешно")
        
        # Запускаем бота
        logger.info("Инициализация приложения бота...")
        await application.initialize()
        logger.info("Приложение бота успешно инициализировано")
        logger.info("Запуск приложения бота...")
        await application.start()
        logger.info("Приложение бота успешно запущено")
        logger.info("Запуск поллинга...")
        
        # Запускаем поллинг с обработкой ошибок
        await run_polling(application)
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main(): {type(e).__name__} - {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Безопасное завершение работы
        try:
            if scheduler.is_running:
                logger.info("Остановка планировщика...")
                await scheduler.stop()
                logger.info("Планировщик успешно остановлен")
            
            logger.info("Остановка приложения бота...")
            await application.stop()
            await application.shutdown()
            logger.info("Приложение бота успешно остановлено")
            
        except Exception as e:
            logger.error(f"Ошибка при завершении работы: {type(e).__name__} - {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

# Run bot
if __name__ == "__main__":
    try:
        logger.info("Запуск бота...")
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка при работе бота: {type(e).__name__} - {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        # Останавливаем планировщик при выходе
        if scheduler.is_running:
            try:
                logger.info("Остановка планировщика...")
                # Создаем новый event loop для остановки планировщика
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                new_loop.run_until_complete(scheduler.stop())
                new_loop.close()
                logger.info("Планировщик успешно остановлен")
            except Exception as e:
                logger.error(f"Ошибка при остановке планировщика: {type(e).__name__} - {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")