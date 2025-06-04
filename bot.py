from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from telegram import Update
from telegram.error import NetworkError, TimedOut
from utils import (
    logger, config, get_db_connection, require_registration, REPLY_KEYBOARD_MARKUP,
    INLINE_KEYBOARD_MARKUP, format_ratings_table, show_student_rating
)
from handlers import handle_message, handle_inline_buttons
from scheduler import StudentParserScheduler
import asyncio
import signal
import traceback
import sys

async def start_command(update, context):
    user_id = update.effective_user.id
    logger.info(f"Получена команда /start от пользователя {user_id}")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Вы вернулись в главное меню! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

async def cancel_command(update, context):
    user_id = update.effective_user.id
    logger.info(f"Получена команда /cancel от пользователя {user_id}")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Действие отменено. Вы вернулись в главное меню! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

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
                is_superadmin INTEGER DEFAULT 0
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
        conn.commit()

# Initialize database
init_db()

# Bot setup
application = Application.builder().token(config['telegram_token']).build()

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

async def run_polling():
    """Запуск поллинга с обработкой ошибок"""
    retry_count = 0
    max_retries = 5
    base_delay = 5  # начальная задержка в секундах

    while True:
        try:
            logger.info("Запуск поллинга бота...")
            await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            logger.info("Поллинг успешно запущен")
            # Если поллинг успешно запущен, сбрасываем счетчик попыток
            retry_count = 0
            # Ждем бесконечно, пока не произойдет ошибка
            await asyncio.Event().wait()
        except (NetworkError, TimedOut) as e:
            retry_count += 1
            delay = min(base_delay * (2 ** retry_count), 300)  # максимальная задержка 5 минут
            
            error_details = {
                'error_type': type(e).__name__,
                'error_msg': str(e),
                'retry_count': retry_count,
                'max_retries': max_retries,
                'next_delay': delay
            }
            
            logger.warning(
                "Сетевая ошибка при поллинге: %(error_type)s - %(error_msg)s "
                "(попытка %(retry_count)s/%(max_retries)s, "
                "следующая попытка через %(next_delay)s сек)",
                error_details
            )
            
            if retry_count >= max_retries:
                logger.error("Превышено максимальное количество попыток переподключения")
                raise
                
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(
                f"Критическая ошибка при поллинге: {type(e).__name__} - {str(e)}\n"
                f"Traceback: {traceback.format_exc()}"
            )
            # Добавляем небольшую задержку перед следующей попыткой
            await asyncio.sleep(5)

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
        await run_polling()
        
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