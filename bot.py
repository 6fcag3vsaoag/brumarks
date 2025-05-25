from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters
from utils import (
    logger, config, get_db_connection, require_registration, REPLY_KEYBOARD_MARKUP,
    INLINE_KEYBOARD_MARKUP, format_ratings_table, show_student_rating
)
from handlers import handle_message, handle_inline_buttons

async def start_command(update, context):
    logger.info("Получена команда /start")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Вы вернулись в главное меню! Выберите опцию:",
        reply_markup=INLINE_KEYBOARD_MARKUP
    )

async def cancel_command(update, context):
    logger.info("Получена команда /cancel")
    context.user_data.clear()  # Сбрасываем временные данные
    await update.message.reply_text(
        "Действие отменено. Вы вернулись в главное меню! Выберите опцию:",
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

# Register handlers
application.add_handler(CommandHandler("start", start_command))
application.add_handler(CommandHandler("cancel", cancel_command))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
application.add_handler(CallbackQueryHandler(handle_inline_buttons))

# Run bot
if __name__ == "__main__":
    logger.info("Бот запущен.")
    application.run_polling()