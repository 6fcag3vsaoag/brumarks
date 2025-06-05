import sqlite3
import shutil
import os
from datetime import datetime

def backup_database():
    """Создает резервную копию базы данных"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f'students_backup_{timestamp}.db'
    
    try:
        # Проверяем существование оригинальной базы
        if not os.path.exists('students.db'):
            print("❌ Ошибка: База данных не найдена")
            return False
            
        # Создаем резервную копию
        shutil.copy2('students.db', backup_path)
        print(f"✅ Резервная копия создана: {backup_path}")
        return True
    except Exception as e:
        print(f"❌ Ошибка при создании резервной копии: {e}")
        return False

def migrate_database():
    """Добавляет поле notifications в таблицу students"""
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect('students.db')
        cursor = conn.cursor()
        
        # Проверяем, существует ли уже колонка notifications
        cursor.execute("PRAGMA table_info(students)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'notifications' not in columns:
            # Добавляем колонку notifications
            cursor.execute('''
                ALTER TABLE students 
                ADD COLUMN notifications INTEGER DEFAULT 1
            ''')
            
            # Устанавливаем значение 1 для всех существующих записей
            cursor.execute('''
                UPDATE students 
                SET notifications = 1 
                WHERE notifications IS NULL
            ''')
            
            conn.commit()
            print("✅ Миграция успешно завершена")
            print("• Добавлена колонка notifications")
            print("• Установлено значение по умолчанию (1) для всех пользователей")
        else:
            print("ℹ️ Колонка notifications уже существует")
        
        # Выводим количество обновленных записей
        cursor.execute("SELECT COUNT(*) FROM students")
        total_records = cursor.fetchone()[0]
        print(f"📊 Всего записей в базе: {total_records}")
        
        cursor.execute("SELECT COUNT(*) FROM students WHERE notifications = 1")
        enabled_notifications = cursor.fetchone()[0]
        print(f"🔔 Пользователей с включенными уведомлениями: {enabled_notifications}")
        
    except Exception as e:
        print(f"❌ Ошибка при миграции: {e}")
    finally:
        if conn:
            conn.close()

def main():
    print("🔄 Начало процесса миграции базы данных")
    print("------------------------------------------")
    
    # Создаем резервную копию
    if backup_database():
        # Если бэкап успешен, выполняем миграцию
        print("\n🔄 Выполнение миграции...")
        print("------------------------------------------")
        migrate_database()
    else:
        print("\n❌ Миграция отменена из-за ошибки резервного копирования")
    
    print("\n------------------------------------------")
    print("✨ Процесс завершен")

if __name__ == "__main__":
    main() 