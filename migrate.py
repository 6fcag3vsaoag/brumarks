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
    """Выполняет миграцию базы данных"""
    try:
        conn = sqlite3.connect('students.db')
        cursor = conn.cursor()

        # Добавляем новые колонки в таблицу students
        cursor.execute('''
            ALTER TABLE students 
            ADD COLUMN blackmarket_allowed INTEGER DEFAULT 1
        ''')
        cursor.execute('''
            ALTER TABLE students 
            ADD COLUMN blackmarket_announcements INTEGER DEFAULT 1
        ''')

        # Создаем новую таблицу blackmarket
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blackmarket (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id TEXT NOT NULL,
                is_anon INTEGER DEFAULT 0,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                contacts TEXT NOT NULL,
                publication_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (student_id) REFERENCES students(student_id)
            )
        ''')

        conn.commit()
        print("✅ Миграция успешно выполнена")
        
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("ℹ️ Колонки уже существуют, пропускаем...")
        else:
            print(f"❌ Ошибка SQL при миграции: {e}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка при миграции: {e}")
    finally:
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