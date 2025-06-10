import sqlite3
import shutil
import os
from datetime import datetime
import json

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
        # Подключаемся к оригинальной базе
        conn = sqlite3.connect('students.db')
        cursor = conn.cursor()

        print("\n🔄 Добавление колонки subgroup...")
        try:
            # Добавляем поле subgroup в таблицу students
            cursor.execute('''
                ALTER TABLE students 
                ADD COLUMN subgroup INTEGER DEFAULT 1
            ''')
            conn.commit()
            print("✅ Колонка subgroup добавлена успешно")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e).lower():
                print("ℹ️ Колонка subgroup уже существует в таблице students")
            else:
                raise
        
        print("\n🔄 Создание таблицы raspisanie...")
        # Создаем таблицу расписания
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raspisanie (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_full_name TEXT NOT NULL,
                       
                monday_1 TEXT,
                monday_2 TEXT,
                monday_3 TEXT,
                monday_4 TEXT,
                monday_5 TEXT,
                
                tuesday_1 TEXT,
                tuesday_2 TEXT,
                tuesday_3 TEXT,
                tuesday_4 TEXT,
                tuesday_5 TEXT,
                
                wednesday_1 TEXT,
                wednesday_2 TEXT,
                wednesday_3 TEXT,
                wednesday_4 TEXT,
                wednesday_5 TEXT,
                
                thursday_1 TEXT,
                thursday_2 TEXT,
                thursday_3 TEXT,
                thursday_4 TEXT,
                thursday_5 TEXT,
                
                friday_1 TEXT,
                friday_2 TEXT,
                friday_3 TEXT,
                friday_4 TEXT,
                friday_5 TEXT,
                
                saturday_1 TEXT,
                saturday_2 TEXT,
                saturday_3 TEXT,
                saturday_4 TEXT,
                saturday_5 TEXT,
                
                sunday_1 TEXT,
                sunday_2 TEXT,
                sunday_3 TEXT,
                sunday_4 TEXT,
                sunday_5 TEXT,
                
                UNIQUE(group_full_name)
            )
        ''')
        conn.commit()
        
        # Проверяем создание таблицы raspisanie
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='raspisanie'")
        if cursor.fetchone():
            print("✅ Таблица raspisanie создана успешно")
        else:
            raise Exception("Таблица raspisanie не была создана")

        print("\n🔄 Создание таблицы disciplines...")
        # Создаем таблицу дисциплин
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS disciplines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_name TEXT NOT NULL,
                disc_1 TEXT,
                disc_2 TEXT,
                disc_3 TEXT,
                disc_4 TEXT,
                disc_5 TEXT,
                disc_6 TEXT,
                disc_7 TEXT,
                disc_8 TEXT,
                disc_9 TEXT,
                disc_10 TEXT,
                disc_11 TEXT,
                disc_12 TEXT,
                disc_13 TEXT,
                disc_14 TEXT,
                disc_15 TEXT,
                disc_16 TEXT,
                disc_17 TEXT,
                disc_18 TEXT,
                disc_19 TEXT,
                disc_20 TEXT,
                disc_21 TEXT,
                disc_22 TEXT,
                disc_23 TEXT,
                disc_24 TEXT,
                disc_25 TEXT,
                disc_26 TEXT,
                disc_27 TEXT,
                disc_28 TEXT,
                disc_29 TEXT,
                disc_30 TEXT,
                UNIQUE(group_name)
            )
        ''')
        conn.commit()
        
        print("\n🔄 Создание таблицы bot_settings...")
        # Создаем таблицу настроек бота
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        conn.commit()
        
        # Инициализируем начальные настройки, если их нет
        cursor.execute('INSERT OR IGNORE INTO bot_settings (key, value, updated_at) VALUES (?, ?, ?)',
            ('week_type', json.dumps({
                'current_type': 'UP',
                'last_change': datetime.now().isoformat(),
                'auto_switch': True
            }), datetime.now().isoformat())
        )
        conn.commit()
        
        # Проверяем создание таблицы bot_settings
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bot_settings'")
        if cursor.fetchone():
            print("✅ Таблица bot_settings создана успешно")
        else:
            raise Exception("Таблица bot_settings не была создана")

        print("\n✅ Миграция успешно выполнена")
        
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении миграции: {str(e)}")
        print("❌ Миграция не была завершена")
        raise  # Пробрасываем ошибку дальше
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