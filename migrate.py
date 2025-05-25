import sqlite3
import os

DB_PATH = 'students.db'

def migrate_file_paths():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT rowid, file_path FROM course_works')
    rows = cursor.fetchall()
    updated = 0
    for rowid, file_path in rows:
        if not file_path:
            continue
        # Привести путь к нормализованному виду (для текущей ОС)
        norm_path = os.path.normpath(file_path)
        if norm_path != file_path:
            cursor.execute('UPDATE course_works SET file_path=? WHERE rowid=?', (norm_path, rowid))
            updated += 1
    conn.commit()
    conn.close()
    print(f"Миграция завершена. Обновлено записей: {updated}")

if __name__ == '__main__':
    migrate_file_paths()
