import sqlite3

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
        # Заменяем все обратные слэши на прямые
        fixed_path = file_path.replace('\\', '/')
        if fixed_path != file_path:
            cursor.execute('UPDATE course_works SET file_path=? WHERE rowid=?', (fixed_path, rowid))
            updated += 1
    conn.commit()
    conn.close()
    print(f"Миграция завершена. Обновлено записей: {updated}")

if __name__ == '__main__':
    migrate_file_paths()