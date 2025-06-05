import os
import zipfile
import datetime
import logging
import json
from utils import get_db_connection, logger

class CourseWorkArchiveManager:
    MAX_ARCHIVE_SIZE = 45 * 1024 * 1024  # 45MB (оставляем запас до лимита Telegram в 50MB)

    def __init__(self, archive_dir='course_work_archives'):
        self.archive_dir = archive_dir
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

    def _get_archive_info(self, discipline):
        """Получает информацию об архиве для дисциплины"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM course_work_archives WHERE discipline=?', (discipline,))
            return cursor.fetchone()

    def _update_archive_info(self, discipline, archive_paths, file_count, total_size):
        """Обновляет информацию об архиве в базе данных"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            now = datetime.datetime.now().isoformat()
            # Сохраняем все пути к частям архива в archive_parts как JSON
            cursor.execute('''
                INSERT OR REPLACE INTO course_work_archives 
                (discipline, archive_parts, last_updated, file_count, total_size)
                VALUES (?, ?, ?, ?, ?)
            ''', (discipline, json.dumps(archive_paths), now, file_count, total_size))
            conn.commit()

    def _get_course_works(self, discipline):
        """Получает список всех курсовых работ по дисциплине"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT file_path FROM course_works WHERE TRIM(LOWER(discipline))=TRIM(LOWER(?))', 
                (discipline,)
            )
            return [row[0] for row in cursor.fetchall() if row[0] and os.path.isfile(row[0])]

    def _create_archive_part(self, files, part_num, base_path, total_parts):
        """Создает часть архива"""
        archive_path = f"{base_path}.part{part_num+1}of{total_parts}.zip"
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files:
                arcname = os.path.basename(file_path)
                zipf.write(file_path, arcname)
        return archive_path

    def _estimate_total_size(self, files):
        """Оценивает общий размер файлов"""
        return sum(os.path.getsize(f) for f in files)

    def _split_files_into_parts(self, files):
        """Разделяет файлы на части, чтобы каждая часть не превышала максимальный размер"""
        parts = []
        current_part = []
        current_size = 0

        for file_path in files:
            file_size = os.path.getsize(file_path)
            if file_size > self.MAX_ARCHIVE_SIZE:
                logger.warning(f"Файл {file_path} превышает максимально допустимый размер и будет пропущен")
                continue

            if current_size + file_size > self.MAX_ARCHIVE_SIZE:
                if current_part:
                    parts.append(current_part)
                current_part = [file_path]
                current_size = file_size
            else:
                current_part.append(file_path)
                current_size += file_size

        if current_part:
            parts.append(current_part)

        return parts

    async def get_or_create_archive(self, discipline, force_update=False):
        """
        Получает или создает архив курсовых работ.
        
        Args:
            discipline: название дисциплины
            force_update: принудительно обновить архив
            
        Returns:
            tuple: (archive_paths, is_new_or_updated, info_message)
            archive_paths: список путей к частям архива или None в случае ошибки
            is_new_or_updated: был ли архив создан/обновлен
            info_message: информационное сообщение
        """
        logger.info(f"Запрос архива для дисциплины '{discipline}' (force_update={force_update})")
        
        # Проверяем существующий архив
        archive_info = self._get_archive_info(discipline)
        if archive_info and not force_update:
            logger.info(f"Найден существующий архив для '{discipline}', проверка актуальности...")
            archive_parts = json.loads(archive_info[1] or '[]')  # Получаем список всех частей архива
            last_updated = datetime.datetime.fromisoformat(archive_info[2])  # Индекс изменился после удаления archive_path
            
            # Проверяем, были ли изменения в курсовых работах после последнего обновления архива
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT MAX(parsing_time) 
                    FROM course_works 
                    WHERE TRIM(LOWER(discipline))=TRIM(LOWER(?))
                ''', (discipline,))
                latest_work_time = cursor.fetchone()[0]
                
                if latest_work_time:
                    latest_work_time = datetime.datetime.fromisoformat(latest_work_time)
                    if latest_work_time <= last_updated:
                        logger.info(f"Архив для '{discipline}' актуален (последнее обновление: {last_updated})")
                        # Проверяем существование всех частей архива
                        if all(os.path.exists(path) for path in archive_parts):
                            return archive_parts, False, "Архив актуален"
                        else:
                            logger.warning(f"Некоторые части архива не найдены на диске")
                    else:
                        logger.info(f"Требуется обновление архива для '{discipline}' (новые работы от: {latest_work_time})")
                else:
                    logger.warning(f"Не найдены курсовые работы для дисциплины '{discipline}'")

        # Получаем список текущих файлов
        current_files = self._get_course_works(discipline)
        if not current_files:
            logger.warning(f"Нет файлов для архивации по дисциплине '{discipline}'")
            return None, False, "Нет файлов для архивации."

        # Оцениваем общий размер
        total_size = self._estimate_total_size(current_files)
        logger.info(f"Общий размер файлов для '{discipline}': {total_size/1024/1024:.2f} MB")
        base_path = os.path.join(self.archive_dir, f'{discipline.replace(" ", "_")}')

        # Если общий размер превышает лимит, разделяем на части
        if total_size > self.MAX_ARCHIVE_SIZE:
            logger.info(f"Размер превышает лимит ({self.MAX_ARCHIVE_SIZE/1024/1024:.2f} MB), разделение на части")
            file_parts = self._split_files_into_parts(current_files)
            if not file_parts:
                logger.error(f"Все файлы для '{discipline}' превышают максимально допустимый размер")
                return None, False, "Все файлы превышают максимально допустимый размер."

            archive_paths = []
            total_parts = len(file_parts)
            logger.info(f"Создание {total_parts} частей архива для '{discipline}'")
            
            for i, part_files in enumerate(file_parts):
                try:
                    logger.info(f"Создание части {i+1}/{total_parts} архива для '{discipline}'")
                    archive_path = self._create_archive_part(part_files, i, base_path, total_parts)
                    archive_paths.append(archive_path)
                    logger.info(f"Успешно создана часть {i+1}: {archive_path}")
                except Exception as e:
                    logger.error(f"Ошибка при создании части {i+1} архива для '{discipline}': {e}")
                    # Удаляем созданные части в случае ошибки
                    for path in archive_paths:
                        try:
                            if os.path.exists(path):
                                os.remove(path)
                                logger.info(f"Удален частичный архив после ошибки: {path}")
                        except Exception as del_e:
                            logger.error(f"Ошибка при удалении части архива {path}: {del_e}")
                    return None, False, f"Ошибка при создании части {i+1} архива."

            # Обновляем информацию в БД
            self._update_archive_info(
                discipline=discipline,
                archive_paths=archive_paths,
                file_count=len(current_files),
                total_size=total_size
            )
            logger.info(f"Обновлена информация в БД для архива '{discipline}'")

            msg = f"Создано {len(archive_paths)} частей архива. "
            msg += f"Всего файлов: {len(current_files)}"
            return archive_paths, True, msg

        else:
            # Если размер не превышает лимит, создаем один архив
            single_archive_path = f"{base_path}.zip"
            logger.info(f"Создание единого архива для '{discipline}': {single_archive_path}")
            try:
                with zipfile.ZipFile(single_archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file_path in current_files:
                        arcname = os.path.basename(file_path)
                        zipf.write(file_path, arcname)
                        logger.debug(f"Добавлен файл в архив: {arcname}")

                # Обновляем информацию в БД
                self._update_archive_info(
                    discipline=discipline,
                    archive_paths=[single_archive_path],
                    file_count=len(current_files),
                    total_size=total_size
                )
                logger.info(f"Архив успешно создан и информация обновлена в БД: {single_archive_path}")

                msg = f"Архив создан. Всего файлов: {len(current_files)}"
                return [single_archive_path], True, msg

            except Exception as e:
                logger.error(f"Ошибка при создании архива {single_archive_path}: {e}")
                if os.path.exists(single_archive_path):
                    try:
                        os.remove(single_archive_path)
                        logger.info(f"Удален поврежденный архив: {single_archive_path}")
                    except Exception as del_e:
                        logger.error(f"Не удалось удалить поврежденный архив: {del_e}")
                return None, False, "Произошла ошибка при создании архива." 