import asyncio
import datetime
from utils import get_db_connection, parse_student_data, save_to_db, logger
from telegram.ext import Application

class StudentParserScheduler:
    def __init__(self, application: Application):
        self.application = application
        self.parsing_queue = asyncio.Queue()
        self.is_running = False
        self.parser_task = None

    async def start(self):
        """Запускает планировщик парсинга"""
        if not self.is_running:
            self.is_running = True
            self.parser_task = asyncio.create_task(self._parser_worker())
            asyncio.create_task(self._schedule_parser())

    async def stop(self):
        """Останавливает планировщик парсинга"""
        if self.is_running:
            self.is_running = False
            if self.parser_task:
                self.parser_task.cancel()
                try:
                    await self.parser_task
                except asyncio.CancelledError:
                    pass

    async def _schedule_parser(self):
        """Планирует парсинг студентов каждые 10 минут"""
        while self.is_running:
            try:
                # Получаем студентов, которых нужно распарсить
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    current_time = datetime.datetime.now()
                    # Находим студентов, которых нужно распарсить
                    cursor.execute('''
                        SELECT student_id, telegram_id, student_group 
                        FROM students 
                        WHERE last_parsed_time IS NULL 
                        OR datetime(last_parsed_time) <= datetime(?)
                    ''', (current_time - datetime.timedelta(minutes=5),))
                    students = cursor.fetchall()

                # Добавляем студентов в очередь
                for student in students:
                    await self.parsing_queue.put(student)

                # Ждем 10 минут перед следующей проверкой
                await asyncio.sleep(10 * 60)  # 10 минут в секундах

            except Exception as e:
                logger.error(f"Ошибка в планировщике парсинга: {e}")
                await asyncio.sleep(60)  # Ждем минуту перед повторной попыткой

    def _is_system_telegram_id(self, telegram_id):
        """Проверяет, является ли telegram_id системным (добавлен админом или суперадмином)"""
        return telegram_id in ["added by admin", "added_by_superadmin"]

    def _get_existing_course_works(self, student_id):
        """Получает список существующих курсовых работ студента"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT discipline, semester, file_path 
                FROM course_works 
                WHERE student_id = ?
            ''', (student_id,))
            return {(row[0], row[1]): row[2] for row in cursor.fetchall()}

    async def _parser_worker(self):
        """Обрабатывает очередь студентов для парсинга"""
        while self.is_running:
            try:
                # Получаем студента из очереди
                student = await self.parsing_queue.get()
                student_id, telegram_id, student_group = student

                try:
                    # Получаем существующие курсовые работы
                    existing_course_works = self._get_existing_course_works(student_id)

                    # Парсим данные студента
                    name, grades, subjects, course_works = parse_student_data(
                        student_id, 
                        telegram_id=telegram_id,  # Передаем telegram_id как есть
                        student_group=student_group,
                        skip_existing_course_works=existing_course_works
                    )

                    if name != "Unknown":
                        # Сохраняем данные в базу
                        save_to_db(
                            student_id=student_id,
                            name=name,
                            grades=grades,
                            subjects=subjects,
                            telegram_id=telegram_id,  # Передаем telegram_id как есть
                            student_group=student_group
                        )
                        logger.info(f"Успешно обновлены данные для студента {name} (ID: {student_id})")
                    else:
                        logger.warning(f"Не удалось получить данные для студента {student_id}")

                except Exception as e:
                    logger.error(f"Ошибка при парсинге студента {student_id}: {e}")

                finally:
                    # Отмечаем задачу как выполненную
                    self.parsing_queue.task_done()
                    # Делаем небольшую паузу между парсингом студентов
                    await asyncio.sleep(5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в обработчике парсинга: {e}")
                await asyncio.sleep(60)  # Ждем минуту перед повторной попыткой 