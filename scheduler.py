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
        """Планирует парсинг студентов каждые 2 часа"""
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

                # Ждем 2 часа перед следующей проверкой
                await asyncio.sleep(5 * 60)  # 5 минут в секундах

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

    def _get_student_ratings(self, student_id):
        """Получает текущие оценки студента из базы данных"""
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM students WHERE student_id=?", (student_id,))
            row = cursor.fetchone()
            if not row:
                return None
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))

    def _compare_ratings(self, old_ratings, new_ratings):
        """Сравнивает старые и новые оценки, возвращает список изменений"""
        changes = []
        if not old_ratings or not new_ratings:
            return changes

        # Проверяем изменение группы
        old_group = old_ratings.get('student_group')
        new_group = new_ratings.get('student_group')
        if old_group != new_group and new_group is not None:
            changes.append({
                'type': 'group',
                'old_value': old_group,
                'new_value': new_group
            })

        # Проверяем изменения в оценках
        for key, new_value in new_ratings.items():
            if "(модуль" in str(key):
                old_value = old_ratings.get(key)
                if old_value != new_value and new_value not in ["не изучает", None, "None"]:
                    subject = key.split(' (модуль')[0]
                    module = key.split(' (модуль ')[1].replace(')', '')
                    changes.append({
                        'type': 'grade',
                        'subject': subject,
                        'module': module,
                        'old_value': old_value,
                        'new_value': new_value
                    })
        return changes

    def _format_changes_message(self, name, changes):
        """Форматирует сообщение об изменениях в успеваемости"""
        if not changes:
            return None

        message = f"📊 Изменения в данных {name}:\n\n"
        
        # Сначала выводим изменение группы, если оно есть
        group_changes = [c for c in changes if c['type'] == 'group']
        if group_changes:
            change = group_changes[0]
            message += f"👥 Изменение группы:\n"
            message += f"   Было: {change['old_value'] if change['old_value'] else '-'}\n"
            message += f"   Стало: {change['new_value']}\n\n"

        # Затем выводим изменения в оценках
        grade_changes = [c for c in changes if c['type'] == 'grade']
        if grade_changes:
            message += "📚 Изменения в успеваемости:\n\n"
            for change in grade_changes:
                message += f"📚 {change['subject']} (модуль {change['module']}):\n"
                message += f"   Было: {change['old_value'] if change['old_value'] not in ['не изучает', None, 'None'] else '-'}\n"
                message += f"   Стало: {change['new_value']}\n\n"
        
        return message

    async def _parser_worker(self):
        """Обрабатывает очередь студентов для парсинга"""
        while self.is_running:
            try:
                # Получаем студента из очереди
                student = await self.parsing_queue.get()
                student_id, telegram_id, student_group = student

                try:
                    # Получаем текущие оценки студента
                    old_ratings = self._get_student_ratings(student_id)

                    # Получаем существующие курсовые работы
                    existing_course_works = self._get_existing_course_works(student_id)

                    # Парсим данные студента
                    name, grades, subjects, course_works = parse_student_data(
                        student_id, 
                        telegram_id=telegram_id,
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
                            telegram_id=telegram_id,
                            student_group=student_group
                        )

                        # Обновляем группу в таблице course_works
                        with get_db_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute('''
                                UPDATE course_works 
                                SET student_group = ? 
                                WHERE student_id = ?
                            ''', (student_group, student_id))
                            conn.commit()
                            if cursor.rowcount > 0:
                                logger.info(f"Обновлена группа в курсовых работах для студента {name} (ID: {student_id})")

                        # Получаем новые оценки после сохранения
                        new_ratings = self._get_student_ratings(student_id)

                        # Сравниваем оценки и отправляем уведомление если есть изменения
                        if telegram_id and not self._is_system_telegram_id(telegram_id):
                            changes = self._compare_ratings(old_ratings, new_ratings)
                            if changes:
                                message = self._format_changes_message(name, changes)
                                if message:
                                    try:
                                        await self.application.bot.send_message(
                                            chat_id=telegram_id,
                                            text=message
                                        )
                                        logger.info(f"Отправлено уведомление об изменениях студенту {name} (ID: {student_id})")
                                    except Exception as e:
                                        logger.error(f"Ошибка при отправке уведомления студенту {student_id}: {e}")

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