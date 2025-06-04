#!/bin/bash
echo "Setting up Telegram Bot..."

# Обновление системы
sudo apt update && sudo apt upgrade -y

# Установка Python и зависимостей
sudo apt install -y python3 python3-pip python3-venv git sqlite3

# # Клонирование репозитория
# read -p "Введите URL репозитория: " repo_url
# git clone $repo_url student_bot
# cd student_bot

# Виртуальное окружение
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# # Конфигурация
# cp config.json.example config.json
# echo "Заполните config.json с вашим Telegram-токеном."
# nano config.json

# # Настройка автозапуска
# sudo cp systemd.service.example /etc/systemd/system/telegram_bot.service
# sudo systemctl daemon-reload
# sudo systemctl enable telegram_bot
# sudo systemctl start telegram_bot

# echo "✅ Установка завершена!"
# echo "Проверьте статус: sudo systemctl status telegram_bot"