
#!/bin/bash

echo "🤖 Установка Telegram бота на VPS..."

# Обновление системы
sudo apt update && sudo apt upgrade -y

# Установка Python и pip
sudo apt install python3 python3-pip python3-venv -y

# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt

# Создание базы данных
python3 -c "
import sqlite3
import os

# Создание базы данных в корневой директории
conn = sqlite3.connect('database.db')
with open('schema.sql', 'r', encoding='utf-8') as f:
    conn.executescript(f.read())
conn.close()
print('✅ База данных создана успешно!')
"

# Создание systemd сервиса
sudo tee /etc/systemd/system/memo-bot.service > /dev/null <<EOF
[Unit]
Description=Memo Telegram Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
Environment=PATH=$(pwd)/venv/bin
ExecStart=$(pwd)/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo "✅ Установка завершена!"
echo "📝 Не забудьте:"
echo "1. Скопировать .env.example в .env и заполнить данные"
echo "2. Запустить сервис: sudo systemctl enable memo-bot && sudo systemctl start memo-bot"
echo "3. Проверить статус: sudo systemctl status memo-bot"
