
#!/bin/bash

# Установка зависимостей
echo "Устанавливаем зависимости..."
sudo apt update
sudo apt install -y python3 python3-pip python3-venv

# Создание виртуального окружения
echo "Создаем виртуальное окружение..."
python3 -m venv venv
source venv/bin/activate

# Установка Python пакетов
echo "Устанавливаем Python пакеты..."
pip install -r requirements.txt

# Настройка прав
chmod +x main.py
chmod +x start.sh

echo "Установка завершена!"
echo "1. Отредактируйте .env файл с вашими токенами"
echo "2. Запустите бота: source venv/bin/activate && python3 main.py"
echo "3. Для автозапуска настройте systemd сервис"
