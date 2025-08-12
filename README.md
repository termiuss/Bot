
# Telegram Bot для управления эскорт-услугами

## Описание
Telegram бот с админ-панелью для управления сопровождающими, балансами, заказами и отчетами.

## Функции
- ✅ Управление сопровождающими (добавление/удаление)
- 💰 Управление балансами
- 🚫 Система банов и ограничений
- 📈 Отчеты и аналитика
- 📞 Система поддержки
- 📤 Экспорт данных

## Установка на VPS

### 1. Подготовка сервера
```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Клонирование репозитория
git clone <your-repo-url>
cd <repo-name>
```

### 2. Автоматическая установка
```bash
# Запуск скрипта установки
chmod +x install.sh
./install.sh
```

### 3. Настройка окружения
```bash
# Копирование и настройка переменных окружения
cp .env.example .env
nano .env
```

Заполните `.env` файл:
```env
BOT_TOKEN=your_bot_token_from_botfather
ADMIN_IDS=123456789,987654321
DB_PATH=data/memo_bot.db
LOG_LEVEL=INFO
LOG_FILE=memo_bot.log
```

### 4. Запуск сервиса
```bash
# Включение и запуск сервиса
sudo systemctl enable memo-bot
sudo systemctl start memo-bot

# Проверка статуса
sudo systemctl status memo-bot
```

## Управление ботом

### Использование скрипта управления
```bash
# Сделать исполняемым
chmod +x bot_control.sh

# Команды управления
./bot_control.sh start    # Запуск
./bot_control.sh stop     # Остановка
./bot_control.sh restart  # Перезапуск
./bot_control.sh status   # Статус
./bot_control.sh logs     # Просмотр логов
./bot_control.sh update   # Обновление из git
```

### Ручное управление через systemctl
```bash
sudo systemctl start memo-bot    # Запуск
sudo systemctl stop memo-bot     # Остановка
sudo systemctl restart memo-bot  # Перезапуск
sudo systemctl status memo-bot   # Статус
```

## Просмотр логов
```bash
# Логи systemd
sudo journalctl -u memo-bot -f

# Логи приложения
tail -f memo_bot.log
```

## Структура проекта
```
├── main.py              # Основной файл бота
├── schema.sql           # Схема базы данных
├── requirements.txt     # Зависимости Python
├── .env.example         # Пример переменных окружения
├── install.sh           # Скрипт установки
├── bot_control.sh       # Скрипт управления
├── README.md            # Документация
└── data/
    └── memo_bot.db      # База данных SQLite
```

## Backup базы данных
```bash
# Создание резервной копии
sqlite3 data/memo_bot.db ".backup backup_$(date +%Y%m%d_%H%M%S).db"

# Восстановление из резервной копии
sqlite3 data/memo_bot.db ".restore backup_20231201_120000.db"
```

## Обновление
```bash
# Через скрипт управления
./bot_control.sh update

# Или вручную
git pull
source venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart memo-bot
```

## Мониторинг
- Статус сервиса: `sudo systemctl status memo-bot`
- Логи в реальном времени: `sudo journalctl -u memo-bot -f`
- Использование ресурсов: `top -p $(pgrep -f "python.*main.py")`

## Решение проблем

### Бот не запускается
1. Проверьте токен в `.env` файле
2. Убедитесь что все зависимости установлены: `pip install -r requirements.txt`
3. Проверьте логи: `sudo journalctl -u memo-bot -n 50`

### База данных не создается
1. Убедитесь что папка `data/` существует и доступна для записи
2. Проверьте права доступа: `chmod 755 data/`
3. Запустите создание БД вручную: `python3 -c "import asyncio; from main import init_db; asyncio.run(init_db())"`

### Проблемы с правами
```bash
# Установка правильного владельца
sudo chown -R $USER:$USER .

# Установка прав на исполнение
chmod +x *.sh
```
