
# Memo Telegram Bot

Телеграм бот для управления заказами и сопровождающими.

## Установка на VPS

### 1. Подготовка сервера

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git
```

### 2. Клонирование проекта

```bash
git clone <your-repo-url>
cd memo-bot
```

### 3. Установка зависимостей

```bash
chmod +x install.sh
./install.sh
```

### 4. Настройка

Отредактируйте файл `.env`:

```bash
nano .env
```

Заполните необходимые переменные:
- `BOT_TOKEN` - токен бота от @BotFather
- `ADMIN_IDS` - ID администраторов через запятую

### 5. Запуск

Для разового запуска:
```bash
chmod +x start.sh
./start.sh
```

### 6. Автозапуск через systemd

Отредактируйте файл `memo_bot.service`:
```bash
sudo nano memo_bot.service
```

Замените `YOUR_USERNAME` и `/path/to/your/bot` на актуальные значения.

Скопируйте сервис:
```bash
sudo cp memo_bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable memo_bot
sudo systemctl start memo_bot
```

Проверка статуса:
```bash
sudo systemctl status memo_bot
```

Просмотр логов:
```bash
sudo journalctl -u memo_bot -f
```

## Команды для управления

- Запуск: `sudo systemctl start memo_bot`
- Остановка: `sudo systemctl stop memo_bot`
- Перезапуск: `sudo systemctl restart memo_bot`
- Автозапуск: `sudo systemctl enable memo_bot`

## Структура файлов

- `main.py` - основной файл бота
- `schema.sql` - схема базы данных
- `.env` - переменные окружения
- `requirements.txt` - зависимости Python
- `install.sh` - скрипт установки
- `start.sh` - скрипт запуска
- `memo_bot.service` - сервис systemd

## Troubleshooting

Если бот не запускается, проверьте:
1. Правильность токена в `.env`
2. Доступность интернета
3. Логи: `tail -f memo_bot.log`
