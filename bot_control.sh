
#!/bin/bash

case "$1" in
    start)
        echo "🚀 Запуск бота..."
        sudo systemctl start memo-bot
        sudo systemctl status memo-bot
        ;;
    stop)
        echo "⏹️ Остановка бота..."
        sudo systemctl stop memo-bot
        ;;
    restart)
        echo "🔄 Перезапуск бота..."
        sudo systemctl restart memo-bot
        sudo systemctl status memo-bot
        ;;
    status)
        echo "📊 Статус бота:"
        sudo systemctl status memo-bot
        ;;
    logs)
        echo "📋 Логи бота:"
        sudo journalctl -u memo-bot -f
        ;;
    update)
        echo "📥 Обновление бота..."
        git pull
        source venv/bin/activate
        pip install -r requirements.txt
        sudo systemctl restart memo-bot
        echo "✅ Бот обновлен и перезапущен!"
        ;;
    *)
        echo "Использование: $0 {start|stop|restart|status|logs|update}"
        exit 1
        ;;
esac
