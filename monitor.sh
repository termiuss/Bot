
#!/bin/bash

# Скрипт мониторинга состояния бота

echo "🤖 Мониторинг Telegram бота"
echo "============================"

# Проверка статуса сервиса
echo "📊 Статус сервиса:"
if systemctl is-active --quiet memo-bot; then
    echo "✅ Сервис активен"
else
    echo "❌ Сервис неактивен"
fi

# Проверка процесса
echo -e "\n🔍 Процессы:"
pgrep -f "python.*main.py" > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ Процесс Python запущен"
    echo "PID: $(pgrep -f "python.*main.py")"
else
    echo "❌ Процесс Python не найден"
fi

# Использование ресурсов
echo -e "\n💾 Использование ресурсов:"
if pgrep -f "python.*main.py" > /dev/null; then
    ps -p $(pgrep -f "python.*main.py") -o pid,pcpu,pmem,cmd --no-headers
fi

# Проверка базы данных
echo -e "\n🗄️ База данных:"
if [ -f "data/memo_bot.db" ]; then
    echo "✅ База данных существует"
    echo "Размер: $(du -h data/memo_bot.db | cut -f1)"
    
    # Проверка целостности
    sqlite3 data/memo_bot.db "PRAGMA integrity_check;" | head -1
else
    echo "❌ База данных не найдена"
fi

# Проверка логов
echo -e "\n📋 Логи:"
if [ -f "memo_bot.log" ]; then
    echo "✅ Файл логов существует"
    echo "Размер: $(du -h memo_bot.log | cut -f1)"
    echo "Последние ошибки:"
    tail -20 memo_bot.log | grep -i error | tail -3
else
    echo "⚠️ Файл логов не найден"
fi

# Проверка свободного места
echo -e "\n💽 Свободное место:"
df -h . | tail -1 | awk '{print "Использовано: " $3 "/" $2 " (" $5 ")"}'

# Проверка времени работы
echo -e "\n⏰ Время работы системы:"
uptime

echo -e "\n============================"
echo "Мониторинг завершен $(date)"
