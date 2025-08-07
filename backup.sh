
#!/bin/bash

# Скрипт резервного копирования базы данных и логов

BACKUP_DIR="backups"
DATE=$(date +%Y%m%d_%H%M%S)
DB_PATH="data/memo_bot.db"

# Создание папки для резервных копий
mkdir -p $BACKUP_DIR

echo "🔄 Создание резервной копии..."

# Резервная копия базы данных
if [ -f "$DB_PATH" ]; then
    sqlite3 $DB_PATH ".backup $BACKUP_DIR/memo_bot_$DATE.db"
    echo "✅ База данных: $BACKUP_DIR/memo_bot_$DATE.db"
else
    echo "⚠️ База данных не найдена: $DB_PATH"
fi

# Резервная копия логов
if [ -f "memo_bot.log" ]; then
    cp memo_bot.log "$BACKUP_DIR/memo_bot_$DATE.log"
    echo "✅ Логи: $BACKUP_DIR/memo_bot_$DATE.log"
fi

# Резервная копия конфигурации
if [ -f ".env" ]; then
    cp .env "$BACKUP_DIR/env_$DATE.backup"
    echo "✅ Конфигурация: $BACKUP_DIR/env_$DATE.backup"
fi

# Очистка старых резервных копий (старше 30 дней)
find $BACKUP_DIR -name "*.db" -mtime +30 -delete
find $BACKUP_DIR -name "*.log" -mtime +30 -delete
find $BACKUP_DIR -name "*.backup" -mtime +30 -delete

echo "✅ Резервное копирование завершено!"
echo "📁 Файлы сохранены в: $BACKUP_DIR/"
