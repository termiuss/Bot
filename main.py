import asyncio
import logging
import csv
import os
import traceback
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart, Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.client.default import DefaultBotProperties
import aiosqlite
from aiogram.exceptions import TelegramAPIError

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('memo_bot.log', encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("\n Не указан BOT_TOKEN в .env файле")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
if not ADMIN_IDS:
    raise ValueError("\n Не указаны ADMIN_IDS в .env файле")
DB_PATH = "database.db"

# Ссылки на документы
OFFER_URL = "https://telegra.ph/Publichnaya-oferta-07-25-7"
PRIVACY_URL = "https://telegra.ph/Politika-konfidencialnosti-07-19-25"
RULES_URL = "https://telegra.ph/Pravila-07-19-160"

# Инициализация бота
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# Словарь для отслеживания контекста пользователя
user_context = {}

# Константы сообщений
MESSAGES = {
    "welcome": (
        "🎮 Добро пожаловать в бота сопровождения PUBG Mobile - Metro Royale!\n"
        "💼 Комиссия сервиса: 20% от суммы заказа."
    ),
    "no_access": "❌ У вас нет доступа к этой команде.",
    "no_squads": "🏠 Нет доступных сквадов.",
    "no_escorts": "👤 Нет зарегистрированных сопровождающих.",
    "no_orders": "📋 Сейчас нет доступных заказов.",
    "no_active_orders": "📋 У вас нет активных заказов.",
    "error": "⚠️ Произошла ошибка. Пожалуйста, попробуйте снова.",
    "invalid_format": "❌ Неверный формат ввода. Попробуйте снова.",
    "order_completed": "✅ Заказ #{order_id} завершен пользователем @{username} (Telegram ID: {telegram_id}, PUBG ID: {pubg_id})!",
    "order_already_completed": "⚠️ Заказ #{order_id} уже завершен.",
    "balance_added": "💸 Баланс {amount} руб. начислен пользователю {user_id}",
    "squad_full": "⚠️ Сквад '{squad_name}' уже имеет максимум 6 участников!",
    "squad_too_small": "⚠️ В скваде '{squad_name}' должно быть минимум 2 участника для принятия заказа!",
    "order_added": "📝 Заказ #{order_id} добавлен! Сумма: {amount} руб., Описание: {description}, Клиент: {customer}",
    "rules_not_accepted": "📜 Пожалуйста, примите правила, оферту и политику конфиденциальности.",
    "user_banned": "🚫 Вы заблокированы.",
    "user_restricted": "⛔ Ваш доступ к сопровождениям ограничен до {date}.",
    "balance_zeroed": "💰 Баланс пользователя {user_id} обнулен.",
    "pubg_id_updated": "🔢 PUBG ID успешно обновлен!",
    "ping": "🏓 Бот активен!",
    "order_taken": "📝 Заказ #{order_id} принят сквадом {squad_name}!\nУчастники:\n{participants}",
    "order_not_enough_members": "⚠️ В скваде '{squad_name}' недостаточно участников (минимум 2)!",
    "order_already_in_progress": "⚠️ Заказ #{order_id} уже в наборе или принят!",
    "order_joined": "✅ Вы присоединились к набору для заказа #{order_id}!\nТекущий состав:\n{participants}",
    "order_confirmed": "✅ Заказ #{order_id} подтвержден и принят!\nУчастники:\n{participants}",
    "not_in_squad": "⚠️ Вы не состоите в скваде!",
    "max_participants": "⚠️ Достигнуто максимальное количество участников!",
    "rating_submitted": "🌟 Оценка {rating} для заказа #{order_id} сохранена! Репутация обновлена.",
    "rate_order": "🌟 Поставьте оценку за заказ #{order_id} (1-5):",
    "payout_log": "💸 Выплата: @{username} получил {amount} руб. за заказ #{order_id}. Дата: {date}",
    "payout_request": "📥 Запрос выплаты от @{username} на сумму {amount} руб. за заказ #{order_id}",
    "payout_receipt": "✅ Я, @{username}, получил оплату {amount} руб. за заказ #{order_id}.",
    "export_success": "📤 Данные успешно экспортированы в {filename}!",
    "no_data_to_export": "⚠️ Нет данных для экспорта.",
    "reminder": "⏰ Напоминание: Заказ #{order_id} не завершен более 12 часов! Пожалуйста, завершите его.",
    "squad_deleted": "🏠 Сквад '{squad_name}' успешно расформирован!",
    "cancel_action": "🚫 Действие отменено.",
    "support_request": "📩 Введите ваше сообщение для поддержки:",
    "support_sent": "✅ Ваше сообщение отправлено администраторам!",
    "user_unbanned": "🔒 Пользователь @{username} разблокирован!",
    "user_unrestricted": "🔓 Ограничения для пользователя @{username} сняты!"
}

# Состояния FSM
class Form(StatesGroup):
    squad_name = State()
    escort_info = State()
    assign_squad = State()
    remove_escort = State()
    zero_balance = State()
    pubg_id = State()
    balance_amount = State()
    complete_order = State()
    add_order = State()
    ban_duration = State()
    restrict_duration = State()
    rate_order = State()
    ban_permanent = State()
    profit_user = State()
    payout_request = State()
    delete_squad = State()
    support_message = State()
    support_reply = State()
    unban_user = State()
    unrestrict_user = State()
    # Новые состояния для данных пользователей
    view_cd_user_id = State()
    view_cd_value = State()
    view_id_user_id = State()
    view_id_value = State()
    view_squad_user_id = State()
    view_squad_value = State()
    view_other_user_id = State()
    view_other_value = State()
    add_cd_user_id = State()
    add_cd_value = State()
    add_id_user_id = State()
    add_id_value = State()
    add_squad_user_id = State()
    add_squad_value = State()
    add_other_user_id = State()
    add_other_value = State()
    delete_user_info = State()
    add_user_info = State()
    add_user_info_text = State()
    view_user_info = State()
    delete_order = State()
    admin_rate_order = State()
    add_reputation = State()
    remove_reputation = State()
    # Новые состояния для управления лидерами
    leader_user_id = State()
    leader_squad_name = State()
    remove_leader_user_id = State()
    # Состояния для функций лидера
    rename_squad = State()
    add_member = State()
    remove_member = State()
    # Состояния для системы связи
    contact_leader_message = State()
    contact_user_id = State()
    contact_user_message = State()
    broadcast_message = State()

# --- Функции базы данных ---
async def init_db():
    logger.info(f"Попытка подключения к базе данных: {DB_PATH}")
    try:
        # Создаем директории если их нет
        import os
        db_dir = os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else "."
        if not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.executescript('''
                CREATE TABLE IF NOT EXISTS squads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rating REAL DEFAULT 0,
                    rating_count INTEGER DEFAULT 0,
                    leader_id INTEGER,
                    FOREIGN KEY (leader_id) REFERENCES escorts (id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS escorts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    pubg_id TEXT,
                    squad_id INTEGER,
                    balance REAL DEFAULT 0,
                    reputation INTEGER DEFAULT 0,
                    completed_orders INTEGER DEFAULT 0,
                    rating REAL DEFAULT 0,
                    rating_count INTEGER DEFAULT 0,
                    total_rating REAL DEFAULT 0,
                    is_banned INTEGER DEFAULT 0,
                    ban_until TIMESTAMP,
                    restrict_until TIMESTAMP,
                    rules_accepted INTEGER DEFAULT 0,
                    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    memo_order_id TEXT UNIQUE NOT NULL,
                    customer_info TEXT NOT NULL,
                    amount REAL NOT NULL,
                    commission_amount REAL DEFAULT 0.0,
                    status TEXT DEFAULT 'pending',
                    squad_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    rating INTEGER DEFAULT 0,
                    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS order_escorts (
                    order_id INTEGER,
                    escort_id INTEGER,
                    pubg_id TEXT,
                    PRIMARY KEY (order_id, escort_id),
                    FOREIGN=KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                    FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS order_applications (
                    order_id INTEGER,
                    escort_id INTEGER,
                    squad_id INTEGER,
                    pubg_id TEXT,
                    PRIMARY KEY (order_id, escort_id),
                    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                    FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE,
                    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS payouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER,
                    escort_id INTEGER,
                    amount REAL,
                    payout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE SET NULL,
                    FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS action_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_type TEXT,
                    user_id INTEGER,
                    order_id INTEGER,
                    description TEXT,
                    action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                -- Таблица для дополнительной информации о пользователях
                CREATE TABLE IF NOT EXISTS user_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    data_type TEXT NOT NULL, -- 'cd', 'id', 'squad', 'other'
                    data_value TEXT,
                    added_by INTEGER, -- ID администратора, добавившего данные
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (telegram_id) REFERENCES escorts (telegram_id) ON DELETE CASCADE
                );
                -- Таблица для дополнительной информации о пользователях (новая)
                CREATE TABLE IF NOT EXISTS user_additional_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER,
                    info_text TEXT,
                    added_by INTEGER, -- ID администратора, добавившего информацию
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (telegram_id) REFERENCES escorts (telegram_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id);
                CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id);
                CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id);
                CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id);
                CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id);
                CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date);
                CREATE INDEX IF NOT EXISTS idx_user_data_telegram_id ON user_data (telegram_id);
                CREATE INDEX IF NOT EXISTS idx_user_data_data_type ON user_data (data_type);
                -- Таблица лидеров сквадов
                CREATE TABLE IF NOT EXISTS squad_leaders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    leader_id INTEGER NOT NULL,
                    squad_id INTEGER NOT NULL,
                    appointed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (leader_id) REFERENCES escorts (id) ON DELETE CASCADE,
                    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE CASCADE,
                    UNIQUE(leader_id, squad_id)
                );
                CREATE INDEX IF NOT EXISTS idx_user_additional_info_telegram_id ON user_additional_info (telegram_id);
                CREATE INDEX IF NOT EXISTS idx_squad_leaders_leader_id ON squad_leaders (leader_id);
                CREATE INDEX IF NOT EXISTS idx_squad_leaders_squad_id ON squad_leaders (squad_id);
            ''')
            await conn.commit()
        logger.info("База данных успешно инициализирована")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка инициализации базы данных: {e}\n\n{traceback.format_exc()}")
        raise

async def log_action(action_type: str, user_id: int, order_id: int = None, description: str = None):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO action_log (action_type, user_id, order_id, description) VALUES (?, ?, ?, ?)",
                (action_type, user_id, order_id, description)
            )
            await conn.commit()
        logger.info(f"Лог действия: {action_type}, user_id: {user_id}, order_id: {order_id}, description: {description}")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка при записи лога действия: {e}\n\n{traceback.format_exc()}")

async def get_escort(telegram_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id, pubg_id, balance, reputation, completed_orders, username, "
                "rating, rating_count, is_banned, ban_until, restrict_until, rules_accepted "
                "FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            return await cursor.fetchone()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в get_escort для {telegram_id}: {e}\n\n{traceback.format_exc()}")
        return None

async def add_escort(telegram_id: int, username: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO escorts (telegram_id, username, rules_accepted) VALUES (?, ?, 0)",
                (telegram_id, username)
            )
            await conn.commit()
        logger.info(f"Добавлен пользователь {telegram_id} (@{username})")
        return True
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в add_escort для {telegram_id}: {e}\n\n{traceback.format_exc()}")
        return False

async def get_squad_escorts(squad_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT telegram_id, username, pubg_id, rating FROM escorts WHERE squad_id = ?", (squad_id,)
            )
            return await cursor.fetchall()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в get_squad_escorts для squad_id {squad_id}: {e}\n\n{traceback.format_exc()}")
        return []

async def get_squad_info(squad_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT s.name, COUNT(e.id) as member_count,
                       COALESCE(SUM(e.completed_orders), 0) as total_orders,
                       COALESCE(SUM(e.balance), 0) as total_balance,
                       s.rating, s.rating_count
                FROM squads s
                LEFT JOIN escorts e ON e.squad_id = s.id
                WHERE s.id = ?
                GROUP BY s.id
                ''', (squad_id,)
            )
            return await cursor.fetchone()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в get_squad_info для squad_id {squad_id}: {e}\n\n{traceback.format_exc()}")
        return None

async def notify_squad(squad_id: int, message: str):
    if squad_id is None:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id FROM escorts")
            escorts = await cursor.fetchall()
    else:
        escorts = await get_squad_escorts(squad_id)
    for telegram_id, *_ in escorts:
        try:
            await bot.send_message(telegram_id, message)
        except TelegramAPIError as e:
            logger.warning(f"Не удалось уведомить {telegram_id}: {e}")

async def notify_squad_with_mentions(squad_id: int, message: str):
    """Отправляет уведомление сквадам с упоминаниями участников"""
    if squad_id is None:
        return

    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, s.name
                FROM escorts e
                JOIN squads s ON e.squad_id = s.id
                WHERE e.squad_id = ?
                ''', (squad_id,)
            )
            squad_members = await cursor.fetchall()

        if not squad_members:
            return

        # Создаем список упоминаний
        mentions = []
        for telegram_id, username, squad_name in squad_members:
            if username:
                mentions.append(f"@{username}")
            else:
                mentions.append(f"[Пользователь](tg://user?id={telegram_id})")

        # Формируем сообщение с упоминаниями
        mention_text = ", ".join(mentions)
        full_message = f"{message}\n\n👥 Участники сквада: {mention_text}"

        # Отправляем уведомление всем участникам сквада
        for telegram_id, username, _ in squad_members:
            try:
                await bot.send_message(telegram_id, full_message, parse_mode=ParseMode.MARKDOWN)
            except TelegramAPIError:
                # Пытаемся отправить без форматирования
                try:
                    await bot.send_message(telegram_id, message)
                except TelegramAPIError:
                    pass

    except Exception as e:
        logger.error(f"Ошибка в notify_squad_with_mentions: {e}")
        # Fallback на обычные уведомления
        await notify_squad(squad_id, message)

async def notify_admins(message: str, reply_markup=None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message, reply_markup=reply_markup)
        except TelegramAPIError as e:
            logger.warning(f"Не удалось уведомить админа {admin_id}: {e}")

async def get_order_applications(order_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id, e.squad_id, s.name
                FROM order_applications oa
                JOIN escorts e ON oa.escort_id = e.id
                LEFT JOIN squads s ON e.squad_id = s.id
                WHERE oa.order_id = ?
                ''', (order_id,)
            )
            return await cursor.fetchall()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в get_order_applications для order_id {order_id}: {e}\n\n{traceback.format_exc()}")
        return []

async def get_order_info(memo_order_id: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, customer_info, amount, status, squad_id, commission_amount FROM orders WHERE memo_order_id = ?",
                (memo_order_id,)
            )
            return await cursor.fetchone()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в get_order_info для memo_order_id {memo_order_id}: \n{e}\n{traceback.format_exc()}")
        return None

async def update_escort_reputation(escort_id: int, rating: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Обновляем систему рейтинга в звездах
            await conn.execute(
                '''
                UPDATE escorts
                SET total_rating = total_rating + ?,
                    rating_count = rating_count + 1
                WHERE id = ?
                ''',
                (rating, escort_id)
            )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в update_escort_reputation для escort_id {escort_id}: \n{e}\n{traceback.format_exc()}")

async def update_squad_reputation(squad_id: int, rating: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем текущие значения рейтинга
            cursor = await conn.execute(
                "SELECT rating, rating_count FROM squads WHERE id = ?", (squad_id,)
            )
            row = await cursor.fetchone()
            if row:
                current_rating, rating_count = row
                new_rating_count = rating_count + 1
                new_rating = (current_rating * rating_count + rating) / new_rating_count

                await conn.execute(
                    '''
                    UPDATE squads
                    SET rating = ?,
                        rating_count = ?
                    WHERE id = ?
                    ''',
                    (new_rating, new_rating_count, squad_id)
                )
            await conn.commit()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в update_squad_reputation для squad_id {squad_id}: \n{e}\n{traceback.format_exc()}")

async def get_order_escorts(order_id: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, oe.pubg_id, e.squad_id, s.name
                FROM order_escorts oe
                JOIN escorts e ON oe.escort_id = e.id
                LEFT JOIN squads s ON e.squad_id = s.id
                WHERE oe.order_id = ?
                ''', (order_id,)
            )
            return await cursor.fetchall()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в get_order_escorts для order_id {order_id}: {e}\n\n{traceback.format_exc()}")
        return []

async def export_orders_to_csv():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.memo_order_id, o.customer_info, o.amount, o.commission_amount, o.status, o.created_at, o.completed_at,
                s.name as squad_name, p.amount as payout_amount, p.payout_date
                FROM orders o
                LEFT JOIN squads s ON o.squad_id = s.id
                LEFT JOIN payouts p ON o.id = p.order_id
                '''
            )
            rows = await cursor.fetchall()
        if not rows:
            return None
        filename = f"orders_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Memo Order ID', 'Customer Info', 'Amount', 'Commission', 'Status', 'Created At', 'Completed At',
                             'Squad', 'Payout Amount', 'Payout Date'])
            writer.writerows(rows)
        return filename
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в export_orders_to_csv: {e}\n{traceback.format_exc()}")
        return None

async def check_pending_orders():
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT id, memo_order_id, squad_id
                FROM orders
                WHERE status = 'in_progress' AND created_at < ?
                ''', ((datetime.now() - timedelta(hours=12)).isoformat(),)
            )
            orders = await cursor.fetchall()

        for order_id, memo_order_id, squad_id in orders:
            await notify_squad(squad_id, MESSAGES["reminder"].format(order_id=memo_order_id))
            await log_action("reminder_sent", None, order_id, f"Напоминание о заказе #{memo_order_id}")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в check_pending_orders: {e}\n{traceback.format_exc()}")

# --- Функции для работы с данными пользователей ---
async def get_user_data(telegram_id: int, data_type: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT data_value FROM user_data WHERE telegram_id = ? AND data_type = ?",
                (telegram_id, data_type)
            )
            row = await cursor.fetchone()
            return row[0] if row else None
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных при получении данных пользователя {telegram_id} ({data_type}): {e}\n{traceback.format_exc()}")
        return None

async def add_user_data(telegram_id: int, data_type: str, data_value: str, added_by: int):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Проверяем, существует ли уже запись для этого пользователя и типа данных
            cursor = await conn.execute(
                "SELECT id FROM user_data WHERE telegram_id = ? AND data_type = ?",
                (telegram_id, data_type)
            )
            existing_record = await cursor.fetchone()

            if existing_record:
                # Если запись существует, обновляем ее
                await conn.execute(
                    "UPDATE user_data SET data_value = ?, added_by = ?, added_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (data_value, added_by, existing_record[0])
                )
            else:
                # Если записи нет, создаем новую
                await conn.execute(
                    "INSERT INTO user_data (telegram_id, data_type, data_value, added_by) VALUES (?, ?, ?, ?)",
                    (telegram_id, data_type, data_value, added_by)
                )
            await conn.commit()
        return True
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных при добавлении данных пользователя {telegram_id} ({data_type}): {e}\n{traceback.format_exc()}")
        return False

async def get_all_user_data_by_type(data_type: str):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT ud.telegram_id, e.username, ud.data_value
                FROM user_data ud
                LEFT JOIN escorts e ON ud.telegram_id = e.telegram_id
                WHERE ud.data_type = ?
                ORDER BY ud.telegram_id
                ''',
                (data_type,)
            )
            return await cursor.fetchall()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных при получении всех данных типа {data_type}: {e}\n{traceback.format_exc()}")
        return []

# --- Проверка доступа ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def is_leader(user_id: int) -> bool:
    """Проверяет, является ли пользователь лидером сквада"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT COUNT(*) FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            result = await cursor.fetchone()
            return result[0] > 0
    except aiosqlite.Error as e:
        logger.error(f"Ошибка проверки лидерства для {user_id}: {e}")
        return False

async def get_user_rating_position(user_id: int):
    """Получает позицию пользователя в рейтинге"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем рейтинг пользователя
            cursor = await conn.execute(
                "SELECT total_rating, rating_count FROM escorts WHERE telegram_id = ?",
                (user_id,)
            )
            user_data = await cursor.fetchone()
            if not user_data or user_data[1] == 0:
                return None, 0.0

            user_rating = user_data[0] / user_data[1]

            # Получаем все рейтинги для подсчета позиции
            cursor = await conn.execute(
                '''
                SELECT telegram_id, total_rating, rating_count
                FROM escorts
                WHERE rating_count > 0
                ORDER BY (total_rating / rating_count) DESC
                '''
            )
            ratings = await cursor.fetchall()

            position = 1
            for telegram_id, total_rating, rating_count in ratings:
                if telegram_id == user_id:
                    return position, user_rating
                position += 1

            return None, user_rating
    except aiosqlite.Error as e:
        logger.error(f"Ошибка получения позиции рейтинга для {user_id}: {e}")
        return None, 0.0

async def get_squad_rating_position(user_id: int):
    """Получает позицию сквада пользователя в рейтинге"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем сквад пользователя
            cursor = await conn.execute(
                "SELECT squad_id FROM escorts WHERE telegram_id = ?",
                (user_id,)
            )
            user_data = await cursor.fetchone()
            if not user_data or not user_data[0]:
                return None, None, 0.0

            squad_id = user_data[0]

            # Получаем название сквада
            cursor = await conn.execute(
                "SELECT name FROM squads WHERE id = ?",
                (squad_id,)
            )
            squad_name_result = await cursor.fetchone()
            squad_name = squad_name_result[0] if squad_name_result else "Unknown"

            # Вычисляем средний рейтинг сквада
            cursor = await conn.execute(
                '''
                SELECT AVG(total_rating / rating_count) as avg_rating
                FROM escorts
                WHERE squad_id = ? AND rating_count > 0
                ''', (squad_id,)
            )
            squad_rating_result = await cursor.fetchone()
            squad_rating = squad_rating_result[0] if squad_rating_result and squad_rating_result[0] else 0.0

            # Получаем все рейтинги сквадов
            cursor = await conn.execute(
                '''
                SELECT s.id, s.name, AVG(e.total_rating / e.rating_count) as avg_rating
                FROM squads s
                JOIN escorts e ON s.id = e.squad_id
                WHERE e.rating_count > 0
                GROUP BY s.id
                HAVING COUNT(e.id) > 0
                ORDER BY avg_rating DESC
                '''
            )
            squad_ratings = await cursor.fetchall()

            position = 1
            for sid, sname, rating in squad_ratings:
                if sid == squad_id:
                    return position, squad_name, squad_rating
                position += 1

            return None, squad_name, squad_rating
    except aiosqlite.Error as e:
        logger.error(f"Ошибка получения позиции сквада для {user_id}: {e}")
        return None, None, 0.0

# --- Клавиатуры ---
async def get_menu_keyboard(user_id: int):
    base_keyboard = [
        [KeyboardButton(text="📋 Заказы")],
        [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="🔢 Ввести PUBG ID")],
        [KeyboardButton(text="ℹ️ Информация"), KeyboardButton(text="📩 Поддержка")],
        [KeyboardButton(text="⭐ Рейтинг пользователей"), KeyboardButton(text="🏆 Рейтинг сквадов")],
        [KeyboardButton(text="📥 Получить выплату"), KeyboardButton(text="👨‍💼 Связаться с лидером")],
    ]

    # Добавляем кнопки лидера если пользователь является лидером
    if await is_leader(user_id):
        base_keyboard.append([KeyboardButton(text="👥 Управление участниками"), KeyboardButton(text="🏠 Управление сквадом")])

    # Добавляем админ-панель для админов
    if is_admin(user_id):
        base_keyboard.append([KeyboardButton(text="🚪 Админ-панель")])

    keyboard = ReplyKeyboardMarkup(
        keyboard=base_keyboard,
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Сквады"), KeyboardButton(text="👤 Сопровождающие")],
            [KeyboardButton(text="📝 Заказы"), KeyboardButton(text="🚫 Баны/ограничения")],
            [KeyboardButton(text="💰 Балансы"), KeyboardButton(text="👥 Пользователи")],
            [KeyboardButton(text="👑 Управление лидерами"), KeyboardButton(text="📊 Прочее")],
            [KeyboardButton(text="📞 Связь"), KeyboardButton(text="🚪 Выйти из админ-панели")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_orders_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Доступные заказы")],
            [KeyboardButton(text="📋 Мои заказы"), KeyboardButton(text="✅ Завершить заказ")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_squads_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Добавить сквад"), KeyboardButton(text="📋 Список сквадов")],
            [KeyboardButton(text="🗑️ Расформировать сквад")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_escorts_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👤 Добавить сопровождающего"), KeyboardButton(text="🗑️ Удалить сопровождающего")],
            [KeyboardButton(text="💰 Балансы сопровождающих")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_bans_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚫 Бан навсегда"), KeyboardButton(text="⏰ Бан на время")],
            [KeyboardButton(text="🔓 Снять бан"), KeyboardButton(text="🔓 Снять ограничение")],
            [KeyboardButton(text="⛔ Ограничить")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_balances_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💸 Начислить"), KeyboardButton(text="💰 Обнулить баланс")],
            [KeyboardButton(text="📊 Все балансы")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_misc_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📜 Журнал действий"), KeyboardButton(text="📤 Экспорт данных")],
            [KeyboardButton(text="📊 Отчет за месяц")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_admin_orders_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Добавить заказ"), KeyboardButton(text="❌ Удалить заказ")],
            [KeyboardButton(text="⭐ Оценить заказ")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_users_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👥 Список пользователей"), KeyboardButton(text="📊 Экспорт CSV")],
            [KeyboardButton(text="ℹ️ Информация о пользователе"), KeyboardButton(text="📈 Доход пользователя")],
            [KeyboardButton(text="➕ Добавить информацию"), KeyboardButton(text="🗑️ Стереть информацию")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_reputation_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить репутацию"), KeyboardButton(text="➖ Снять репутацию")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

# Клавиатура для группы "Данные пользователей" в админ-панели
# def get_user_data_submenu_keyboard():
#     keyboard = ReplyKeyboardMarkup(
#         keyboard=[
#             # Общие кнопки
#             [KeyboardButton(text="📊 Общее КД"), KeyboardButton(text="📊 Общее Айди")],
#             [KeyboardButton(text="📊 Общий Сквад"), KeyboardButton(text="📊 Общее Прочее")],
#             # Просмотр
#             [KeyboardButton(text="🕒 КД"), KeyboardButton(text="🔢 Айди")],
#             [KeyboardButton(text="🏠 Сквад"), KeyboardButton(text="📝 Прочее")],
#             # Добавление
#             [KeyboardButton(text="➕ Добавить КД"), KeyboardButton(text="➕ Добавить Айди")],
#             [KeyboardButton(text="➕ Добавить Сквад"), KeyboardButton(text="➕ Добавить Прочее")],
#             [KeyboardButton(text="🔙 Назад")],
#         ],
#         resize_keyboard=True,
#         one_time_keyboard=False
#     )
#     return keyboard

def get_rules_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Принять условия")],
            [KeyboardButton(text="📜 Политика конфиденциальности")],
            [KeyboardButton(text="📖 Правила")],
            [KeyboardButton(text="📜 Публичная оферта")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard

def get_cancel_keyboard(is_admin: bool = False):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚫 Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def get_order_keyboard(order_id: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Готово", callback_data=f"join_order_{order_id}")],
        [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_id}")]
    ])
    return keyboard

def get_confirmed_order_keyboard(order_id: str, is_admin: bool = False):
    buttons = [[InlineKeyboardButton(text="Завершить заказ", callback_data=f"complete_order_{order_id}")]]

    # Кнопка отмены только для админов
    if is_admin:
        buttons.append([InlineKeyboardButton(text="Отменить заказ", callback_data=f"cancel_confirmed_order_{order_id}")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_confirmed_order_keyboard_user(order_id: str):
    # Для обычных пользователей - никаких кнопок после старта заказа
    return None

def get_rating_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1 ⭐", callback_data=f"rate_{order_id}_1"),
            InlineKeyboardButton(text="2 ⭐", callback_data=f"rate_{order_id}_2"),
            InlineKeyboardButton(text="3 ⭐", callback_data=f"rate_{order_id}_3")
        ],
        [
            InlineKeyboardButton(text="4 ⭐", callback_data=f"rate_{order_id}_4"),
            InlineKeyboardButton(text="5 ⭐", callback_data=f"rate_{order_id}_5")
        ]
    ])
    return keyboard

# --- Новые клавиатуры для управления лидерами ---
def get_leaders_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👑 Добавить лидера команды")],
            [KeyboardButton(text="📋 Список лидеров"), KeyboardButton(text="🗑️ Убрать лидера")],
            [KeyboardButton(text="📞 Связаться с лидером")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_squad_management_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Переименовать сквад")],
            [KeyboardButton(text="📋 Список заказов")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_members_management_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить участника")],
            [KeyboardButton(text="➖ Удалить участника")],
            [KeyboardButton(text="📋 Список участников")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_communication_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👤 Связаться с пользователем")],
            [KeyboardButton(text="📢 Сделать объявление")],
            [KeyboardButton(text="🔙 Назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

# --- Проверка доступа ---
async def check_access(message: types.Message, initial_start: bool = False):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    try:
        escort = await get_escort(user_id)
        if not escort:
            if not await add_escort(user_id, username):
                logger.error(f"Не удалось создать профиль для пользователя {user_id}")
                await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
                return False
            escort = await get_escort(user_id)
            if not escort:
                logger.error(f"Не удалось получить профиль после создания для пользователя {user_id}")
                await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
                return False
        if escort[9]:  # is_banned
            await message.answer(MESSAGES["user_banned"], reply_markup=ReplyKeyboardRemove())
            return False
        if escort[10] and datetime.fromisoformat(escort[10]) > datetime.now():  # ban_until
            formatted_date = datetime.fromisoformat(escort[10]).strftime("%d.%m.%Y %H:%M")
            await message.answer(MESSAGES["user_banned"].format(date=formatted_date), reply_markup=ReplyKeyboardRemove())
            return False
        if escort[11] and datetime.fromisoformat(escort[11]) > datetime.now():  # restrict_until
            formatted_date = datetime.fromisoformat(escort[11]).strftime("%d.%m.%Y %H:%M")
            await message.answer(MESSAGES["user_restricted"].format(date=formatted_date), reply_markup=ReplyKeyboardRemove())
            return False
        if not escort[12] and initial_start:  # rules_accepted
            await message.answer(MESSAGES["rules_not_accepted"], reply_markup=get_rules_keyboard())
            return False
        return True
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в check_access для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
        return False
    except ValueError as e:
        logger.error(f"Ошибка формата данных в check_access для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
        return False

# --- Обработчики ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    try:
        if not await check_access(message, initial_start=True):
            return
        user_context[user_id] = 'main_menu'
        await message.answer(f"{MESSAGES['welcome']}\n\n Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} (@{username}) запустил бота")
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cmd_start для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    try:
        await message.answer(MESSAGES["ping"], reply_markup=await get_menu_keyboard(message.from_user.id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cmd_ping для {message.from_user.id}: \n{e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "✅ Принять условия")
async def accept_rules(message: types.Message):
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("UPDATE escorts SET rules_accepted = 1 WHERE telegram_id = ?", (user_id,))
            await conn.commit()
        user_context[user_id] = 'main_menu'
        await message.answer(f"✅ Условия приняты! Добро пожаловать!\n\n📌 Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} принял условия")
        await log_action("accept_rules", user_id, None, "Пользователь принял условия")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в accept_rules для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в accept_rules для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "🔢 Ввести PUBG ID")
async def enter_pubg_id(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    try:
        await message.answer("🔢 Введите ваш PUBG ID:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.pubg_id)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в enter_pubg_id для \n{message.from_user.id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(message.from_user.id))
        await state.clear()

@dp.message(Form.pubg_id)
async def process_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    pubg_id = message.text.strip()
    if not pubg_id:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard())
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE escorts SET pubg_id = ? WHERE telegram_id = ?",
                (pubg_id, user_id)
            )
            await conn.commit()
        await message.answer("🔢 PUBG ID успешно обновлен!", reply_markup=await get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} обновил PUBG ID: {pubg_id}")
        await log_action("update_pubg_id", user_id, None, f"Обновлен PUBG ID: {pubg_id}")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_pubg_id для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_pubg_id для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.message(F.text == "ℹ️ Информация")
async def info_handler(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)],
            [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)],
            [InlineKeyboardButton(text="📜 Публичная оферта", url=OFFER_URL)],
            [InlineKeyboardButton(text="🔗 Система репутации", url="https://telegra.ph/Sistema-reputacii-08-17-2")]
        ])
        response = (
            "ℹ️ Информация о боте:\n"
            "\n Комиссия сервиса: 20% от суммы заказа."
        )
        await message.answer(response, reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в info_handler: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(message.from_user.id))

@dp.callback_query(F.data == "about_project")
async def about_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        response = (
            "ℹ️ О проекте:\n"
            "Этот бот предназначен для распределения заказов по сопровождению в Metro Royale. "
            "Все действия фиксируются, выплаты прозрачны."
        )
        await callback.message.answer(response, reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в about_project для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

# Обработчики для просмотра данных конкретного пользователя
# @dp.message(F.text == "🕒 КД")
# async def view_cd_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("🕒 Введите айди пользователя для просмотра КД:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.view_cd_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в view_cd_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.view_cd_user_id)
# async def process_view_cd_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         data = await get_user_data(target_user_id, "cd")

#         if data:
#             await message.answer(f"🕒 КД пользователя {target_user_id}: {data}", reply_markup=get_user_data_submenu_keyboard())
#         else:
#             await message.answer(f"🕒 Нет данных КД для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_view_cd_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(F.text == "🔢 Айди")
# async def view_id_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("🔢 Введите айди пользователя для просмотра Айди:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.view_id_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в view_id_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.view_id_user_id)
# async def process_view_id_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         data = await get_user_data(target_user_id, "id")

#         if data:
#             await message.answer(f"🔢 Айди пользователя {target_user_id}: {data}", reply_markup=get_user_data_submenu_keyboard())
#         else:
#             await message.answer(f"🔢 Нет данных Айди для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_view_id_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(F.text == "🏠 Сквад")
# async def view_squad_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("🏠 Введите айди пользователя для просмотра Сквада:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.view_squad_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в view_squad_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.view_squad_user_id)
# async def process_view_squad_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         data = await get_user_data(target_user_id, "squad")

#         if data:
#             await message.answer(f"🏠 Сквад пользователя {target_user_id}: {data}", reply_markup=get_user_data_submenu_keyboard())
#         else:
#             await message.answer(f"🏠 Нет данных Сквада для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_view_squad_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(F.text == "📝 Прочее")
# async def view_other_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("📝 Введите айди пользователя для просмотра Прочего:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.view_other_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в view_other_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.view_other_user_id)
# async def process_view_other_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         data = await get_user_data(target_user_id, "other")

#         if data:
#             await message.answer(f"📝 Прочее пользователя {target_user_id}: {data}", reply_markup=get_user_data_submenu_keyboard())
#         else:
#             await message.answer(f"📝 Нет прочих данных для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_view_other_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# # Обработчики для добавления данных
# @dp.message(F.text == "➕ Добавить КД")
# async def add_cd_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("➕ Введите айди пользователя:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_cd_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в add_cd_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.add_cd_user_id)
# async def process_add_cd_user_id(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         await state.update_data(target_user_id=target_user_id)
#         await message.answer("🕒 Введите значение для КД:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_cd_value)
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))

# @dp.message(Form.add_cd_value)
# async def process_add_cd_value(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         data = await state.get_data()
#         target_user_id = data.get('target_user_id')
#         cd_value = message.text.strip()

#         if await add_user_data(target_user_id, "cd", cd_value, user_id):
#             await message.answer(f"✅ КД '{cd_value}' добавлено для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())
#             await log_action("add_cd_data", user_id, None, f"Добавлено КД '{cd_value}' для пользователя {target_user_id}")
#         else:
#             await message.answer("❌ Ошибка при сохранении данных", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_add_cd_value для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(F.text == "➕ Добавить Айди")
# async def add_id_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("➕ Введите айди пользователя:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_id_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в add_id_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.add_id_user_id)
# async def process_add_id_user_id(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         await state.update_data(target_user_id=target_user_id)
#         await message.answer("🔢 Введите значение для Айди:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_id_value)
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))

# @dp.message(Form.add_id_value)
# async def process_add_id_value(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         data = await state.get_data()
#         target_user_id = data.get('target_user_id')
#         id_value = message.text.strip()

#         if await add_user_data(target_user_id, "id", id_value, user_id):
#             await message.answer(f"✅ Айди '{id_value}' добавлено для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())
#             await log_action("add_id_data", user_id, None, f"Добавлено Айди '{id_value}' для пользователя {target_user_id}")
#         else:
#             await message.answer("❌ Ошибка при сохранении данных", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_add_id_value для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(F.text == "➕ Добавить Сквад")
# async def add_squad_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("➕ Введите айди пользователя:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_squad_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в add_squad_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.add_squad_user_id)
# async def process_add_squad_user_id(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         await state.update_data(target_user_id=target_user_id)
#         await message.answer("🏠 Введите значение для Сквада:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_squad_value)
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))

# @dp.message(Form.add_squad_value)
# async def process_add_squad_value(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         data = await state.get_data()
#         target_user_id = data.get('target_user_id')
#         squad_value = message.text.strip()

#         if await add_user_data(target_user_id, "squad", squad_value, user_id):
#             await message.answer(f"✅ Сквад '{squad_value}' добавлен для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())
#             await log_action("add_squad_data", user_id, None, f"Добавлен Сквад '{squad_value}' для пользователя {target_user_id}")
#         else:
#             await message.answer("❌ Ошибка при сохранении данных", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_add_squad_value для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(F.text == "➕ Добавить Прочее")
# async def add_other_data(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if not is_admin(user_id):
#         await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
#         return
#     try:
#         await message.answer("➕ Введите айди пользователя:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_other_user_id)
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в add_other_data для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

# @dp.message(Form.add_other_user_id)
# async def process_add_other_user_id(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         target_user_id = int(message.text.strip())
#         await state.update_data(target_user_id=target_user_id)
#         await message.answer("📝 Введите значение для Прочего:", reply_markup=get_cancel_keyboard(True))
#         await state.set_state(Form.add_other_value)
#     except ValueError:
#         await message.answer("❌ Неверный формат ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))

# @dp.message(Form.add_other_value)
# async def process_add_other_value(message: types.Message, state: FSMContext):
#     user_id = message.from_user.id
#     if message.text == "🚫 Отмена":
#         await message.answer(MESSAGES["cancel_action"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()
#         return

#     try:
#         data = await state.get_data()
#         target_user_id = data.get('target_user_id')
#         other_value = message.text.strip()

#         if await add_user_data(target_user_id, "other", other_value, user_id):
#             await message.answer(f"✅ Прочее '{other_value}' добавлено для пользователя {target_user_id}", reply_markup=get_user_data_submenu_keyboard())
#             await log_action("add_other_data", user_id, None, f"Добавлено Прочее '{other_value}' для пользователя {target_user_id}")
#         else:
#             await message.answer("❌ Ошибка при сохранении данных", reply_markup=get_user_data_submenu_keyboard())

#         await state.clear()
#     except TelegramAPIError as e:
#         logger.error(f"Ошибка Telegram API в process_add_other_value для {user_id}: {e}")
#         await message.answer(MESSAGES["error"], reply_markup=get_user_data_submenu_keyboard())
#         await state.clear()

@dp.message(F.text == "📜 Политика конфиденциальности")
async def rules_links(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)]
        ])
        await message.answer("📜 Политика конфиденциальности:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в rules_links: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "📖 Правила")
async def rules_links(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)]
        ])
        await message.answer("📖 Правила:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в rules_links: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "📜 Публичная оферта")
async def rules_links(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Публичная оферта", url=OFFER_URL)]
        ])
        await message.answer("📜 Публичная оферта:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в rules_links: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "👤 Мой профиль")
async def my_profile(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            return
        escort_id, squad_id, pubg_id, balance, reputation, completed_orders, username, rating, rating_count, _, ban_until, restrict_until, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad = await cursor.fetchone()
        # Получаем данные о рейтинге в звездах
        cursor = await conn.execute(
            "SELECT total_rating, rating_count FROM escorts WHERE telegram_id = ?",
            (user_id,)
        )
        rating_data = await cursor.fetchone()

        stars_rating = "Нет оценок"
        if rating_data and rating_data[1] > 0:
            star_rating = rating_data[0] / rating_data[1]
            stars_rating = f"★ {star_rating:.2f} / 5.00"

        avg_rating = rating / rating_count if rating_count > 0 else 0
        response = (
            f"👤 Ваш профиль:\n\n"
            f"Username: @{username or 'Unknown'}\n"
            f"PUBG ID: {pubg_id or 'не указан'}\n"
            f"Сквад: {squad[0] if squad else 'не назначен'}\n"
            f"Баланс: {balance:.2f} руб.\n"
            f"Выполнено заказов: {completed_orders}\n"
            f"Рейтинг в звездах: {stars_rating}\n"
        )
        await message.answer(response, reply_markup=await get_menu_keyboard(user_id))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в my_profile для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в my_profile для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Заказы")
async def orders_menu(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        user_context[user_id] = 'orders_submenu'
        await message.answer("\n Управление заказами:", reply_markup=get_orders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в orders_menu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Доступные заказы")
async def available_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        # Получаем информацию о пользователе и его скваде
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("❌ Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            return

        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort

        if not squad_id:
            await message.answer(MESSAGES["not_in_squad"], reply_markup=await get_menu_keyboard(user_id))
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем заказы, которые либо свободны, либо уже набираются этим сквадом
            cursor = await conn.execute(
                '''
                SELECT DISTINCT o.id, o.memo_order_id, o.customer_info, o.amount,
                       (SELECT COUNT(*) FROM order_applications oa WHERE oa.order_id = o.id) as app_count,
                       (SELECT oa.squad_id FROM order_applications oa WHERE oa.order_id = o.id LIMIT 1) as recruiting_squad
                FROM orders o
                WHERE o.status = 'pending'
                AND (
                    o.id NOT IN (SELECT order_id FROM order_applications)
                    OR
                    o.id IN (SELECT order_id FROM order_applications WHERE squad_id = ?)
                )
                ORDER BY o.created_at
                ''', (squad_id,)
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer("📋 Нет доступных заказов для вашего сквада.", reply_markup=await get_menu_keyboard(user_id))
            return

        # Создаем клавиатуру с информацией о статусе набора
        keyboard_buttons = []
        for db_id, order_id, customer, amount, app_count, recruiting_squad in orders:
            button_text = f"#{order_id} - {customer}, {amount:.2f} руб."
            if app_count > 0 and recruiting_squad == squad_id:
                button_text += f" (Набор: {app_count} чел.)"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"select_order_{db_id}")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        # Получаем название сквада
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad_result = await conn.fetchone()
            squad_name = squad_result[0] if squad_result else "Unknown"

        await message.answer(f"📋 Доступные заказы для сквада '{squad_name}':", reply_markup=keyboard)

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в available_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в available_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Мои заказы")
async def my_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем заказы из order_escorts (принятые заказы) и order_applications (заявки)
            cursor = await conn.execute(
                '''
                SELECT DISTINCT o.memo_order_id, o.customer_info, o.amount, o.status
                FROM orders o
                WHERE o.id IN (
                    SELECT order_id FROM order_escorts WHERE escort_id = ?
                    UNION
                    SELECT order_id FROM order_applications WHERE escort_id = ?
                )
                ORDER BY o.created_at DESC
                ''', (escort_id, escort_id)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer(MESSAGES["no_active_orders"], reply_markup=await get_menu_keyboard(user_id))
            return
        response = "\n Ваши заказы:\n"
        for order_id, customer, amount, status in orders:
            status_text = "Ожидает" if status == "pending" else "В процессе" if status == "in_progress" else "Завершен"
            response += f"#{order_id} - {customer}, {amount:.2f} руб., Статус: {status_text}\n"
        await message.answer(response, reply_markup=await get_menu_keyboard(user_id))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в my_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в my_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "✅ Завершить заказ")
async def complete_order(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            # Показываем заказы в процессе, где пользователь является участником
            cursor = await conn.execute(
                '''
                SELECT DISTINCT o.memo_order_id, o.id, o.squad_id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE o.status = 'in_progress'
                AND e.telegram_id = ?
                ''', (user_id,)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer("\n У вас нет активных заказов для завершения.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return
        response = "\n Выберите заказ для завершения:\n"
        for order_id, _, _, amount in orders:
            response += f"#{order_id} - {amount:.2f} руб.\n"
        await message.answer(response, reply_markup=get_cancel_keyboard())
        await state.set_state(Form.complete_order)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в complete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в complete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Form.complete_order)
async def process_complete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    order_id = message.text.strip()
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id, _, pubg_id, _, _, _, username, _, _, _, _, _, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            # Исправленный запрос с проверкой принадлежности заказа пользователю
            cursor = await conn.execute(
                """
                SELECT o.id, o.status, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE o.memo_order_id = ?
                AND o.status = 'in_progress'
                AND e.telegram_id = ?
                """,
                (order_id, user_id)
            )
            order = await cursor.fetchone()
            if not order:
                await message.answer(f"\n Заказ #{order_id} не найден или не в процессе.", reply_markup=await get_menu_keyboard(user_id))
                await state.clear()
                return
            order_db_id, _, order_amount = order

            # Проверяем количество участников в заказе
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts WHERE order_id = ?",
                (order_db_id,)
            )
            participant_count = (await cursor.fetchone())[0]

            # Проверяем, достаточно ли участников (минимум 2)
            if participant_count < 2:
                await message.answer(f"\n Недостаточно сопровождающих для завершения заказа (требуется минимум 2, есть {participant_count}).", reply_markup=await get_menu_keyboard(user_id))
                await state.clear()
                return

            # Рассчитываем выплату с учетом 20% комиссии
            commission = order_amount * 0.2
            payout_per_participant = (order_amount - commission) / participant_count

            await conn.execute(
                '''
                UPDATE orders SET status = 'completed', completed_at = ? WHERE id = ?
                ''', (datetime.now().isoformat(), order_db_id)
            )

            # Начисляем баланс участникам (80% от суммы заказа, разделенные поровну)
            await conn.execute(
                '''
                UPDATE escorts SET
                    completed_orders = completed_orders + 1,
                    reputation = reputation + 200,
                    balance = balance + ?
                WHERE id IN (
                    SELECT escort_id FROM order_escorts WHERE order_id = ?
                )
                ''', (payout_per_participant, order_db_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            ) + f"\n💰 Участникам начислено по: {payout_per_participant:.2f} руб.",
            reply_markup=await get_menu_keyboard(user_id)
        )
        await notify_admins(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            ) + f"\n💰 Участникам начислено по: {payout_per_participant:.2f} руб."
        )
        await log_action(
            "complete_order",
            user_id,
            order_db_id,
            f"Заказ #{order_id} завершен пользователем @{username}, начислено по {payout_per_participant:.2f} руб."
        )
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_complete_order для {user_id}: \n{e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_complete_order для {user_id}: \n{e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.message(F.text == "⭐ Оценить заказ")
async def admin_rate_orders(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT memo_order_id, customer_info, amount FROM orders WHERE status = 'completed' AND rating = 0"
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer("📋 Нет завершённых заказов для оценки.", reply_markup=get_admin_orders_submenu_keyboard())
            return

        for memo_order_id, customer_info, amount in orders:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="1⭐", callback_data=f"admin_rate_{memo_order_id}_1"),
                    InlineKeyboardButton(text="2⭐", callback_data=f"admin_rate_{memo_order_id}_2"),
                    InlineKeyboardButton(text="3⭐", callback_data=f"admin_rate_{memo_order_id}_3"),
                    InlineKeyboardButton(text="4⭐", callback_data=f"admin_rate_{memo_order_id}_4"),
                    InlineKeyboardButton(text="5⭐", callback_data=f"admin_rate_{memo_order_id}_5")
                ]
            ])

            await message.answer(
                f"📝 Заказ #{memo_order_id}\n"
                f"Клиент: {customer_info}\n"
                f"Сумма: {amount:.2f} руб.\n\n"
                f"Пожалуйста, оцените выполнение заказа:",
                reply_markup=keyboard
            )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в admin_rate_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_orders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_rate_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_orders_submenu_keyboard())


@dp.callback_query(F.data.startswith("admin_rate_"))
async def admin_rate_order_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("У вас нет доступа к этой функции.")
        return
    try:
        parts = callback.data.split("_")
        memo_order_id = parts[2]
        rating = int(parts[3])

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id FROM orders WHERE memo_order_id = ? AND status = 'completed'",
                (memo_order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.edit_text("❌ Заказ не найден или не завершён.")
                await callback.answer()
                return

            order_db_id, squad_id = order

            # Обновляем рейтинг заказа
            await conn.execute(
                "UPDATE orders SET rating = ? WHERE id = ?",
                (rating, order_db_id)
            )

            # Добавляем дополнительную репутацию участникам в зависимости от оценки
            reputation_bonus = rating * 100  # 1⭐=100, 2⭐=200, ..., 5⭐=500

            cursor = await conn.execute(
                "SELECT escort_id FROM order_escorts WHERE order_id = ?",
                (order_db_id,)
            )
            escorts = await cursor.fetchall()

            for (escort_id,) in escorts:
                await conn.execute(
                    "UPDATE escorts SET reputation = reputation + ? WHERE id = ?",
                    (reputation_bonus, escort_id)
                )

            # Обновляем рейтинг сквада
            if squad_id:
                await update_squad_reputation(squad_id, rating)

            await conn.commit()

        await callback.message.edit_text(
            f"✅ Заказ #{memo_order_id} оценён на {rating}⭐\n"
            f"Участникам добавлено по +{reputation_bonus} репутации."
        )

        await log_action("admin_rate_order", user_id, order_db_id, f"Админ оценил заказ #{memo_order_id} на {rating} звёзд")
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка в admin_rate_order_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.answer("Произошла ошибка")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в admin_rate_order_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.answer("Произошла ошибка")
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_rate_order_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.answer("Произошла ошибка")

@dp.message(F.text == "📥 Получить выплату")
async def request_payout(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id, _, _, balance, _, _, username, _, _, _, _, _, _ = escort

        if balance <= 0:
            await message.answer("❗ У вас нет средств для вывода", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return

        await message.answer(f"\n Введите сумму для выплаты (доступно: {balance:.2f} руб.):", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.payout_request)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в request_payout для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в request_payout для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Form.payout_request)
async def process_payout_request(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    try:
        payout_amount = float(message.text.strip())
        if payout_amount <= 0:
            await message.answer("\n Сумма должна быть больше 0", reply_markup=get_cancel_keyboard())
            return

        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id, _, _, balance, _, _, username, _, _, _, _, _, _ = escort

        if payout_amount > balance:
            await message.answer(f"\n Недостаточно средств на балансе. Доступно: {balance:.2f} руб.", reply_markup=get_cancel_keyboard())
            return

        # Создаем клавиатуру для администраторов
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять выплату", callback_data=f"approve_payout_{user_id}_{payout_amount}")],
            [InlineKeyboardButton(text="❌ Отклонить выплату", callback_data=f"reject_payout_{user_id}_{payout_amount}")]
        ])

        await message.answer(
            f"\n Запрос на выплату {payout_amount:.2f} руб. отправлен администраторам!\n\n"
            f"Просьба связаться с администратором @ItMEMOO\n"
            f"Или @MemoSpamBlock_bot",
            reply_markup=await get_menu_keyboard(user_id)
        )
        await notify_admins(
            f"\n Запрос выплаты от @{username or 'Unknown'} (ID: {user_id}) на сумму {payout_amount:.2f} руб.",
            reply_markup=admin_keyboard
        )
        await log_action(
            "payout_request",
            user_id,
            None,
            f"Запрос выплаты {payout_amount:.2f} руб."
        )
        await state.clear()
    except ValueError:
        await message.answer("\n Неверный формат суммы. Введите числовое значение:", reply_markup=get_cancel_keyboard())
        return
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_payout_request для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_payout_request для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.callback_query(F.data.startswith("select_order_"))
async def select_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT memo_order_id FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer("\n Заказ не найден.", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return
        await callback.message.edit_text(f"\n Заказ #{order[0]}. Нажмите 'Готово' или 'Отмена'.", reply_markup=get_order_keyboard(order_db_id))
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в select_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в select_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("join_order_"))
async def join_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort
        if not pubg_id:
            await callback.message.answer("\n Укажите PUBG ID!", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        if not squad_id:
            await callback.message.answer(MESSAGES["not_in_squad"], reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT status, memo_order_id FROM orders WHERE id = ?", (order_db_id,)
            )
            order = await cursor.fetchone()
            if not order or order[0] != 'pending':
                await callback.message.answer(MESSAGES["order_already_in_progress"].format(order_id=order[1]), reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Проверяем, набирается ли заказ другим сквадом
            cursor = await conn.execute(
                "SELECT squad_id FROM order_applications WHERE order_id = ? LIMIT 1", (order_db_id,)
            )
            existing_squad = await cursor.fetchone()
            if existing_squad and existing_squad[0] != squad_id:
                await callback.message.answer("⚠️ Этот заказ уже набирается другим сквадом!", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Проверяем, не присоединился ли пользователь уже
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                await callback.message.answer("✅ Вы уже присоединились к этому заказу!", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Проверяем текущее количество участников из этого сквада
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND squad_id = ?",
                (order_db_id, squad_id)
            )
            participant_count = (await cursor.fetchone())[0]

            # Добавляем пользователя к заказу
            await conn.execute(
                "INSERT INTO order_applications (order_id, escort_id, squad_id, pubg_id) VALUES (?, ?, ?, ?)",
                (order_db_id, escort_id, squad_id, pubg_id)
            )
            await conn.commit()
        # Получаем участников только из текущего сквада
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id, e.squad_id, s.name
                FROM order_applications oa
                JOIN escorts e ON oa.escort_id = e.id
                LEFT JOIN squads s ON e.squad_id = s.id
                WHERE oa.order_id = ? AND oa.squad_id = ?
                ''', (order_db_id, squad_id)
            )
            squad_applications = await cursor.fetchall()

        participants = "\n".join(
            f"@{username or 'Unknown'} (PUBG ID: {pubg_id})"
            for _, username, pubg_id, _, squad_name in squad_applications
        )

        memo_order_id = order[1]

        # Получаем название сквада
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad_result = await conn.fetchone()
            current_squad_name = squad_result[0] if squad_result else "Unknown"

        response = f"📋 Заказ #{memo_order_id} - Сквад '{current_squad_name}':\n\n"
        response += f"👥 Участники:\n{participants if participants else 'Никого пока нет'}\n\n"
        response += f"📊 Участников: {len(squad_applications)} (минимум 2 для старта)"

        # Кнопка "Начать выполнение" всегда показывается
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Начать выполнение", callback_data=f"start_order_{order_db_id}")],
            [InlineKeyboardButton(text="❌ Покинуть заказ", callback_data=f"leave_order_{order_db_id}")]
        ])
        await callback.message.edit_text(response, reply_markup=keyboard)
        await callback.message.answer(
            f"✅ Вы присоединились к заказу #{memo_order_id}!\n\n👥 Участники сквада '{current_squad_name}':\n{participants}",
            reply_markup=await get_menu_keyboard(user_id)
        )
        await log_action("join_order", user_id, order_db_id, f"Пользователь {user_id} присоединился к заказу #{memo_order_id}")
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в join_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в join_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("start_order_"))
async def start_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort or not escort[1]:
            await callback.message.answer(MESSAGES["not_in_squad"], reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT memo_order_id, status, amount FROM orders WHERE id = ?",
                (order_db_id,)
            )
            order = await cursor.fetchone()
            if not order or order[1] != 'pending':
                await callback.message.answer(MESSAGES["order_already_in_progress"].format(order_id=order[0]), reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return
            # Получаем участников только из сквада пользователя
            cursor = await conn.execute(
                "SELECT escort_id, squad_id FROM order_applications WHERE order_id = ? AND squad_id = ?",
                (order_db_id, squad_id)
            )
            squad_applications = await cursor.fetchall()

            if len(squad_applications) < 2:  # Требуется минимум 2 участника из одного сквада
                async with aiosqlite.connect(DB_PATH) as conn:
                    cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
                    squad_result = await conn.fetchone()
                    current_squad_name = squad_result[0] if squad_result else "Unknown"

                new_text = f"⚠️ Недостаточно участников для начала выполнения заказа!\n\nСквад: {current_squad_name}\nТребуется: минимум 2 участника\nСейчас: {len(squad_applications)} участников"

                # Обновляем сообщение
                try:
                    await callback.message.edit_text(
                        new_text,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🚀 Начать выполнение", callback_data=f"start_order_{order_db_id}")],
                            [InlineKeyboardButton(text="❌ Покинуть заказ", callback_data=f"leave_order_{order_db_id}")]
                        ])
                    )
                except TelegramAPIError as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"Ошибка при обновлении сообщения: {e}")

                await callback.answer("⚠️ Недостаточно участников для начала выполнения заказа!")
                return

            # Всех участников берем только из одного сквада
            winning_squad_id = squad_id
            valid_applications = squad_applications
            commission = order[2] * 0.2
            for escort_id, _ in valid_applications:
                cursor = await conn.execute("SELECT pubg_id FROM escorts WHERE id = ?", (escort_id,))
                pubg_id = (await cursor.fetchone())[0]
                await conn.execute(
                    "INSERT INTO order_escorts (order_id, escort_id, pubg_id) VALUES (?, ?, ?)",
                    (order_db_id, escort_id, pubg_id)
                )
                # Removed: UPDATE completed_orders +1 here
            await conn.execute(
                "UPDATE orders SET status = 'in_progress', squad_id = ?, commission_amount = ? WHERE id = ?",
                (winning_squad_id, commission, order_db_id)
            )
            await conn.execute(
                "DELETE FROM order_applications WHERE order_id = ?",
                (order_db_id,)
            )
            await conn.commit()
        order_id = order[0]
        participants = "\n".join(
            f"@{username or 'Unknown'} (PUBG ID: {pubg_id}, Squad: {squad_name or 'No squad'})"
            for _, username, pubg_id, _, squad_name in await get_order_escorts(order_db_id)
        )
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (winning_squad_id,))
            squad_result = await conn.fetchone()
            squad_name = squad_result[0] if squad_result else "Unknown"
        response = MESSAGES["order_taken"].format(order_id=order_id, squad_name=squad_name, participants=participants)

        # Для админа показываем кнопки управления
        keyboard = get_confirmed_order_keyboard(order_id, is_admin=is_admin(user_id))
        await callback.message.edit_text(response, reply_markup=keyboard)

        # Уведомляем участников заказа (без кнопок для обычных пользователей)
        for telegram_id, _, _, _, _ in await get_order_escorts(order_db_id):
            try:
                user_keyboard = get_confirmed_order_keyboard(order_id, is_admin=is_admin(telegram_id)) if is_admin(telegram_id) else None
                await bot.send_message(
                    telegram_id,
                    f"Заказ #{order_id} начат!\n{participants}",
                    reply_markup=user_keyboard
                )
            except TelegramAPIError as e:
                logger.warning(f"Не удалось уведомить {telegram_id}: {e}")
        await notify_squad(
            winning_squad_id,
            MESSAGES["order_taken"].format(
                order_id=order_id,
                squad_name=squad_name,
                participants=participants
            )
        )
        await log_action("start_order", user_id, order_db_id, f"Заказ #{order_id} начат на скваде {squad_name}")
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в start_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в start_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("complete_order_"))
async def complete_order_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    memo_order_id = callback.data.split('_')[-1]
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("\n Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id, _, pubg_id, _, _, _, username, _, _, _, _, _, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, status, amount FROM orders WHERE memo_order_id = ? AND status = 'in_progress'",
                (memo_order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer(f"\n Заказ #{memo_order_id} не найден или не в процессе.", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return
            order_db_id, _, amount = order

            # Проверяем количество участников в заказе
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts WHERE order_id = ?",
                (order_db_id,)
            )
            participant_count = (await cursor.fetchone())[0]

            # Рассчитываем выплату с учетом 20% комиссии
            commission = amount * 0.2
            payout_per_participant = (amount - commission) / participant_count

            await conn.execute(
                '''
                UPDATE orders SET status = 'completed', completed_at = ? WHERE id = ?
                ''', (datetime.now().isoformat(), order_db_id)
            )
            await conn.execute(
                '''
                UPDATE escorts SET
                    completed_orders = completed_orders + 1,
                    reputation = reputation + 200,
                    balance = balance + ?
                WHERE id IN (
                    SELECT escort_id FROM order_escorts WHERE order_id = ?
                )
                ''', (payout_per_participant, order_db_id)
            )
            await conn.commit()
        await callback.message.edit_text(
            MESSAGES["order_completed"].format(
                order_id=memo_order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            ) + f"\n💰 Участникам начислено по: {payout_per_participant:.2f} руб.",
            reply_markup=None
        )
        await notify_admins(
            MESSAGES["order_completed"].format(
                order_id=memo_order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            ) + f"\n💰 Участникам начислено по: {payout_per_participant:.2f} руб."
        )
        await log_action(
            "complete_order",
            user_id,
            order_db_id,
            f"Заказ #{memo_order_id} завершен пользователем @{username}, начислено по {payout_per_participant:.2f} руб."
        )
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в complete_order_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в complete_order_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("cancel_confirmed_order_"))
async def cancel_confirmed_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        memo_order_id = callback.data.split("_")[-1]

        # Проверяем права пользователя на отмену заказа
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, status, squad_id FROM orders WHERE memo_order_id = ?",
                (memo_order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer("Заказ не найден.", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            order_db_id, status, squad_id = order

            if status != 'in_progress':
                await callback.message.answer("Заказ не находится в процессе выполнения.", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Проверяем, участвует ли пользователь в заказе или является ли админом
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts oe JOIN escorts e ON oe.escort_id = e.id WHERE oe.order_id = ? AND e.telegram_id = ?",
                (order_db_id, user_id)
            )
            is_participant = (await cursor.fetchone())[0] > 0

            if not (is_participant or is_admin(user_id)):
                await callback.message.answer("У вас нет прав на отмену этого заказа.", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Отменяем заказ
            await conn.execute(
                "UPDATE orders SET status = 'pending' WHERE id = ?",
                (order_db_id,)
            )

            # Удаляем участников из заказа
            await conn.execute(
                "DELETE FROM order_escorts WHERE order_id = ?",
                (order_db_id,)
            )

            await conn.commit()

        await callback.message.edit_text(f"Заказ #{memo_order_id} отменен и возвращен в статус ожидания.", reply_markup=None)

        # Уведомляем участников об отмене
        if squad_id:
            await notify_squad(squad_id, f"Заказ #{memo_order_id} был отменен и возвращен в статус ожидания.")

        await log_action(
            "cancel_confirmed_order",
            user_id,
            order_db_id,
            f"Заказ #{memo_order_id} отменен из статуса 'в процессе'"
        )
        await callback.answer()

    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в cancel_confirmed_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cancel_confirmed_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.message(F.text == "🚪 Админ-панель")
async def admin_panel(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'admin_panel'
        await message.answer("\n Админ-панель:", reply_markup=get_admin_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_panel для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Сквады")
async def squads_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'squads_submenu'
        await message.answer("🏠 Управление сквадами:", reply_markup=get_squads_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в squads_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👤 Сопровождающие")
async def escorts_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'escorts_submenu'
        await message.answer("👤 Управление сопровождающими:", reply_markup=get_escorts_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в escorts_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📝 Заказы")
async def admin_orders_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'admin_orders_submenu'
        await message.answer("\n Управление заказами:", reply_markup=get_admin_orders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_orders_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "🚫 Баны/ограничения")
async def bans_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'bans_submenu'
        await message.answer("\n Управление банами и ограничениями:", reply_markup=get_bans_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в bans_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💰 Балансы")
async def balances_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'balances_submenu'
        await message.answer("\n Управление балансами:", reply_markup=get_balances_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в balances_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👥 Пользователи")
async def users_submenu_handler(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'users_submenu'
        await message.answer("\n Управление пользователями:", reply_markup=get_users_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в users_submenu_handler для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📊 Прочее")
async def misc_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'misc_submenu'
        await message.answer("\n Прочие функции:", reply_markup=get_misc_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в misc_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👑 Управление лидерами")
async def leaders_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'leaders_submenu'
        await message.answer("👑 Управление лидерами:", reply_markup=get_leaders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в leaders_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📞 Связь")
async def communication_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'communication_submenu'
        await message.answer("📞 Управление связью:", reply_markup=get_communication_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в communication_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())


@dp.message(F.text == "🚪 Выйти из админ-панели")
async def exit_admin_panel(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'main_menu'
        await message.answer("\n Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в exit_admin_panel для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.callback_query(F.data.startswith("approve_payout_"))
async def approve_payout(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("У вас нет доступа к этой функции.")
        return
    try:
        parts = callback.data.split("_")
        target_user_id = int(parts[2])
        payout_amount = float(parts[3])

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username, balance FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await callback.message.edit_text("❌ Пользователь не найден.")
                await callback.answer()
                return

            username, balance = user
            if balance < payout_amount:
                await callback.message.edit_text(f"❌ Недостаточно средств на балансе. Доступно: {balance:.2f} руб.")
                await callback.answer()
                return

            # Списываем деньги с баланса
            await conn.execute(
                "UPDATE escorts SET balance = balance - ? WHERE telegram_id = ?",
                (payout_amount, target_user_id)
            )
            await conn.commit()

        # Уведомляем пользователя
        try:
            await bot.send_message(
                target_user_id,
                f"✅ Ваша выплата на сумму {payout_amount:.2f} руб. одобрена и выплачена!"
            )
        except TelegramAPIError:
            pass

        await callback.message.edit_text(
            f"✅ Выплата на сумму {payout_amount:.2f} руб. одобрена для @{username or 'Unknown'} (ID: {target_user_id})"
        )

        await log_action(
            "approve_payout",
            user_id,
            None,
            f"Одобрена выплата {payout_amount:.2f} руб. для @{username or 'Unknown'}"
        )
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка в approve_payout для {user_id}: {e}")
        await callback.answer("Произошла ошибка")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в approve_payout для {user_id}: {e}")
        await callback.answer("Произошла ошибка")

@dp.callback_query(F.data.startswith("reject_payout_"))
async def reject_payout(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("У вас нет доступа к этой функции.")
        return
    try:
        parts = callback.data.split("_")
        target_user_id = int(parts[2])
        payout_amount = float(parts[3])

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await callback.message.edit_text("❌ Пользователь не найден.")
                await callback.answer()
                return

            username = user[0]

        # Уведомляем пользователя
        try:
            await bot.send_message(
                target_user_id,
                f"❌ Ваша выплата на сумму {payout_amount:.2f} руб. отклонена администратором."
            )
        except TelegramAPIError:
            pass

        await callback.message.edit_text(
            f"❌ Выплата на сумму {payout_amount:.2f} руб. отклонена для @{username or 'Unknown'} (ID: {target_user_id})"
        )

        await log_action(
            "reject_payout",
            user_id,
            None,
            f"Отклонена выплата {payout_amount:.2f} руб. для @{username or 'Unknown'}"
        )
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка в reject_payout для {user_id}: {e}")
        await callback.answer("Произошла ошибка")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в reject_payout для {user_id}: {e}")
        await callback.answer("Произошла ошибка")

@dp.callback_query(F.data.startswith("select_leader_"))
async def select_leader_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        leader_telegram_id = int(callback.data.split("_")[-1])

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.username, s.name
                FROM escorts e
                JOIN squads s ON s.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (leader_telegram_id,)
            )
            leader_info = await cursor.fetchone()

        if not leader_info:
            await callback.message.edit_text("❌ Информация о лидере не найдена.")
            await callback.answer()
            return

        leader_username, squad_name = leader_info

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📩 Связаться с лидером", callback_data=f"contact_leader_{leader_telegram_id}")]
        ])

        await callback.message.edit_text(
            f"👨‍💼 Лидер: @{leader_username or 'Unknown'}\n"
            f"ID: {leader_telegram_id}\n"
            f"Сквад: {squad_name}",
            reply_markup=keyboard
        )
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка в select_leader_callback для {user_id}: {e}")
        await callback.answer("Произошла ошибка")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в select_leader_callback для {user_id}: {e}")
        await callback.answer("Произошла ошибка")

@dp.callback_query(F.data.startswith("contact_leader_"))
async def contact_leader_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        leader_telegram_id = int(callback.data.split("_")[-1])
        await state.update_data(target_leader_id=leader_telegram_id)
        await callback.message.answer("✍️ Введите ваше сообщение для лидера:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.contact_leader_message)
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка в contact_leader_callback для {user_id}: {e}")
        await callback.answer("Произошла ошибка")

@dp.callback_query(F.data.startswith("reply_support_"))
async def reply_support(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.answer("У вас нет доступа к этой функции.")
        return
    try:
        target_user_id = int(callback.data.split("_")[-1])
        await state.update_data(target_user_id=target_user_id)
        await callback.message.answer("\n Введите ответ пользователю:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.support_reply)
        await callback.answer()
    except (ValueError, TelegramAPIError) as e:
        logger.error(f"Ошибка в reply_support для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.answer("Произошла ошибка")

@dp.message(Form.support_reply)
async def process_support_reply(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return

    reply_text = message.text.strip()
    if not reply_text:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        return

    try:
        data = await state.get_data()
        target_user_id = data.get('target_user_id')

        if not target_user_id:
            await message.answer("\n Ошибка: не найден получатель сообщения.", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        # Отправляем ответ пользователю
        try:
            await bot.send_message(
                target_user_id,
                f"\n Ответ от поддержки:\n{reply_text}"
            )
            await message.answer("\n Ответ отправлен пользователю!", reply_markup=get_admin_keyboard())
        except TelegramAPIError:
            await message.answer("\n Не удалось отправить сообщение пользователю.", reply_markup=get_admin_keyboard())

        await log_action("support_reply", user_id, None, f"Ответ пользователю {target_user_id}: {reply_text}")
        await state.clear()

    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_support_reply для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.contact_leader_message)
async def process_contact_leader_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return

    contact_message = message.text.strip()
    if not contact_message:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard())
        return

    try:
        data = await state.get_data()
        target_leader_id = data.get('target_leader_id')

        if not target_leader_id:
            await message.answer("❌ Ошибка: не найден получатель сообщения.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return

        # Получаем информацию об отправителе
        sender_username = message.from_user.username or "Unknown"
        sender_info = f"от пользователя @{sender_username} (ID: {user_id})" if not is_admin(user_id) else "от администратора"

        # Отправляем сообщение лидеру
        try:
            await bot.send_message(
                target_leader_id,
                f"📩 Новое сообщение {sender_info}:\n\n{contact_message}"
            )
            await message.answer("✅ Ваше сообщение отправлено лидеру!", reply_markup=await get_menu_keyboard(user_id))
        except TelegramAPIError:
            await message.answer("❌ Не удалось отправить сообщение лидеру.", reply_markup=await get_menu_keyboard(user_id))

        await log_action("contact_leader", user_id, None, f"Сообщение лидеру {target_leader_id}: {contact_message}")
        await state.clear()

    except Exception as e:
        logger.error(f"Ошибка в process_contact_leader_message для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

# --- Обработчики для управления лидерами ---

@dp.message(F.text == "👑 Добавить лидера команды")
async def add_leader(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("Введите Telegram ID пользователя, которого хотите сделать лидером:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.leader_user_id)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_leader для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.leader_user_id)
async def process_leader_user_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        leader_telegram_id = int(message.text.strip())
        await state.update_data(leader_telegram_id=leader_telegram_id)
        await message.answer("Введите название нового сквада:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.leader_squad_name)
    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))

@dp.message(Form.leader_squad_name)
async def process_leader_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer("❌ Название сквада не может быть пустым.", reply_markup=get_cancel_keyboard(True))
        return

    try:
        data = await state.get_data()
        leader_telegram_id = data.get('leader_telegram_id')

        async with aiosqlite.connect(DB_PATH) as conn:
            # Проверяем, существует ли пользователь
            cursor = await conn.execute("SELECT id FROM escorts WHERE telegram_id = ?", (leader_telegram_id,))
            escort_record = await cursor.fetchone()
            if not escort_record:
                await message.answer(f"❌ Пользователь с Telegram ID {leader_telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            escort_id = escort_record[0]

            # Проверяем, не является ли пользователь уже лидером
            cursor = await conn.execute("SELECT squad_id FROM squad_leaders WHERE leader_id = ?", (escort_id,))
            existing_leader = await cursor.fetchone()
            if existing_leader:
                await message.answer("❌ Этот пользователь уже является лидером.", reply_markup=get_admin_keyboard())
                await state.clear()
                return

            # Создаем новый сквад
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            squad_id = cursor.lastrowid

            # Назначаем пользователя лидером
            await conn.execute("INSERT INTO squad_leaders (leader_id, squad_id) VALUES (?, ?)", (escort_id, squad_id))

            # Обновляем информацию о пользователе (связываем с новым сквадом)
            await conn.execute("UPDATE escorts SET squad_id = ? WHERE id = ?", (squad_id, escort_id))

            await conn.commit()

        await message.answer(f"👑 Пользователь {leader_telegram_id} назначен лидером сквада '{squad_name}'!", reply_markup=get_admin_keyboard())
        await log_action("add_leader", user_id, None, f"Назначен лидер {leader_telegram_id} для сквада '{squad_name}'")
        await state.clear()

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID. Введите числовое значение:", reply_except ValueError:
        await message.answer("❌ Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_leader_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_leader_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📋 Список лидеров")
async def list_leaders(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, s.name as squad_name
                FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                JOIN squads s ON sl.squad_id = s.id
                ORDER BY s.name
                '''
            )
            leaders = await cursor.fetchall()

        if not leaders:
            await message.answer("👑 Список лидеров пуст.", reply_markup=get_leaders_submenu_keyboard())
            return

        response = "👑 Список лидеров:\n\n"
        for telegram_id, username, squad_name in leaders:
            response += f"ID: {telegram_id}\n"
            response += f"@{username or 'Unknown'}\n"
            response += f"Сквад: {squad_name}\n\n"

        await message.answer(response, reply_markup=get_leaders_submenu_keyboard())

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в list_leaders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в list_leaders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())

@dp.message(F.text == "🗑️ Убрать лидера")
async def remove_leader(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.id, e.telegram_id, e.username, s.name as squad_name
                FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                JOIN squads s ON sl.squad_id = s.id
                ORDER BY s.name
                '''
            )
            leaders_info = await cursor.fetchall()

        if not leaders_info:
            await message.answer("👑 Нет назначенных лидеров для удаления.", reply_markup=get_leaders_submenu_keyboard())
            return

        response = "Выберите лидера для удаления:\n\n"
        for escort_id, telegram_id, username, squad_name in leaders_info:
            response += f"ID: {escort_id} | Telegram ID: {telegram_id} | @{username or 'Unknown'} | Сквад: {squad_name}\n"

        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.remove_leader_user_id)
        await state.update_data(leaders_info=leaders_info) # Сохраняем информацию для проверки

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в remove_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в remove_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()

@dp.message(Form.remove_leader_user_id)
async def process_remove_leader_user_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
        return

    try:
        target_telegram_id = int(message.text.strip())

        data = await state.get_data()
        leaders_info = data.get('leaders_info', [])

        escort_id_to_remove = None
        for escort_id, telegram_id, _, _ in leaders_info:
            if telegram_id == target_telegram_id:
                escort_id_to_remove = escort_id
                break

        if escort_id_to_remove is None:
            await message.answer("❌ Пользователь с таким Telegram ID не найден в списке лидеров.", reply_markup=get_cancel_keyboard(True))
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем информацию о лидере и его скваде перед удалением
            cursor = await conn.execute(
                '''
                SELECT e.username, s.name
                FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                JOIN squads s ON sl.squad_id = s.id
                WHERE e.id = ?
                ''', (escort_id_to_remove,)
            )
            leader_info = await cursor.fetchone()
            if not leader_info: # Дополнительная проверка
                await message.answer("❌ Не удалось получить информацию о лидере.", reply_markup=get_leaders_submenu_keyboard())
                await state.clear()
                return
            leader_username, squad_name = leader_info

            # Получаем ID сквада до удаления записи из squad_leaders
            cursor = await conn.execute("SELECT squad_id FROM squad_leaders WHERE leader_id = ?", (escort_id_to_remove,))
            squad_id_result = await cursor.fetchone()
            squad_id_to_delete = None
            if squad_id_result:
                squad_id_to_delete = squad_id_result[0]

            # Удаляем запись из squad_leaders
            await conn.execute("DELETE FROM squad_leaders WHERE leader_id = ?", (escort_id_to_remove,))

            # Удаляем сквад
            if squad_id_to_delete:
                await conn.execute("DELETE FROM squads WHERE id = ?", (squad_id_to_delete,))
                # Также нужно сбросить squad_id у всех участников сквада, если они были
                await conn.execute("UPDATE escorts SET squad_id = NULL WHERE squad_id = ?", (squad_id_to_delete,))

            # Обновляем пользователя, убирая связь со сквадом (если он остался, например, из-за ошибки)
            await conn.execute("UPDATE escorts SET squad_id = NULL WHERE id = ?", (escort_id_to_remove,))

            await conn.commit()

        await message.answer(f"👑 Лидер @{leader_username or 'Unknown'} (ID: {target_telegram_id}) удален, сквад '{squad_name}' расформирован.", reply_markup=get_leaders_submenu_keyboard())
        await log_action("remove_leader", user_id, None, f"Удален лидер {target_telegram_id} (сквад: {squad_name})")
        await state.clear()

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_remove_leader_user_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_remove_leader_user_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "📞 Связаться с лидером")
async def contact_leader(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("Введите Telegram ID пользователя, чтобы найти его лидера:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.leader_user_id) # Переиспользуем состояние для ввода ID
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()

@dp.message(Form.leader_user_id)
async def process_contact_leader(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
        return

    try:
        target_telegram_id = int(message.text.strip())

        async with aiosqlite.connect(DB_PATH) as conn:
            # Находим escort_id пользователя
            cursor = await conn.execute("SELECT id FROM escorts WHERE telegram_id = ?", (target_telegram_id,))
            escort_record = await cursor.fetchone()
            if not escort_record:
                await message.answer(f"❌ Пользователь с Telegram ID {target_telegram_id} не найден.", reply_markup=get_leaders_submenu_keyboard())
                await state.clear()
                return
            escort_id = escort_record[0]

            # Находим лидера этого пользователя
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, s.name
                FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts user_escort ON user_escort.squad_id = s.id
                WHERE user_escort.id = ?
                '''
                , (escort_id,)
            )
            leader_info = await cursor.fetchone()

            if not leader_info:
                await message.answer(f"❌ У пользователя {target_telegram_id} нет назначенного лидера.", reply_markup=get_leaders_submenu_keyboard())
                await state.clear()
                return

            leader_telegram_id, leader_username, squad_name = leader_info

        await message.answer(f"Лидер сквада '{squad_name}' - @{leader_username or 'Unknown'} (Telegram ID: {leader_telegram_id}).\n"
                           f"Вы можете связаться с ним напрямую.", reply_markup=get_leaders_submenu_keyboard())
        await log_action("contact_leader", user_id, None, f"Просмотр лидера пользователя {target_telegram_id}")
        await state.clear()

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()

# --- Запуск бота ---
async def main():
    try:
        await init_db()
        scheduler.add_job(check_pending_orders, 'interval', hours=12)
        scheduler.start()
        logger.info("Бот запущен")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}\n{traceback.format_exc()}")
        raise

if __name__ == "__main__":
    asyncio.run(main())