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
    add_user_info = State()
    add_user_info_text = State()
    view_user_info = State()
    delete_user_info = State()
    delete_order = State()
    admin_rate_order = State()
    add_reputation = State()
    remove_reputation = State()

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
                    rating_count INTEGER DEFAULT 0
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
                    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
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
                CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id);
                CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id);
                CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id);
                CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id);
                CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id);
                CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date);
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
            # Получаем текущие значения рейтинга
            cursor = await conn.execute(
                "SELECT rating, rating_count FROM escorts WHERE id = ?",
                (escort_id,)
            )
            row = await cursor.fetchone()
            if row:
                current_rating, rating_count = row
                new_rating_count = rating_count + 1
                new_rating = (current_rating * rating_count + rating) / new_rating_count

                await conn.execute(
                    '''
                    UPDATE escorts
                    SET reputation = reputation + ?,
                        rating = ?,
                        rating_count = ?
                    WHERE id = ?
                    ''',
                    (rating, new_rating, new_rating_count, escort_id)
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

# --- Проверка админских прав ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# --- Клавиатуры ---
def get_menu_keyboard(user_id: int):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Заказы")],
            [KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="🔢 Ввести PUBG ID")],
            [KeyboardButton(text="ℹ️ Информация"), KeyboardButton(text="📩 Поддержка")],
            [KeyboardButton(text="📥 Получить выплату")],
            [KeyboardButton(text="🔐 Админ-панель")] if is_admin(user_id) else [],
        ],
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
            [KeyboardButton(text="⭐ Репутация"), KeyboardButton(text="📊 Прочее")],
            [KeyboardButton(text="🚪 Выйти из админ-панели")],
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
        await message.answer(f"{MESSAGES['welcome']}\n\n Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} (@{username}) запустил бота")
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cmd_start для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    try:
        await message.answer(MESSAGES["ping"], reply_markup=get_menu_keyboard(message.from_user.id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cmd_ping для {message.from_user.id}: \n{e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "✅ Принять условия")
async def accept_rules(message: types.Message):
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("UPDATE escorts SET rules_accepted = 1 WHERE telegram_id = ?", (user_id,))
            await conn.commit()
        user_context[user_id] = 'main_menu'
        await message.answer(f"✅ Условия приняты! Добро пожаловать!\n\n📌 Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} принял условия")
        await log_action("accept_rules", user_id, None, "Пользователь принял условия")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в accept_rules для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в accept_rules для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "🔢 Ввести PUBG ID")
async def enter_pubg_id(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    try:
        await message.answer("🔢 Введите ваш PUBG ID:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.pubg_id)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в enter_pubg_id для \n{message.from_user.id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))
        await state.clear()

@dp.message(Form.pubg_id)
async def process_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
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
        await message.answer("🔢 PUBG ID успешно обновлен!", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} обновил PUBG ID: {pubg_id}")
        await log_action("update_pubg_id", user_id, None, f"Обновлен PUBG ID: {pubg_id}")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_pubg_id для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_pubg_id для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
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
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.callback_query(F.data == "about_project")
async def about_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        response = (
            "ℹ️ О проекте:\n"
            "Этот бот предназначен для распределения заказов по сопровождению в Metro Royale. "
            "Все действия фиксируются, выплаты прозрачны."
        )
        await callback.message.answer(response, reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в about_project для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text.in_(["📜 Политика конфиденциальности", "📖 Правила", "📜 Публичная оферта"]))
async def rules_links(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        if message.text == "📜 Политика конфиденциальности":
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)]
            ])
            await message.answer("📜 Политика конфиденциальности:", reply_markup=keyboard)
        elif message.text == "📖 Правила":
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)]
            ])
            await message.answer("📖 Правила:", reply_markup=keyboard)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Публичная оферта", url=OFFER_URL)]
            ])
            await message.answer("📜 Публичная оферта:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в rules_links: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "👤 Мой профиль")
async def my_profile(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            return
        escort_id, squad_id, pubg_id, balance, reputation, completed_orders, username, rating, rating_count, _, ban_until, restrict_until, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad = await cursor.fetchone()
        avg_rating = rating / rating_count if rating_count > 0 else 0
        response = (
            f"\n Ваш профиль:\n"
            f"\n Username: @{username or 'Unknown'}\n"
            f"\n PUBG ID: {pubg_id or 'не указан'}\n"
            f"\n Сквад: {squad[0] if squad else 'не назначен'}\n"
            f"\n Баланс: {balance:.2f} руб.\n"
            f"\n Репутация: {reputation}\n"
            f"\n Выполнено заказов: {completed_orders}\n"
            f"\n Рейтинг: {avg_rating:.2f} ({rating_count} оценок)\n"
        )
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в my_profile для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в my_profile для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

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
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Доступные заказы")
async def available_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, memo_order_id, customer_info, amount FROM orders WHERE status = 'pending'"
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer(MESSAGES["no_orders"], reply_markup=get_menu_keyboard(user_id))
            return
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"#{order_id} - {customer}, {amount:.2f} руб.", callback_data=f"select_order_{db_id}")]
            for db_id, order_id, customer, amount in orders
        ])
        await message.answer("\n Доступные заказы:", reply_markup=keyboard)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в available_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в available_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Мои заказы")
async def my_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
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
            await message.answer(MESSAGES["no_active_orders"], reply_markup=get_menu_keyboard(user_id))
            return
        response = "\n Ваши заказы:\n"
        for order_id, customer, amount, status in orders:
            status_text = "Ожидает" if status == "pending" else "В процессе" if status == "in_progress" else "Завершен"
            response += f"#{order_id} - {customer}, {amount:.2f} руб., Статус: {status_text}\n"
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в my_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в my_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "✅ Завершить заказ")
async def complete_order(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            # Показываем заказы в процессе, где пользователь является участником
            cursor = await conn.execute(
                '''
                SELECT DISTINCT o.memo_order_id, o.id, o.squad_id, o.amount
                FROM orders o
                WHERE o.status = 'in_progress' AND o.id IN (
                    SELECT order_id FROM order_escorts WHERE escort_id = ?
                )
                ''', (escort_id,)
            )
            orders = await cursor.fetchall()
        if not orders:
            await message.answer("\n У вас нет активных заказов для завершения.", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return
        response = "\n Выберите заказ для завершения:\n"
        for order_id, _, _, amount in orders:
            response += f"#{order_id} - {amount:.2f} руб.\n"
        await message.answer(response, reply_markup=get_cancel_keyboard())
        await state.set_state(Form.complete_order)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в complete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в complete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Form.complete_order)
async def process_complete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    order_id = message.text.strip()
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
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
                await message.answer(f"\n Заказ #{order_id} не найден или не в процессе.", reply_markup=get_menu_keyboard(user_id))
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
                await message.answer(f"\n Недостаточно сопровождающих для завершения заказа (требуется минимум 2, есть {participant_count}).", reply_markup=get_menu_keyboard(user_id))
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
            ) + f"\n💰 Вам начислено: {payout_per_participant:.2f} руб.",
            reply_markup=get_menu_keyboard(user_id)
        )
        await notify_admins(
            MESSAGES["order_completed"].format(
                order_id=order_id,
                username=username or "Unknown",
                telegram_id=user_id,
                pubg_id=pubg_id or "не указан"
            ) + f"\n💰 Участникам начислено по: {payout_per_participant:.2f} руб."
        )
        await log_action("complete_order", user_id, order_db_id, f"Заказ #{order_id} завершен пользователем @{username}, начислено {payout_per_participant:.2f} руб.")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_complete_order для {user_id}: \n{e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_complete_order для {user_id}: \n{e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(F.text == "⭐ Оценить заказ")
async def admin_rate_orders(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
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
            await message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id, _, _, balance, _, _, _, _, _, _, _, _, _ = escort

        if balance <= 0:
            await message.answer("❗ У вас нет средств для вывода", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return

        await message.answer(f"\n Введите сумму для выплаты (доступно: {balance:.2f} руб.):", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.payout_request)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в request_payout для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в request_payout для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Form.payout_request)
async def process_payout_request(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    try:
        payout_amount = float(message.text.strip())
        if payout_amount <= 0:
            await message.answer("\n Сумма должна быть больше 0", reply_markup=get_cancel_keyboard())
            return

        escort = await get_escort(user_id)
        if not escort:
            await message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
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
            reply_markup=get_menu_keyboard(user_id)
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
        await message.answer("\n Неверный формат суммы. Введите число:", reply_markup=get_cancel_keyboard())
        return
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_payout_request для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_payout_request для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
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
                await callback.message.answer("\n Заказ не найден.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
        await callback.message.edit_text(f"\n Заказ #{order[0]}. Нажмите 'Готово' или 'Отмена'.", reply_markup=get_order_keyboard(order_db_id))
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в select_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в select_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("join_order_"))
async def join_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort
        if not pubg_id:
            await callback.message.answer("\n Укажите PUBG ID!", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        if not squad_id:
            await callback.message.answer(MESSAGES["not_in_squad"], reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT status, memo_order_id FROM orders WHERE id = ?", (order_db_id,)
            )
            order = await cursor.fetchone()
            if not order or order[0] != 'pending':
                await callback.message.answer(MESSAGES["order_already_in_progress"].format(order_id=order[1]), reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            # New: Check if existing applications exist and squad matches
            cursor = await conn.execute(
                "SELECT squad_id FROM order_applications WHERE order_id = ? LIMIT 1", (order_db_id,)
            )
            existing_squad = await cursor.fetchone()
            if existing_squad and existing_squad[0] != squad_id:
                await callback.message.answer("⚠️ Этот заказ уже набирается другим сквадом!", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                await callback.message.answer("\n Вы уже присоединились!", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ?", (order_db_id,)
            )
            participant_count = (await cursor.fetchone())[0]
            # Убираем ограничение на максимальное количество участников
            await conn.execute(
                "INSERT INTO order_applications (order_id, escort_id, squad_id, pubg_id) VALUES (?, ?, ?, ?)",
                (order_db_id, escort_id, squad_id, pubg_id)
            )
            await conn.commit()
        applications = await get_order_applications(order_db_id)
        participants = "\n".join(
            f"@{username or 'Unknown'} (PUBG ID: {pubg_id}, Squad: {squad_name or 'No squad'})"
            for _, username, pubg_id, _, squad_name in applications
        )
        memo_order_id = order[1]
        response = f"\n Заказ #{memo_order_id} в наборе:\n"
        response += f"Участники: {participants if participants else 'Никто не участвует'}\n"
        response += f"Участников: {len(applications)} (минимум 2 для старта)"

        # Кнопка "Начать выполнение" всегда показывается
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Начать выполнение", callback_data=f"start_order_{order_db_id}")],
            [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
        ])
        await callback.message.edit_text(response, reply_markup=keyboard)
        await callback.message.answer(
            MESSAGES["order_joined"].format(order_id=memo_order_id, participants=participants),
            reply_markup=get_menu_keyboard(user_id)
        )
        await log_action("join_order", user_id, order_db_id, f"Пользователь {user_id} присоединился к заказу #{memo_order_id}")
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в join_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в join_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("start_order_"))
async def start_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort or not escort[1]:
            await callback.message.answer(MESSAGES["not_in_squad"], reply_markup=get_menu_keyboard(user_id))
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
                await callback.message.answer(MESSAGES["order_already_in_progress"].format(order_id=order[0]), reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            cursor = await conn.execute(
                "SELECT escort_id, squad_id FROM order_applications WHERE order_id = ?",
                (order_db_id,)
            )
            applications = await cursor.fetchall()
            if len(applications) < 2:  # Требуется минимум 2 участника
                # Проверяем, нужно ли обновлять сообщение
                current_text = callback.message.text or ""
                new_text = f"⚠️ Недостаточно участников для начала выполнения заказа!\nТребуется: минимум 2 участника\nСейчас: {len(applications)} участников"

                # Обновляем сообщение только если текст изменился
                if new_text not in current_text:
                    try:
                        await callback.message.edit_text(
                            new_text,
                            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                [InlineKeyboardButton(text="Начать выполнение", callback_data=f"start_order_{order_db_id}")],
                                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
                            ])
                        )
                    except TelegramAPIError as e:
                        # Игнорируем ошибку "message is not modified"
                        if "message is not modified" not in str(e):
                            logger.error(f"Ошибка при обновлении сообщения: {e}")

                await callback.answer("⚠️ Недостаточно участников для начала выполнения заказа!")
                return
            winning_squad_id = applications[0][1]
            valid_applications = [app for app in applications if app[1] == squad_id] # Фильтруем по скваду пользователя, если он есть
            if len(valid_applications) < 2:
                async with aiosqlite.connect(DB_PATH) as conn:
                    cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
                    squad_result = await cursor.fetchone()
                    squad_name = squad_result[0] if squad_result else "Unknown"
                await callback.message.answer(
                    f"⚠️ В скваде '{squad_name}' недостаточно участников (минимум 2)!",
                    reply_markup=get_menu_keyboard(user_id)
                )
                await callback.answer()
                return
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
            squad_result = await cursor.fetchone()
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
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в start_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("complete_order_"))
async def complete_order_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    memo_order_id = callback.data.split('_')[-1]
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("\n Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
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
                await callback.message.answer(f"\n Заказ #{memo_order_id} не найден или не в процессе.", reply_markup=get_menu_keyboard(user_id))
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
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в complete_order_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT status, memo_order_id FROM orders WHERE id = ?",
                (order_db_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer("\n Заказ не найден.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            if order[0] != 'pending':
                await callback.message.answer(MESSAGES["order_already_in_progress"].format(order_id=order[1]), reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            await conn.execute(
                "DELETE FROM order_applications WHERE order_id = ?",
                (order_db_id,)
            )
            await conn.commit()
        await callback.message.edit_text("Вы не приняли или отменили этот заказ", reply_markup=None)
        await callback.message.answer("\n Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        await log_action(
            "cancel_order",
            user_id,
            order_db_id,
            f"Заказ #{order[1]} отменен пользователем @{callback.from_user.username or 'Unknown'}"
        )
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в cancel_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cancel_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("cancel_confirmed_order_"))
async def cancel_confirmed_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        memo_order_id = callback.data.split("_")[-1]

        # Проверяем права пользователя на отмену заказа
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, status, squad_id FROM orders WHERE memo_order_id = ?",
                (memo_order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer("Заказ не найден.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            order_db_id, status, squad_id = order

            if status != 'in_progress':
                await callback.message.answer("Заказ не находится в процессе выполнения.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Проверяем, участвует ли пользователь в заказе или является ли админом
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts oe JOIN escorts e ON oe.escort_id = e.id WHERE oe.order_id = ? AND e.telegram_id = ?",
                (order_db_id, user_id)
            )
            is_participant = (await cursor.fetchone())[0] > 0

            if not (is_participant or is_admin(user_id)):
                await callback.message.answer("У вас нет прав на отмену этого заказа.", reply_markup=get_menu_keyboard(user_id))
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
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cancel_confirmed_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.message(F.text == "🔐 Админ-панель")
async def admin_panel(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'admin_panel'
        await message.answer("\n Админ-панель:", reply_markup=get_admin_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_panel для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "🏠 Добавить сквад")
async def add_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("\n Введите название сквада:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.squad_name)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_squad для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.squad_name)
async def process_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        await message.answer(f"\n Сквад '{squad_name}' успешно создан!", reply_markup=get_admin_keyboard())
        await log_action("add_squad", user_id, None, f"Создан сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_squad_name для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_squad_name для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🗑️ Расформировать сквад")
async def delete_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, name FROM squads")
            squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_admin_keyboard())
            await state.clear()
            return
        response = "\n Введите название сквада для расформирования:\n"
        for _, name in squads:
            response += f"- {name}\n"
        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.delete_squad)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в delete_squad для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в delete_squad для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.delete_squad)
async def process_delete_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    squad_name = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f"\n Сквад '{squad_name}' не найден.", reply_markup=get_cancel_keyboard(True))
                return
            squad_id = squad[0]
            await conn.execute("DELETE FROM squads WHERE id = ?", (squad_id,))
            await conn.execute("UPDATE escorts SET squad_id = NULL WHERE squad_id = ?", (squad_id,))
            await conn.commit()
        await message.answer(MESSAGES["squad_deleted"].format(squad_name=squad_name), reply_markup=get_admin_keyboard())
        await log_action("delete_squad", user_id, None, f"Сквад '{squad_name}' расформирован")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_delete_squad для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_delete_squad для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "👤 Добавить сопровождающего")
async def add_escort_admin(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID, username (через @), PUBG ID и название сквада через запятую\n"
            "Пример: 123456789, @username, 987654321, SquadName",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.escort_info)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_escort_admin для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.escort_info)
async def process_escort_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        data = [x.strip() for x in message.text.split(",")]
        if len(data) != 4:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        telegram_id, username, pubg_id, squad_name = data
        telegram_id = int(telegram_id)
        username = username.lstrip("@")
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f"\n Сквад '{squad_name}' не найден.", reply_markup=get_cancel_keyboard(True))
                return
            squad_id = squad[0]
            cursor = await conn.execute("SELECT id FROM escorts WHERE telegram_id = ?", (telegram_id,))
            existing_escort = await cursor.fetchone()
            if existing_escort:
                await conn.execute(
                    "UPDATE escorts SET username = ?, pubg_id = ?, squad_id = ?, rules_accepted = 1 WHERE telegram_id = ?",
                    (username, pubg_id, squad_id, telegram_id)
                )
            else:
                await conn.execute(
                    "INSERT INTO escorts (telegram_id, username, pubg_id, squad_id, rules_accepted) VALUES (?, ?, ?, ?, 1)",
                    (telegram_id, username, pubg_id, squad_id)
                )
            await conn.commit()
        await message.answer(
            f"\n Сопровождающий @{username} добавлен в сквад '{squad_name}'!", reply_markup=get_admin_keyboard()
        )
        await log_action(
            "add_escort", user_id, None, f"Добавлен сопровождающий @{username} в сквад '{squad_name}'"
        )
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования данных в process_escort_info для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_escort_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_escort_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🗑️ Удалить сопровождающего")
async def remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            await state.clear()
            return
        response = "\n Введите Telegram ID сопровождающего для удаления:\n"
        for telegram_id, username in escorts:
            response += f"@{username or 'Unknown'} - {telegram_id}\n"
        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.remove_escort)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в remove_escort для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в remove_escort для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.remove_escort)
async def process_remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        escort_telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (escort_telegram_id,))
            escort = await cursor.fetchone()
            if not escort:
                await message.answer(f"\n Сопровождающий с ID {escort_telegram_id} не найден.", reply_markup=get_cancel_keyboard(True))
                return
            username = escort[0]
            await conn.execute("DELETE FROM escorts WHERE telegram_id = ?", (escort_telegram_id,))
            await conn.commit()
        await message.answer(f"\n Сопровождающий @{username or 'Unknown'} удален!", reply_markup=get_admin_keyboard())
        await log_action("remove_escort", user_id, None, f"Удален сопровождающий @{username or 'Unknown'}")
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в process_remove_escort для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_remove_escort для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_remove_escort для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📋 Список сквадов")
async def list_squads(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, name FROM squads")
            squads = await cursor.fetchall()
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_admin_keyboard())
            return
        response = "\n Список сквадов:\n"
        for squad_id, squad_name in squads:
            squad_info = await get_squad_info(squad_id)
            if squad_info:
                name, member_count, total_orders, total_balance, rating, rating_count = squad_info
                avg_rating = rating / rating_count if rating_count > 0 else 0
                response += (
                    f"\n {name:25}\n"
                    f"\n Участников:  {member_count:10}\n"
                    f"\n Заказов:     {total_orders:10}\n"
                    f"\n Баланс:      {total_balance:10.2f} руб.\n"
                    f"\n Рейтинг:     {avg_rating:6.2f} ({rating_count} оценок)\n\n\n"
                )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в list_squads для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в list_squads для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💰 Балансы сопровождающих")
async def list_escort_balances(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT telegram_id, username, balance FROM escorts WHERE balance > 0 ORDER BY balance DESC"
            )
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer("\n Нет сопровождающих с положительным балансом.", reply_markup=get_admin_keyboard())
            return
        response = "\n Балансы сопровождающих:\n"
        for telegram_id, username, balance in escorts:
            response += f"@{username or 'Unknown':20} (ID: {telegram_id:12}): {balance:10.2f} руб.\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в list_escort_balances для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в list_escort_balances для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💸 Начислить")
async def add_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID и сумму для начисления через запятую\nПример: 123456789, 500.00",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.balance_amount)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_balance для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.balance_amount)
async def process_balance_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        telegram_id, amount = [x.strip() for x in message.text.split(",")]
        telegram_id = int(telegram_id)
        amount = float(amount)
        if amount <= 0:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            escort = await cursor.fetchone()
            if not escort:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_cancel_keyboard(True))
                return
            username = escort[0]
            await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?",
                (amount, telegram_id)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_added"].format(amount=amount, user_id=telegram_id),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "add_balance",
            user_id,
            None,
            f"Начислено {amount:.2f} руб. пользователю @{username or 'Unknown'}"
        )
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования данных в process_balance_amount для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_balance_amount для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_balance_amount для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📝 Добавить заказ")
async def add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите ID заказа, описание клиента и сумму через запятую\nПример: ORDER123, Клиент Иванов, 1000.00",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.add_order)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.add_order)
async def process_add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        parts = [x.strip() for x in message.text.split(',', 2)]
        if len(parts) < 3:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return

        order_id = parts[0]
        customer = parts[1]
        try:
            amount = float(parts[2])
        except ValueError:
            await message.answer("\n Неверный формат суммы", reply_markup=get_cancel_keyboard(True))
            return

        if amount <= 0 or not order_id or not customer:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                '''
                INSERT INTO orders (memo_order_id, customer_info, amount)
                VALUES (?, ?, ?)
                ''', (order_id, customer, amount)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["order_added"].format(order_id=order_id, amount=amount, description=customer, customer=customer),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "add_order",
            user_id,
            None,
            f"Добавлен заказ #{order_id}, клиент: {customer}, сумма: {amount:.2f} руб."
        )
        await notify_squad(None, f"\n Новый заказ #{order_id} добавлен!\nКлиент: {customer}\nСумма: {amount:.2f} руб.")
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования суммы в process_add_order для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_add_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_add_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🚫 Бан навсегда")
async def ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID пользователя для перманентного бана:",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.ban_permanent)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в ban_permanent для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.ban_permanent)
async def process_ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        ban_user_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (ban_user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {ban_user_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 1, ban_until = NULL WHERE telegram_id = ?",
                (ban_user_id,)
            )
            await conn.commit()
        await message.answer(f"\n Пользователь @{username or 'Unknown'} заблокирован навсегда!", reply_markup=get_admin_keyboard())
        await log_action("ban_permanent", user_id, None, f"Пользователь @{username or 'Unknown'} заблокирован навсегда")
        try:
            await bot.send_message(ban_user_id, MESSAGES["user_banned"])
        except TelegramAPIError:
            pass
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в process_ban_permanent для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_ban_permanent для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_ban_permanent для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "⏰ Бан на время")
async def ban_temporary(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID и количество дней бана через запятую\nПример: 123456789, 7",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.ban_duration)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в ban_temporary для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.ban_duration)
async def process_ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        telegram_id, days = [x.strip() for x in message.text.split(",")]
        telegram_id = int(telegram_id)
        days = int(days)
        if days <= 0:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            await state.clear()
            return
        ban_until = datetime.now() + timedelta(days=days)
        formatted_date = ban_until.strftime("%d.%m.%Y %H:%M")
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 1, ban_until = ? WHERE telegram_id = ?",
                (ban_until.isoformat(), telegram_id)
            )
            await conn.commit()
        await message.answer(
            f"\n Пользователь @{username or 'Unknown'} заблокирован до {formatted_date}!",
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "ban_temporary",
            user_id,
            None,
            f"Пользователь @{username or 'Unknown'} заблокирован до {formatted_date}"
        )
        try:
            await bot.send_message(telegram_id, "\n Вы заблокированы до {formatted_date}.")
        except TelegramAPIError:
            pass
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования данных в process_ban_duration для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_ban_duration для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_ban_duration для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "⛔ Ограничить")
async def restrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID и количество дней ограничения через запятую\nПример: 123456789, 7",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.restrict_duration)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в restrict_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.restrict_duration)
async def process_restrict_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        telegram_id, days = [x.strip() for x in message.text.split(",")]
        telegram_id = int(telegram_id)
        days = int(days)
        if days <= 0:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            await state.clear()
            return
        restrict_until = datetime.now() + timedelta(days=days)
        formatted_date = restrict_until.strftime("%d.%m.%Y %H:%M")
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET restrict_until = ? WHERE telegram_id = ?",
                (restrict_until.isoformat(), telegram_id)
            )
            await conn.commit()
        await message.answer(
            f"\n Пользователь @{username or 'Unknown'} ограничен до {formatted_date}!",
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "restrict_user",
            user_id,
            None,
            f"Пользователь @{username or 'Unknown'} ограничен до {formatted_date}"
        )
        try:
            await bot.send_message(telegram_id, MESSAGES["user_restricted"].format(date=formatted_date))
        except TelegramAPIError:
            pass
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования данных в process_restrict_duration для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_restrict_duration для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_restrict_duration для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🔓 Снять бан")
async def unban_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID пользователя для снятия бана:",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.unban_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в unban_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.unban_user)
async def process_unban_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET is_banned = 0, ban_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["user_unbanned"].format(username=username or 'Unknown'),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "unban_user",
            user_id,
            None,
            f"Снят бан с пользователя @{username or 'Unknown'}"
        )
        try:
            await bot.send_message(telegram_id, "\n Ваш бан снят!")
        except TelegramAPIError:
            pass
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в process_unban_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_unban_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_unban_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🔓 Снять ограничение")
async def unrestrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID пользователя для снятия ограничения:",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.unrestrict_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в unrestrict_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.unrestrict_user)
async def process_unrestrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET restrict_until = NULL WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["user_unrestricted"].format(username=username or 'Unknown'),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "unrestrict_user",
            user_id,
            None,
            f"Снято ограничение с пользователя @{username or 'Unknown'}"
        )
        try:
            await bot.send_message(telegram_id, "\n Ваше ограничение снято!")
        except TelegramAPIError:
            pass
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в process_unrestrict_user для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_unrestrict_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_unrestrict_user для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "💰 Обнулить баланс")
async def zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID пользователя для обнуления баланса:",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.zero_balance)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в zero_balance для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.zero_balance)
async def process_zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET balance = 0 WHERE telegram_id = ?",
                (telegram_id,)
            )
            await conn.commit()
        await message.answer(
            MESSAGES["balance_zeroed"].format(user_id=telegram_id),
            reply_markup=get_admin_keyboard()
        )
        await log_action(
            "zero_balance",
            user_id,
            None,
            f"Баланс пользователя @{username or 'Unknown'} обнулен"
        )
        await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в process_zero_balance для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_zero_balance для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_zero_balance для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📊 Все балансы")
async def all_balances(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT telegram_id, username, balance FROM escorts ORDER BY balance DESC"
            )
            escorts = await cursor.fetchall()
        if not escorts:
            await message.answer("\n Нет зарегистрированных сопровождающих.", reply_markup=get_admin_keyboard())
            return
        response = "\n Все балансы:\n"
        for telegram_id, username, balance in escorts:
            response += f"@{username or 'Unknown':20} (ID: {telegram_id:12}): {balance:10.2f} руб.\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в all_balances для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в all_balances для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📜 Журнал действий")
async def action_log(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT action_type, user_id, order_id, description, action_date
                FROM action_log
                ORDER BY action_date DESC
                '''
            )
            logs = await cursor.fetchall()

        if not logs:
            await message.answer("\n Журнал действий пуст.", reply_markup=get_misc_submenu_keyboard())
            return

        # Создаем CSV файл с журналом действий
        filename = f"action_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Action Type', 'User ID', 'Order ID', 'Description', 'Action Date'])
            for log in logs:
                writer.writerow(log)

        with open(filename, 'rb') as f:
            await message.answer_document(types.FSInputFile(filename, filename), reply_markup=get_misc_submenu_keyboard())

        os.remove(filename)
        await log_action("action_log", user_id, None, "Экспортирован журнал действий")
    except (aiosqlite.Error, OSError) as e:
        logger.error(f"Ошибка в action_log для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_misc_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в action_log для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_misc_submenu_keyboard())

@dp.message(F.text == "📤 Экспорт данных")
async def export_data(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT COUNT(*) FROM orders")
            order_count = (await cursor.fetchone())[0]

        if order_count == 0:
            await message.answer(MESSAGES["no_data_to_export"], reply_markup=get_misc_submenu_keyboard())
            await log_action("export_data", user_id, None, "Попытка экспорта: нет данных")
            return

        filename = await export_orders_to_csv()
        if not filename:
            await message.answer(MESSAGES["no_data_to_export"], reply_markup=get_misc_submenu_keyboard())
            await log_action("export_data", user_id, None, "Попытка экспорта: не удалось создать файл")
            return

        with open(filename, 'rb') as f:
            await message.answer_document(types.FSInputFile(filename, filename), reply_markup=get_misc_submenu_keyboard())

        os.remove(filename)
        await log_action("export_data", user_id, None, f"Экспортированы данные в {filename}")
    except (OSError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в export_data для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_misc_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в export_data для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_misc_submenu_keyboard())

@dp.message(F.text == "📊 Отчет за месяц")
async def monthly_report(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        start_date = (datetime.now() - timedelta(days=30)).isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT
                    o.memo_order_id, o.customer_info, o.amount, o.commission_amount,
                    o.status, o.created_at, o.completed_at, s.name as squad_name
                FROM orders o
                LEFT JOIN squads s ON o.squad_id = s.id
                WHERE o.created_at >= ?
                ORDER BY o.created_at DESC
                ''', (start_date,)
            )
            orders = await cursor.fetchall()

            cursor = await conn.execute(
                '''
                SELECT
                    COUNT(*) as total_orders,
                    COALESCE(SUM(amount), 0) as total_amount,
                    COALESCE(SUM(commission_amount), 0) as total_commission
                FROM orders
                WHERE created_at >= ?
                ''', (start_date,)
            )
            report = await cursor.fetchone()

        if not report or report[0] == 0:
            await message.answer("\n Нет заказов за последние 30 дней.", reply_markup=get_misc_submenu_keyboard())
            return

        total_orders, total_amount, total_commission = report

        # Создаем CSV файл с отчетом
        filename = f"monthly_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Order ID', 'Customer', 'Amount', 'Commission', 'Status', 'Created At', 'Completed At', 'Squad'])
            for order in orders:
                writer.writerow(order)

        with open(filename, 'rb') as f:
            await message.answer_document(types.FSInputFile(filename, filename), reply_markup=get_misc_submenu_keyboard())

        os.remove(filename)
        await log_action("monthly_report", user_id, None, "Сгенерирован отчет за месяц")
    except (aiosqlite.Error, OSError) as e:
        logger.error(f"Ошибка в monthly_report для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_misc_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в monthly_report для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_misc_submenu_keyboard())

@dp.message(F.text == "📈 Доход пользователя")
async def user_profit(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("\n Введите Telegram ID пользователя для отчета о доходе:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.profit_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в user_profit для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(Form.profit_user)
async def process_user_profit(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
        return

    try:
        telegram_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.username, COALESCE(SUM(p.amount), 0) as total_payout
                FROM escorts e
                LEFT JOIN payouts p ON p.escort_id = e.id
                WHERE e.telegram_id = ?
                GROUP BY e.id
                ''', (telegram_id,)
            )
            result = await cursor.fetchone()
            if not result:
                await message.answer(f"\n Пользователь с ID {telegram_id} не найден.", reply_markup=get_users_submenu_keyboard())
                await state.clear()
                return
            username, total_payout = result
            await message.answer(
                f"\n Доход пользователя @{username or 'Unknown'} (ID: {telegram_id}):\n"
                f"\n Всего выплачено: {total_payout:.2f} руб.",
                reply_markup=get_users_submenu_keyboard()
            )
            await log_action(
                "user_profit",
                user_id,
                None,
                f"Просмотрен доход пользователя @{username or 'Unknown'}"
            )
            await state.clear()
    except ValueError as e:
        logger.error(f"Ошибка преобразования ID в process_user_profit для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_user_profit для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_user_profit для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "ℹ️ Информация о пользователе")
async def view_user_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("\n Введите Telegram ID пользователя для просмотра информации:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.view_user_info)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в view_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(Form.view_user_info)
async def process_view_user_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
        return

    try:
        target_user_id = int(message.text.strip())
        async with aiosqlite.connect(DB_PATH) as conn:
            # Проверяем существование пользователя
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {target_user_id} не найден.", reply_markup=get_users_submenu_keyboard())
                await state.clear()
                return

            username = user[0]

            # Получаем дополнительную информацию
            cursor = await conn.execute(
                '''
                SELECT info_text, added_at, added_by
                FROM user_additional_info
                WHERE telegram_id = ?
                ORDER BY added_at DESC
                ''', (target_user_id,)
            )
            info_records = await cursor.fetchall()

        if not info_records:
            await message.answer(f"\n Для пользователя @{username or 'Unknown'} (ID: {target_user_id}) нет дополнительной информации.", reply_markup=get_users_submenu_keyboard())
        else:
            response = f"\n Информация о пользователе @{username or 'Unknown'} (ID: {target_user_id}):\n\n"
            for i, (info_text, added_at, added_by) in enumerate(info_records, 1):
                try:
                    formatted_date = datetime.fromisoformat(added_at).strftime("%d.%m.%Y %H:%M")
                except:
                    formatted_date = added_at
                response += f"\n Запись {i}:\n{info_text}\n Добавлено: {formatted_date} (админ ID: {added_by})\n\n"
            await message.answer(response, reply_markup=get_users_submenu_keyboard())

        await log_action("view_user_info", user_id, None, f"Просмотрена информация о пользователе {target_user_id}")
        await state.clear()

    except ValueError:
        await message.answer("\n Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
        return
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_view_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_view_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "➕ Добавить информацию")
async def add_user_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("\n Введите Telegram ID пользователя:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.add_user_info)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(Form.add_user_info)
async def process_add_user_info_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
        return

    try:
        target_user_id = int(message.text.strip())
        await state.update_data(target_user_id=target_user_id)
        await message.answer("\n Введите информацию о пользователе:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.add_user_info_text)
    except ValueError:
        await message.answer("\n Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
        return

@dp.message(Form.add_user_info_text)
async def process_add_user_info_text(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
        return

    user_info = message.text.strip()
    if not user_info:
        await message.answer("\n Информация не может быть пустой. Введите информацию о пользователе:", reply_markup=get_cancel_keyboard(True))
        return

    try:
        data = await state.get_data()
        target_user_id = data.get('target_user_id')

        # Проверим, существует ли пользователь
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {target_user_id} не найден в базе данных.", reply_markup=get_users_submenu_keyboard())
                await state.clear()
                return

            # Создаем таблицу для дополнительной информации, если её нет
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_additional_info (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER,
                    info_text TEXT,
                    added_by INTEGER,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (telegram_id) REFERENCES escorts (telegram_id)
                )
            ''')

            # Добавляем информацию
            await conn.execute(
                "INSERT INTO user_additional_info (telegram_id, info_text, added_by) VALUES (?, ?, ?)",
                (target_user_id, user_info, user_id)
            )
            await conn.commit()

        await message.answer(f"\n Информация о пользователе {target_user_id} добавлена!", reply_markup=get_users_submenu_keyboard())
        await log_action("add_user_info", user_id, None, f"Добавлена информация для пользователя {target_user_id}: {user_info}")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_add_user_info_text для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_add_user_info_text для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "🗑️ Стереть информацию")
async def delete_user_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer("\n Введите Telegram ID пользователя для удаления информации:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.delete_user_info)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в delete_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(Form.delete_user_info)
async def process_delete_user_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
        return

    try:
        target_user_id = int(message.text.strip())

        async with aiosqlite.connect(DB_PATH) as conn:
            # Проверяем существование пользователя
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"\n Пользователь с ID {target_user_id} не найден.", reply_markup=get_users_submenu_keyboard())
                await state.clear()
                return

            username = user[0]

            # Проверяем наличие дополнительной информации
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM user_additional_info WHERE telegram_id = ?",
                (target_user_id,)
            )
            info_count = (await cursor.fetchone())[0]

            if info_count == 0:
                await message.answer(f"\n Для пользователя @{username or 'Unknown'} (ID: {target_user_id}) нет дополнительной информации для удаления.", reply_markup=get_users_submenu_keyboard())
                await state.clear()
                return

            # Удаляем всю дополнительную информацию
            await conn.execute(
                "DELETE FROM user_additional_info WHERE telegram_id = ?",
                (target_user_id,)
            )
            await conn.commit()

        await message.answer(f"\n Вся дополнительная информация о пользователе @{username or 'Unknown'} (ID: {target_user_id}) удалена!", reply_markup=get_users_submenu_keyboard())
        await log_action("delete_user_info", user_id, None, f"Удалена информация о пользователе {target_user_id}")
        await state.clear()

    except ValueError:
        await message.answer("\n Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
        return
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_delete_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_delete_user_info для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "❌ Удалить заказ")
async def delete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT memo_order_id, customer_info, amount, status FROM orders ORDER BY created_at DESC")
            orders = await cursor.fetchall()

        if not orders:
            await message.answer("\n Нет заказов для удаления.", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        response = "\n Введите ID заказа для удаления:\n\n"
        for order_id, customer, amount, status in orders:
            status_text = "Ожидает" if status == "pending" else "В процессе" if status == "in_progress" else "Завершен"
            response += f"#{order_id} - {customer}, {amount:.2f} руб., Статус: {status_text}\n"

        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.delete_order)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в delete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в delete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(Form.delete_order)
async def process_delete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return

    order_id = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id, customer_info, amount, status FROM orders WHERE memo_order_id = ?", (order_id,))
            order = await cursor.fetchone()

            if not order:
                await message.answer(f"\n Заказ #{order_id} не найден.", reply_markup=get_cancel_keyboard(True))
                return

            order_db_id, customer, amount, status = order

            # Удаляем связанные записи
            await conn.execute("DELETE FROM order_escorts WHERE order_id = ?", (order_db_id,))
            await conn.execute("DELETE FROM order_applications WHERE order_id = ?", (order_db_id,))
            await conn.execute("DELETE FROM payouts WHERE order_id = ?", (order_db_id,))

            # Удаляем сам заказ
            await conn.execute("DELETE FROM orders WHERE id = ?", (order_db_id,))
            await conn.commit()

        await message.answer(f"\n Заказ #{order_id} (клиент: {customer}, сумма: {amount:.2f} руб.) успешно удален!", reply_markup=get_admin_keyboard())
        await log_action("delete_order", user_id, order_db_id, f"Удален заказ #{order_id}, клиент: {customer}")
        await state.clear()

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_delete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_delete_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "📋 Сквады")
async def squads_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'squads_submenu'
        await message.answer("\n Управление сквадами:", reply_markup=get_squads_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в squads_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👤 Сопровождающие")
async def escorts_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'escorts_submenu'
        await message.answer("\n Управление сопровождающими:", reply_markup=get_escorts_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в escorts_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "🚫 Баны/ограничения")
async def bans_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
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
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'balances_submenu'
        await message.answer("\n Управление балансами:", reply_markup=get_balances_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в balances_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📊 Прочее")
async def misc_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'misc_submenu'
        await message.answer("\n Прочие функции:", reply_markup=get_misc_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в misc_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👥 Пользователи")
async def users_submenu_handler(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'users_submenu'
        await message.answer("\n Управление пользователями:", reply_markup=get_users_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в users_submenu_handler для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📝 Заказы")
async def admin_orders_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'admin_orders_submenu'
        await message.answer("\n Управление заказами:", reply_markup=get_admin_orders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_orders_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "🔙 Назад")
async def back_to_menu(message: types.Message):
    user_id = message.from_user.id
    try:
        current_context = user_context.get(user_id)

        # Если пользователь в подменю обычного меню - возвращаем в обычное меню
        if current_context == 'orders_submenu':
            await message.answer("\n Выберите действие:", reply_markup=get_menu_keyboard(user_id))
            user_context[user_id] = 'main_menu'
        # Если админ находится в любом подменю админ-панели - возвращаем в админ-панель
        elif is_admin(user_id) and current_context in ['admin_panel', 'squads_submenu', 'escorts_submenu', 'bans_submenu', 'balances_submenu', 'misc_submenu', 'users_submenu', 'admin_orders_submenu', 'reputation_submenu']:
            await message.answer("\n Админ-панель:", reply_markup=get_admin_keyboard())
            user_context[user_id] = 'admin_panel'
        else:
            # Для всех остальных случаев - возвращаем в главное меню
            await message.answer("\n Выберите действие:", reply_markup=get_menu_keyboard(user_id))
            user_context[user_id] = 'main_menu'
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в back_to_menu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "⭐ Репутация")
async def reputation_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'reputation_submenu'
        await message.answer("\n Управление репутацией:", reply_markup=get_reputation_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в reputation_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "➕ Добавить репутацию")
async def add_reputation(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID и количество репутации для добавления через запятую\nПример: 123456789, 50",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.add_reputation)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в add_reputation для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()

@dp.message(Form.add_reputation)
async def process_add_reputation(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()
        return
    try:
        telegram_id, reputation_amount = [x.strip() for x in message.text.split(",")]
        telegram_id = int(telegram_id)
        reputation_amount = int(reputation_amount)
        
        if reputation_amount <= 0:
            await message.answer("❌ Количество репутации должно быть больше 0", reply_markup=get_cancel_keyboard(True))
            return
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"❌ Пользователь с ID {telegram_id} не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET reputation = reputation + ? WHERE telegram_id = ?",
                (reputation_amount, telegram_id)
            )
            await conn.commit()
        
        await message.answer(
            f"✅ Пользователю @{username or 'Unknown'} (ID: {telegram_id}) добавлено +{reputation_amount} репутации",
            reply_markup=get_reputation_submenu_keyboard()
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                telegram_id,
                f"🌟 Вам добавлено +{reputation_amount} репутации!"
            )
        except TelegramAPIError:
            pass
        
        await log_action(
            "add_reputation",
            user_id,
            None,
            f"Добавлено {reputation_amount} репутации пользователю @{username or 'Unknown'}"
        )
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Неверный формат. Используйте: ID, количество", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_add_reputation для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_add_reputation для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "➖ Снять репутацию")
async def remove_reputation(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        await message.answer(
            "\n Введите Telegram ID и количество репутации для снятия через запятую\nПример: 123456789, 25",
            reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.remove_reputation)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в remove_reputation для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()

@dp.message(Form.remove_reputation)
async def process_remove_reputation(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()
        return
    try:
        telegram_id, reputation_amount = [x.strip() for x in message.text.split(",")]
        telegram_id = int(telegram_id)
        reputation_amount = int(reputation_amount)
        
        if reputation_amount <= 0:
            await message.answer("❌ Количество репутации должно быть больше 0", reply_markup=get_cancel_keyboard(True))
            return
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (telegram_id,))
            user = await cursor.fetchone()
            if not user:
                await message.answer(f"❌ Пользователь с ID {telegram_id} не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user[0]
            await conn.execute(
                "UPDATE escorts SET reputation = reputation - ? WHERE telegram_id = ?",
                (reputation_amount, telegram_id)
            )
            await conn.commit()
        
        await message.answer(
            f"✅ У пользователя @{username or 'Unknown'} (ID: {telegram_id}) снято -{reputation_amount} репутации",
            reply_markup=get_reputation_submenu_keyboard()
        )
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                telegram_id,
                f"⚠️ У вас снято -{reputation_amount} репутации"
            )
        except TelegramAPIError:
            pass
        
        await log_action(
            "remove_reputation",
            user_id,
            None,
            f"Снято {reputation_amount} репутации у пользователя @{username or 'Unknown'}"
        )
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Неверный формат. Используйте: ID, количество", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_remove_reputation для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_remove_reputation для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_reputation_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "🚪 Выйти из админ-панели")
async def exit_admin_panel(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'main_menu'
        await message.answer("\n Выберите действие:", reply_markup=get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в exit_admin_panel для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

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

# Универсальный обработчик для кнопки "Отмена" во всех FSM состояниях
@dp.message(F.text == "🚫 Отмена")
async def cancel_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    current_state = await state.get_state()
    
    if current_state:
        await state.clear()
    
    # Определяем контекст пользователя для правильного возврата в меню
    current_context = user_context.get(user_id, 'main_menu')
    
    try:
        if is_admin(user_id):
            # Если админ находится в подменю админ-панели
            if current_context in ['squads_submenu', 'escorts_submenu', 'bans_submenu', 'balances_submenu', 'misc_submenu', 'users_submenu', 'admin_orders_submenu', 'reputation_submenu']:
                await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
                user_context[user_id] = 'admin_panel'
            elif current_context == 'admin_panel':
                await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
            else:
                await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
                user_context[user_id] = 'main_menu'
        else:
            # Для обычных пользователей
            if current_context == 'orders_submenu':
                await message.answer(MESSAGES["cancel_action"], reply_markup=get_orders_submenu_keyboard())
            else:
                await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
                user_context[user_id] = 'main_menu'
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в cancel_handler для {user_id}: {e}")
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "📩 Поддержка")
async def support_request(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        await message.answer(MESSAGES["support_request"], reply_markup=get_cancel_keyboard())
        await state.set_state(Form.support_message)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в support_request для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.message(Form.support_message)
async def process_support_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "🚫 Отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    support_message = message.text.strip()
    if not support_message:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard())
        return
    try:
        username = message.from_user.username or "Unknown"
        reply_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\n Ответить", callback_data=f"reply_support_{user_id}")]
        ])
        await notify_admins(
            f"\n Новый запрос в поддержку от @{username} (ID: {user_id}):\n\n{support_message}",
            reply_markup=reply_keyboard
        )
        await message.answer(MESSAGES["support_sent"], reply_markup=get_menu_keyboard(user_id))
        await log_action(
            "support_request",
            user_id,
            None,
            f"Запрос в поддержку: {support_message}"
        )
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_support_message для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
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