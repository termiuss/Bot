import asyncio
import logging
import csv
import os
import traceback

# ====== ВАЖНО ДЛЯ ИИ ======
# При обновлении кода всегда соблюдайте следующие правила для кнопок:
# 1. ВСЕ кнопки должны быть БЕЗ эмодзи 
# 2. ВСЕ кнопки должны быть строчными (маленькими буквами)
# 3. ВСЕ обработчики F.text == "текст" должны точно совпадать с текстом кнопок
# 4. Исключение: только inline кнопки могут содержать эмодзи для красоты
# ========================
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
    logger.error("BOT_TOKEN не найден в переменных окружения")
    logger.info("Пожалуйста, добавьте BOT_TOKEN через секреты Replit или .env файл")
    logger.info("Получите токен бота от @BotFather в Telegram")
    exit(1)

ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "")
if ADMIN_IDS_STR:
    ADMIN_IDS = [int(id.strip()) for id in ADMIN_IDS_STR.split(",") if id.strip()]
else:
    logger.error("ADMIN_IDS не найдены в переменных окружения")
    logger.info("пожалуйста добавьте ADMIN_IDS (разделенные запятой ID админов) через секреты Replit или .env файл")
    logger.info("получите свой Telegram ID от @userinfobot")
    exit(1)
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

# Канал для обязательной подписки
REQUIRED_CHANNEL_ID = -1002734060041
REQUIRED_CHANNEL_URL = "https://t.me/ItMEMOshop"

# Константы сообщений
MESSAGES = {
    "welcome": (
        "Добро пожаловать в бота Memo Shop!"
    ),
    "not_subscribed": "для использования бота необходимо подписаться на канал",
    "no_access": "у вас нет доступа к этой команде",
    "no_squads": "нет доступных сквадов",
    "no_escorts": "нет зарегистрированных сопровождающих",
    "no_orders": "заказы отсутствуют",
    "no_active_orders": "у вас нет активных заказов",
    "error": "произошла ошибка, пожалуйста попробуйте снова",
    "invalid_format": "неверный формат ввода, попробуйте снова",
    "order_completed": "заказ #{order_id} завершен пользователем @{username} (Telegram ID: {telegram_id}, PUBG ID: {pubg_id})",
    "order_already_completed": "заказ #{order_id} уже завершен",
    "balance_added": "баланс {amount} руб начислен пользователю {user_id}",
    "squad_full": "сквад '{squad_name}' уже имеет максимум 6 участников",
    "squad_too_small": "в скваде '{squad_name}' должно быть минимум 2 участника для принятия заказа",
    "order_added": "заказ #{order_id} добавлен, сумма: {amount} руб, описание: {description}, клиент: {customer}",
    "rules_not_accepted": "пожалуйста примите правила, оферту и политику конфиденциальности",
    "user_banned": "вы заблокированы",
    "user_restricted": "ваш доступ к сопровождениям ограничен до {date}",
    "balance_zeroed": "баланс пользователя {user_id} обнулен",
    "pubg_id_updated": "PUBG ID успешно обновлен",
    "ping": "бот активен",
    "order_taken": "заказ #{order_id} принят сквадом {squad_name}\nучастники:\n{participants}",
    "order_not_enough_members": "в скваде '{squad_name}' недостаточно участников (минимум 2)",
    "order_already_in_progress": "заказ #{order_id} уже в наборе или принят",
    "order_joined": "вы присоединились к набору для заказа #{order_id}\nтекущий состав:\n{participants}",
    "order_confirmed": "заказ #{order_id} подтвержден и принят\nучастники:\n{participants}",
    "not_in_squad": "вы не состоите в скваде",
    "max_participants": "достигнуто максимальное количество участников",
    "rating_submitted": "оценка {rating} для заказа #{order_id} сохранена, репутация обновлена",
    "rate_order": "поставьте оценку за заказ #{order_id} (1-5):",
    "payout_log": "выплата: @{username} получил {amount} руб за заказ #{order_id}, дата: {date}",
    "payout_request": "запрос выплаты от @{username} на сумму {amount} руб за заказ #{order_id}",
    "payout_receipt": "я, @{username}, получил оплату {amount} руб за заказ #{order_id}",
    "export_success": "данные успешно экспортированы в {filename}",
    "no_data_to_export": "нет данных для экспорта",
    "reminder": "напоминание: заказ #{order_id} не завершен более 12 часов, пожалуйста завершите его",
    "squad_deleted": "сквад '{squad_name}' успешно расформирован",
    "cancel_action": "действие отменено",
    "support_request": "введите ваше сообщение для поддержки:",
    "support_sent": "ваше сообщение отправлено администраторам",
    "user_unbanned": "пользователь @{username} разблокирован",
    "user_unrestricted": "ограничения для пользователя @{username} сняты"
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
    payout_method = State()
    delete_squad = State()
    support_message = State()
    support_reply = State()
    unban_user = State()
    unrestrict_user = State()


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
    # Состояния для анкеты вступления в команду
    application_city = State()
    application_pubg_id = State()
    application_cd = State()
    application_age = State()
    application_confirm = State()
    # Состояние для информации о пользователе
    user_info_id = State()
    # Состояние для задания критериев команды
    set_criteria = State()
    # Состояния для регистрации новых пользователей
    registration_username = State()
    registration_pubg_id = State()
    # Состояния для добавления участника лидером
    add_member_telegram_id = State()
    add_member_pubg_id = State()
    add_member_city = State()
    add_member_cd = State()
    add_member_age = State()

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
            # Включаем поддержку foreign keys
            await conn.execute("PRAGMA foreign_keys = ON")
            
            # Читаем и выполняем SQL-схему из файла
            try:
                with open('schema.sql', 'r', encoding='utf-8') as f:
                    schema_sql = f.read()
                    await conn.executescript(schema_sql)
            except FileNotFoundError:
                logger.warning("Файл schema.sql не найден, создаем таблицы вручную")
                # Fallback - создание таблиц вручную
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS squads (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        rating REAL DEFAULT 0,
                        rating_count INTEGER DEFAULT 0,
                        leader_id INTEGER,
                        FOREIGN KEY (leader_id) REFERENCES escorts (id) ON DELETE SET NULL
                    )
                ''')
                
                await conn.execute('''
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
                    )
                ''')
                
                await conn.execute('''
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
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS order_escorts (
                        order_id INTEGER,
                        escort_id INTEGER,
                        pubg_id TEXT,
                        PRIMARY KEY (order_id, escort_id),
                        FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                        FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS order_applications (
                        order_id INTEGER,
                        escort_id INTEGER,
                        squad_id INTEGER,
                        pubg_id TEXT,
                        PRIMARY KEY (order_id, escort_id),
                        FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
                        FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE,
                        FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS payouts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id INTEGER,
                        escort_id INTEGER,
                        amount REAL,
                        payout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE SET NULL,
                        FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE SET NULL
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS action_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        action_type TEXT,
                        user_id INTEGER,
                        order_id INTEGER,
                        description TEXT,
                        action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS squad_leaders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        leader_id INTEGER NOT NULL,
                        squad_id INTEGER NOT NULL,
                        appointed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (leader_id) REFERENCES escorts (id) ON DELETE CASCADE,
                        FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE CASCADE,
                        UNIQUE(leader_id, squad_id)
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS squad_applications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        squad_id INTEGER NOT NULL,
                        city TEXT,
                        pubg_id TEXT,
                        cd TEXT,
                        age TEXT,
                        status TEXT DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES escorts (id) ON DELETE CASCADE,
                        FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE CASCADE
                    )
                ''')
                
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS squad_criteria (
                        squad_id INTEGER PRIMARY KEY,
                        criteria_text TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE CASCADE
                    )
                ''')
                
                # Создаем индексы
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_squad_leaders_leader_id ON squad_leaders (leader_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_squad_leaders_squad_id ON squad_leaders (squad_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_squad_applications_user_id ON squad_applications (user_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_squad_applications_squad_id ON squad_applications (squad_id)")
            
            await conn.commit()
        logger.info("База данных успешно инициализирована")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка инициализации базы данных: {e}\n\n{traceback.format_exc()}")
        raise
    except Exception as e:
        logger.error(f"Общая ошибка инициализации базы данных: {e}\n\n{traceback.format_exc()}")
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

async def find_or_create_user(telegram_id: int, username: str = None):
    """Находит пользователя или создает его если не существует"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id, pubg_id, balance, reputation, completed_orders, username, "
                "rating, rating_count, is_banned, ban_until, restrict_until, rules_accepted "
                "FROM escorts WHERE telegram_id = ?", (telegram_id,)
            )
            user = await cursor.fetchone()
            
            if not user:
                # Создаем пользователя если его нет
                await conn.execute(
                    "INSERT OR IGNORE INTO escorts (telegram_id, username, rules_accepted) VALUES (?, ?, 1)",
                    (telegram_id, username or "Unknown")
                )
                await conn.commit()
                
                # Получаем созданного пользователя
                cursor = await conn.execute(
                    "SELECT id, squad_id, pubg_id, balance, reputation, completed_orders, username, "
                    "rating, rating_count, is_banned, ban_until, restrict_until, rules_accepted "
                    "FROM escorts WHERE telegram_id = ?", (telegram_id,)
                )
                user = await cursor.fetchone()
                logger.info(f"Создан новый пользователь {telegram_id} (@{username})")
            
            return user
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в find_or_create_user для {telegram_id}: {e}\n\n{traceback.format_exc()}")
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

async def notify_all_users_about_new_order(order_id: str, customer_info: str, amount: float):
    """Отправляет уведомление всем пользователям о новом заказе"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id FROM escorts WHERE rules_accepted = 1")
            users = await cursor.fetchall()
        
        notification_text = (
            f" НОВЫЙ ЗАКАЗ!\n\n"
            f" Заказ #{order_id}\n"
            f" Клиент: {customer_info}\n"
            f" Сумма: {amount:.0f} руб.\n\n"
            f"Перейдите в раздел 'Заказы' → 'Доступные заказы' чтобы присоединиться!"
        )
        
        successful_notifications = 0
        failed_notifications = 0
        
        for (telegram_id,) in users:
            try:
                await bot.send_message(telegram_id, notification_text)
                successful_notifications += 1
            except TelegramAPIError as e:
                failed_notifications += 1
                logger.warning(f"Не удалось отправить уведомление пользователю {telegram_id}: {e}")
        
        logger.info(f"Уведомления о новом заказе #{order_id}: отправлено {successful_notifications}, не удалось {failed_notifications}")
        
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомлений о новом заказе: {e}")

async def show_order_participants_menu(message, order_db_id: int, memo_order_id: str):
    """Показывает динамическое меню участников заказа"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем всех участников заказа с их Telegram username и PUBG ID
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id
                FROM order_applications oa
                JOIN escorts e ON oa.escort_id = e.id
                WHERE oa.order_id = ?
                ORDER BY oa.escort_id
                ''', (order_db_id,)
            )
            participants = await cursor.fetchall()

        if not participants:
            response = f"заказ #{memo_order_id}\n\nучастники набора:\nпока никого нет"
        else:
            response = f"заказ #{memo_order_id}\n\nучастники набора ({len(participants)}/4):\n\n"
            for i, (telegram_id, username, pubg_id) in enumerate(participants, 1):
                # Форматируем отображение участников с их Telegram и PUBG данными
                response += f"{i}. @{username or 'Unknown'} (ID: {telegram_id})\n"
                response += f"   PUBG ID: {pubg_id or 'не указан'}\n\n"

        # Определяем доступные кнопки
        keyboard_buttons = []
        
        # Проверяем ограничения
        can_start = len(participants) >= 2
        can_join = len(participants) < 4
        
        # Кнопка "Начать выполнение" доступна только при минимум 2 участниках
        if can_start:
            keyboard_buttons.append([InlineKeyboardButton(text="начать выполнение", callback_data=f"start_order_{order_db_id}")])
        
        # Кнопка "Присоединиться" доступна если меньше 4 участников
        if can_join:
            keyboard_buttons.append([InlineKeyboardButton(text="присоединиться", callback_data=f"join_order_{order_db_id}")])
        
        # Всегда доступные кнопки
        keyboard_buttons.append([InlineKeyboardButton(text="покинуть заказ", callback_data=f"leave_order_{order_db_id}")])
        keyboard_buttons.append([InlineKeyboardButton(text="обновить", callback_data=f"refresh_order_{order_db_id}")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        # Информация о статусе набора
        if len(participants) == 0:
            response += "для начала выполнения нужно минимум 2 участника"
        elif len(participants) == 1:
            response += "нужен еще минимум 1 участник для начала выполнения"
        elif len(participants) >= 2 and len(participants) < 4:
            response += f"можно начинать выполнение, можно добавить еще {4 - len(participants)} участник(ов)"
        elif len(participants) == 4:
            response += "набор полный, можно начинать выполнение"

        await message.edit_text(response, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Ошибка в show_order_participants_menu: {e}")
        await message.edit_text("Произошла ошибка при обновлении меню участников.")

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
        full_message = f"{message}\n\n Участники сквада: {mention_text}"

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



# --- Проверка подписки ---
async def check_subscription(user_id: int) -> bool:
    """Проверяет подписку пользователя на обязательный канал"""
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Ошибка проверки подписки для {user_id}: {e}")
        return False

# --- Проверка доступа ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def is_leader(user_id: int) -> bool:
    """Проверяет, является ли пользователь лидером сквада"""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                JOIN squads s ON sl.squad_id = s.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            result = await cursor.fetchone()
            return result is not None
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
    # Проверяем, состоит ли пользователь в скваде
    escort = await get_escort(user_id)
    has_squad = escort and escort[1] is not None  # squad_id
    
    if not has_squad:
        # Для пользователей без сквада - только кнопки для поиска команды
        base_keyboard = [
            [KeyboardButton(text="найти команду")],
            [KeyboardButton(text="личный кабинет")],
            [KeyboardButton(text="информация"), KeyboardButton(text="поддержка")],
        ]
        
        # Добавляем админ-панель для админов
        if is_admin(user_id):
            base_keyboard.append([KeyboardButton(text="админ-панель")])
    else:
        # Обычное меню для пользователей со сквадом (без кнопки информация)
        base_keyboard = [
            [KeyboardButton(text="заказы")],
            [KeyboardButton(text="личный кабинет")],
            [KeyboardButton(text="поддержка")],
            [KeyboardButton(text="рейтинг пользователей"), KeyboardButton(text="рейтинг сквадов")],
        ]

        # Добавляем кнопки лидера если пользователь является лидером
        if await is_leader(user_id):
            base_keyboard.append([KeyboardButton(text="управление участниками"), KeyboardButton(text="управление сквадом")])

        # Добавляем админ-панель для админов
        if is_admin(user_id):
            base_keyboard.append([KeyboardButton(text="админ-панель")])
        
        # Добавляем кнопку выплаты в конец для всех
        base_keyboard.append([KeyboardButton(text="получить выплату")])

    keyboard = ReplyKeyboardMarkup(
        keyboard=base_keyboard,
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="сквады"), KeyboardButton(text="сопровождающие")],
            [KeyboardButton(text="заказы"), KeyboardButton(text="баны/ограничения")],
            [KeyboardButton(text="балансы"), KeyboardButton(text="пользователи")],
            [KeyboardButton(text="управление лидерами")],
            [KeyboardButton(text="связь"), KeyboardButton(text="выйти из админ-панели")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_orders_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="доступные заказы")],
            [KeyboardButton(text="мои заказы"), KeyboardButton(text="завершить заказ")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_squads_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="добавить сквад"), KeyboardButton(text="список сквадов")],
            [KeyboardButton(text="расформировать сквад")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_escorts_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="добавить сопровождающего"), KeyboardButton(text="удалить сопровождающего")],
            [KeyboardButton(text="балансы сопровождающих")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_bans_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="бан навсегда"), KeyboardButton(text="бан на время")],
            [KeyboardButton(text="снять бан"), KeyboardButton(text="снять ограничение")],
            [KeyboardButton(text="ограничить")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_balances_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="начислить"), KeyboardButton(text="обнулить баланс")],
            [KeyboardButton(text="все балансы")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard


def get_admin_orders_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="добавить заказ"), KeyboardButton(text="удалить заказ")],
            [KeyboardButton(text="оценить заказ")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_users_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="список пользователей")],
            [KeyboardButton(text="информация о пользователе")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_reputation_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="добавить репутацию"), KeyboardButton(text="снять репутацию")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard



def get_rules_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="принять условия")],
            [KeyboardButton(text="политика конфиденциальности")],
            [KeyboardButton(text="правила")],
            [KeyboardButton(text="публичная оферта")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard

def get_cancel_keyboard(is_admin: bool = False):
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

def get_order_keyboard(order_id: int):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="присоединиться", callback_data=f"join_order_{order_id}")],
        [InlineKeyboardButton(text="отмена", callback_data=f"cancel_order_{order_id}")]
    ])
    return keyboard

def get_confirmed_order_keyboard(order_id: str, is_admin: bool = False):
    buttons = [[InlineKeyboardButton(text="завершить заказ", callback_data=f"complete_order_{order_id}")]]

    # Кнопка отмены только для админов
    if is_admin:
        buttons.append([InlineKeyboardButton(text="отменить заказ", callback_data=f"cancel_confirmed_order_{order_id}")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_confirmed_order_keyboard_user(order_id: str):
    # Для обычных пользователей - никаких кнопок после старта заказа
    return None

def get_rating_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data=f"rate_{order_id}_1"),
            InlineKeyboardButton(text="2", callback_data=f"rate_{order_id}_2"),
            InlineKeyboardButton(text="3", callback_data=f"rate_{order_id}_3")
        ],
        [
            InlineKeyboardButton(text="4", callback_data=f"rate_{order_id}_4"),
            InlineKeyboardButton(text="5", callback_data=f"rate_{order_id}_5")
        ]
    ])
    return keyboard

# --- Новые клавиатуры для управления лидерами ---
def get_leaders_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="добавить лидера команды")],
            [KeyboardButton(text="список лидеров"), KeyboardButton(text="убрать лидера")],
            [KeyboardButton(text="написать лидеру")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_squad_management_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="переименовать сквад")],
            [KeyboardButton(text="список заказов"), KeyboardButton(text="статус сопровождений")],
            [KeyboardButton(text="критерии команды")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_members_management_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="добавить участника")],
            [KeyboardButton(text="удалить участника")],
            [KeyboardButton(text="список участников")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_communication_submenu_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="связаться с пользователем")],
            [KeyboardButton(text="сделать объявление")],
            [KeyboardButton(text="назад")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_personal_cabinet_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="профиль")],
            [KeyboardButton(text="баланс")],
            [KeyboardButton(text="мой рейтинг")],
            [KeyboardButton(text="назад")],
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
        
        # Проверяем корректность данных escort
        if len(escort) < 13:
            logger.error(f"Некорректная структура данных escort для пользователя {user_id}: {escort}")
            await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
            return False
            
        if escort[9]:  # is_banned
            await message.answer(MESSAGES["user_banned"], reply_markup=ReplyKeyboardRemove())
            return False
        if escort[10]:  # ban_until
            try:
                if datetime.fromisoformat(escort[10]) > datetime.now():
                    formatted_date = datetime.fromisoformat(escort[10]).strftime("%d.%m.%Y %H:%M")
                    await message.answer(MESSAGES["user_banned"].format(date=formatted_date), reply_markup=ReplyKeyboardRemove())
                    return False
            except (ValueError, TypeError):
                logger.warning(f"Некорректная дата ban_until для пользователя {user_id}: {escort[10]}")
        if escort[11]:  # restrict_until
            try:
                if datetime.fromisoformat(escort[11]) > datetime.now():
                    formatted_date = datetime.fromisoformat(escort[11]).strftime("%d.%m.%Y %H:%M")
                    await message.answer(MESSAGES["user_restricted"].format(date=formatted_date), reply_markup=ReplyKeyboardRemove())
                    return False
            except (ValueError, TypeError):
                logger.warning(f"Некорректная дата restrict_until для пользователя {user_id}: {escort[11]}")
        if not escort[12] and initial_start:  # rules_accepted
            await message.answer(MESSAGES["rules_not_accepted"], reply_markup=get_rules_keyboard())
            return False
        
        # Проверка обязательной подписки (кроме админов)
        if not is_admin(user_id) and not await check_subscription(user_id):
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="подписаться на канал", url=REQUIRED_CHANNEL_URL)],
                [InlineKeyboardButton(text="я подписался", callback_data="check_subscription")]
            ])
            await message.answer(MESSAGES["not_subscribed"], reply_markup=keyboard)
            return False
        
        return True
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в check_access для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
        return False
    except (ValueError, TypeError, IndexError) as e:
        logger.error(f"Ошибка данных в check_access для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка в check_access для {user_id}: {e}\n\n{traceback.format_exc()}")
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

@dp.message(F.text == "принять условия")
async def accept_rules(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("UPDATE escorts SET rules_accepted = 1 WHERE telegram_id = ?", (user_id,))
            await conn.commit()
            
            # Проверяем, нужна ли регистрация
            cursor = await conn.execute("SELECT username, pubg_id FROM escorts WHERE telegram_id = ?", (user_id,))
            user_data = await cursor.fetchone()
        
        if user_data and (not user_data[0] or user_data[0] == "Unknown" or not user_data[1]):
            # Нужна регистрация
            await message.answer(
                "Условия приняты! Для завершения регистрации нам нужна дополнительная информация.\n\n"
                "Введите ваш Telegram Username (без @, например: ivan_petrov).\n"
                "Если у вас нет username, напишите тире: -",
                reply_markup=ReplyKeyboardRemove()
            )
            await state.set_state(Form.registration_username)
        else:
            user_context[user_id] = 'main_menu'
            await message.answer(f"Условия приняты! Добро пожаловать!\n\nВыберите действие:", reply_markup=await get_menu_keyboard(user_id))
            
        logger.info(f"Пользователь {user_id} принял условия")
        await log_action("accept_rules", user_id, None, "Пользователь принял условия")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в accept_rules для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в accept_rules для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(Form.registration_username)
async def process_registration_username(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.text.strip()
    
    if not username:
        await message.answer("Пожалуйста, введите ваш username или тире (-)")
        return
    
    # Сохраняем username
    await state.update_data(username=username if username != "-" else None)
    await message.answer(
        "Теперь введите ваш PUBG ID.\n"
        "Если у вас нет PUBG ID, напишите тире: -"
    )
    await state.set_state(Form.registration_pubg_id)

@dp.message(Form.registration_pubg_id)
async def process_registration_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    pubg_id = message.text.strip()
    
    if not pubg_id:
        await message.answer("Пожалуйста, введите ваш PUBG ID или тире (-)")
        return
    
    try:
        # Получаем данные из состояния
        data = await state.get_data()
        username = data.get('username')
        
        # Обновляем данные пользователя
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE escorts SET username = ?, pubg_id = ? WHERE telegram_id = ?",
                (username, pubg_id if pubg_id != "-" else None, user_id)
            )
            await conn.commit()
        
        user_context[user_id] = 'main_menu'
        await message.answer(
            "Регистрация завершена! Добро пожаловать!\n\nВыберите действие:",
            reply_markup=await get_menu_keyboard(user_id)
        )
        await state.clear()
        logger.info(f"Пользователь {user_id} завершил регистрацию")
        
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_registration_pubg_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

# Обработчик для ввода PUBG ID удален - теперь PUBG ID берется автоматически из базы данных

@dp.message(F.text == "информация")
async def info_handler(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="политика конфиденциальности", url=PRIVACY_URL)],
            [InlineKeyboardButton(text="правила", url=RULES_URL)],
            [InlineKeyboardButton(text="публичная оферта", url=OFFER_URL)]
        ])
        response = (
            " Информация о боте:\n"
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
            " О проекте:\n"
            "Этот бот предназначен для распределения заказов по сопровождению в Metro Royale. "
            "Все действия фиксируются, выплаты прозрачны."
        )
        await callback.message.answer(response, reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в about_project для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))



@dp.message(F.text == "политика конфиденциальности")
async def privacy_link(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="политика конфиденциальности", url=PRIVACY_URL)]
        ])
        await message.answer("📜 Политика конфиденциальности:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в privacy_link: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "правила")
async def rules_link(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="правила", url=RULES_URL)]
        ])
        await message.answer("📖 Правила:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в rules_link: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "публичная оферта")
async def offer_link(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="публичная оферта", url=OFFER_URL)]
        ])
        await message.answer("📜 Публичная оферта:", reply_markup=keyboard)
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в rules_links: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "личный кабинет")
async def personal_cabinet(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        user_context[user_id] = 'personal_cabinet'
        await message.answer(" Личный кабинет:", reply_markup=get_personal_cabinet_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка в personal_cabinet для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "профиль")
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
            f" Ваш профиль:\n\n"
            f"Username: @{username or 'Unknown'}\n"
            f"PUBG ID: {pubg_id or 'не указан'}\n"
            f"Сквад: {squad[0] if squad else 'не назначен'}\n"
            f"Баланс: {balance:.2f} руб.\n"
            f"Выполнено заказов: {completed_orders}\n"
            f"Рейтинг в звездах: {stars_rating}\n"
        )
        await message.answer(response, reply_markup=get_personal_cabinet_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в my_profile для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_personal_cabinet_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в my_profile для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_personal_cabinet_keyboard())

@dp.message(F.text == "баланс")
async def my_balance(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("Профиль не найден.", reply_markup=get_personal_cabinet_keyboard())
            return
        
        _, _, _, balance, _, _, username, _, _, _, _, _, _ = escort
        
        if balance > 0:
            response = f"Ваш баланс: {balance:.2f} руб."
        else:
            response = "У вас пока нет средств на балансе"
        
        await message.answer(response, reply_markup=get_personal_cabinet_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в my_balance для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_personal_cabinet_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка в my_balance для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_personal_cabinet_keyboard())

@dp.message(F.text == "мой рейтинг")
async def my_rating(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("Профиль не найден.", reply_markup=get_personal_cabinet_keyboard())
            return
        
        escort_id, _, _, _, _, completed_orders, username, _, _, _, _, _, _ = escort
        
        # Получаем рейтинг в звездах
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT total_rating, rating_count FROM escorts WHERE telegram_id = ?",
                (user_id,)
            )
            rating_data = await cursor.fetchone()
        
        stars_rating = "Нет оценок"
        if rating_data and rating_data[1] > 0:
            star_rating = rating_data[0] / rating_data[1]
            stars_rating = f"★ {star_rating:.2f} / 5.00"
        
        # Получаем позицию в рейтинге
        user_position, user_rating_value = await get_user_rating_position(user_id)
        position_text = f"Позиция в рейтинге: {user_position}" if user_position else "Позиция: не определена"
        
        response = (
            f"Ваш рейтинг:\n\n"
            f" @{username or 'Unknown'}\n"
            f"Рейтинг: {stars_rating}\n"
            f"Выполнено заказов: {completed_orders}\n"
            f"{position_text}"
        )
        
        await message.answer(response, reply_markup=get_personal_cabinet_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в my_rating для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_personal_cabinet_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка в my_rating для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_personal_cabinet_keyboard())

@dp.message(F.text == "заказы")
async def orders_menu(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    
    # Проверяем, является ли пользователь админом
    if is_admin(user_id):
        # Перенаправляем к функции админ-панели заказов
        await admin_orders_submenu(message)
        return
        
    try:
        user_context[user_id] = 'orders_submenu'
        await message.answer("\n Управление заказами:", reply_markup=get_orders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в orders_menu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "доступные заказы")
async def available_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        # Получаем информацию о пользователе и его скваде
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("Ваш профиль не найден.", reply_markup=get_orders_submenu_keyboard())
            return

        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort

        # Получаем все заказы со статусом pending
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.id, o.memo_order_id, o.customer_info, o.amount, o.created_at
                FROM orders o
                WHERE o.status = 'pending'
                ORDER BY o.created_at DESC
                '''
            )
            all_orders = await cursor.fetchall()

        if not all_orders:
            await message.answer("Заказы отсутствуют", reply_markup=get_orders_submenu_keyboard())
            return

        if not squad_id:
            # Для пользователей без сквада показываем инлайн кнопки, но без возможности присоединения
            keyboard_buttons = []
            for order_id, memo_order_id, customer_info, amount, created_at in all_orders:
                button_text = f"#{memo_order_id} - {customer_info} ({amount:.0f}₽)"
                # Добавляем callback_data, но обработчик будет показывать сообщение о необходимости сквада
                keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"no_squad_order_{order_id}")])
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
            await message.answer("Доступные заказы:\n\nДля участия в заказах необходимо состоять в скваде!", reply_markup=keyboard)
            return

        # Для пользователей со сквадом - фильтруем заказы и создаем клавиатуру
        available_orders_list = []
        for order_id, memo_order_id, customer_info, amount, created_at in all_orders:
            # Проверяем, есть ли уже заявки на этот заказ
            async with aiosqlite.connect(DB_PATH) as conn:
                cursor = await conn.execute(
                    "SELECT squad_id, COUNT(*) FROM order_applications WHERE order_id = ? GROUP BY squad_id",
                    (order_id,)
                )
                applications = await cursor.fetchall()

            # Если заявок нет, или есть заявки от нашего сквада - показываем заказ
            if not applications:
                # Свободный заказ
                available_orders_list.append((order_id, memo_order_id, customer_info, amount, 0, None))
            else:
                # Проверяем, есть ли заявки от нашего сквада
                for app_squad_id, app_count in applications:
                    if app_squad_id == squad_id:
                        available_orders_list.append((order_id, memo_order_id, customer_info, amount, app_count, squad_id))
                        break

        if not available_orders_list:
            await message.answer("Заказы отсутствуют", reply_markup=get_orders_submenu_keyboard())
            return

        # Создаем инлайн клавиатуру с заказами
        keyboard_buttons = []
        for db_id, order_id, customer, amount, app_count, recruiting_squad in available_orders_list:
            button_text = f"#{order_id} - {customer} ({amount:.0f}₽)"
            if app_count > 0 and recruiting_squad == squad_id:
                button_text += f" {app_count}"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"select_order_{db_id}")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        # Получаем название сквада
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad_result = await cursor.fetchone()
            squad_name = squad_result[0] if squad_result else "Unknown"

        await message.answer("Доступные заказы\n\nВыберите заказ:", reply_markup=keyboard)

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в available_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_orders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в available_orders для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_orders_submenu_keyboard())

@dp.message(F.text == "мои заказы")
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

@dp.message(F.text == "завершить заказ")
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
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
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
            f"заказ {order_id} завершен\n"
            f"юзер - {user_id} - {pubg_id or 'не указан'}",
            reply_markup=await get_menu_keyboard(user_id)
        )
        await notify_admins(
            f"заказ {order_id} завершен\n"
            f"юзер - {user_id} - {pubg_id or 'не указан'}"
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

@dp.message(F.text == "оценить заказ")
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
            await message.answer(" Нет завершённых заказов для оценки.", reply_markup=get_admin_orders_submenu_keyboard())
            return

        for memo_order_id, customer_info, amount in orders:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="1", callback_data=f"admin_rate_{memo_order_id}_1"),
                    InlineKeyboardButton(text="2", callback_data=f"admin_rate_{memo_order_id}_2"),
                    InlineKeyboardButton(text="3", callback_data=f"admin_rate_{memo_order_id}_3"),
                    InlineKeyboardButton(text="4", callback_data=f"admin_rate_{memo_order_id}_4"),
                    InlineKeyboardButton(text="5", callback_data=f"admin_rate_{memo_order_id}_5")
                ]
            ])

            await message.answer(
                f" Заказ #{memo_order_id}\n"
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
                await callback.message.edit_text(" Заказ не найден или не завершён.")
                await callback.answer()
                return

            order_db_id, squad_id = order

            # Обновляем рейтинг заказа
            await conn.execute(
                "UPDATE orders SET rating = ? WHERE id = ?",
                (rating, order_db_id)
            )

            # Добавляем дополнительную репутацию участникам в зависимости от оценки
            reputation_bonus = rating * 100  # 1звезд=100, 2звезд=200, ..., 5звезд=500

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
            f" Заказ #{memo_order_id} оценён на {rating}звезд\n"
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

@dp.message(F.text == "получить выплату")
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

        # Показываем инлайн-меню для выбора способа вывода
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="uc", callback_data="payout_method_uc")],
            [InlineKeyboardButton(text="card", callback_data="payout_method_card")]
        ])

        await message.answer(
            f" Доступно для вывода: {balance:.2f} руб.\n\n"
            f"Выберите способ вывода:", 
            reply_markup=keyboard
        )
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в request_payout для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в request_payout для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.callback_query(F.data.startswith("payout_method_"))
async def select_payout_method(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        payout_method = callback.data.split("_")[-1]  # "uc" или "card"
        
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        
        balance = escort[3]
        
        # Сохраняем выбранный способ вывода
        await state.update_data(payout_method=payout_method)
        
        method_text = "UC" if payout_method == "uc" else "банковскую карту"
        
        await callback.message.edit_text(
            f" Способ вывода: {method_text}\n"
            f"💳 Доступно: {balance:.2f} руб.\n\n"
            f"Введите сумму для выплаты:"
        )
        
        # Отправляем новое сообщение с клавиатурой отмены
        await callback.message.answer("Введите сумму:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.payout_request)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка в select_payout_method для {user_id}: {e}")
        await callback.answer(" Произошла ошибка")

@dp.message(Form.payout_request)
async def process_payout_request(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
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

        # Получаем выбранный способ вывода
        data = await state.get_data()
        payout_method = data.get('payout_method', 'unknown')
        method_text = "UC" if payout_method == "uc" else "банковскую карту" if payout_method == "card" else "неизвестный способ"

        # Создаем клавиатуру для администраторов
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="принять выплату", callback_data=f"approve_payout_{user_id}_{payout_amount}_{payout_method}")],
            [InlineKeyboardButton(text="отклонить выплату", callback_data=f"reject_payout_{user_id}_{payout_amount}_{payout_method}")]
        ])

        await message.answer(
            f"\n Запрос на выплату {payout_amount:.2f} руб. через {method_text} отправлен администраторам!\n\n"
            f"Просьба связаться с администратором @ItMEMOO\n"
            f"Или @MemoSpamBlock_bot",
            reply_markup=await get_menu_keyboard(user_id)
        )
        await notify_admins(
            f"\n Запрос выплаты от @{username or 'Unknown'} (ID: {user_id})\n"
            f" Сумма: {payout_amount:.2f} руб.\n"
            f"💳 Способ: {method_text}",
            reply_markup=admin_keyboard
        )
        await log_action(
            "payout_request",
            user_id,
            None,
            f"Запрос выплаты {payout_amount:.2f} руб. через {method_text}"
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

@dp.callback_query(F.data.startswith("no_squad_order_"))
async def no_squad_order_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        await callback.answer("Для участия в заказах необходимо состоять в скваде!", show_alert=True)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в no_squad_order_callback для {user_id}: {e}")

@dp.callback_query(F.data.startswith("select_order_"))
async def select_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT memo_order_id, customer_info, amount FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order:
                await callback.answer(" Заказ не найден.")
                return
        
        memo_order_id, customer_info, amount = order
        
        # Показываем кнопку "Присоединиться к набору"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="присоединиться к набору", callback_data=f"join_recruit_{order_db_id}")],
            [InlineKeyboardButton(text="отмена", callback_data=f"cancel_order_{order_db_id}")]
        ])
        
        order_text = f" Заказ #{memo_order_id}\n Клиент: {customer_info}\n Сумма: {amount:.2f} руб.\n\nВыберите действие:"
        await callback.message.edit_text(order_text, reply_markup=keyboard)
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в select_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.answer(" Произошла ошибка")
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в select_order для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.answer(" Произошла ошибка")

@dp.callback_query(F.data.startswith("join_recruit_"))
async def join_recruit(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort
        if not pubg_id:
            await callback.message.answer(" Укажите PUBG ID!", reply_markup=await get_menu_keyboard(user_id))
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

            memo_order_id = order[1]

            # Проверяем, набирается ли заказ другим сквадом
            cursor = await conn.execute(
                "SELECT squad_id FROM order_applications WHERE order_id = ? LIMIT 1", (order_db_id,)
            )
            existing_squad = await cursor.fetchone()
            if existing_squad and existing_squad[0] != squad_id:
                await callback.message.answer(" Этот заказ уже набирается другим сквадом!", reply_markup=await get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Проверяем, не присоединился ли пользователь уже
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                # Показываем меню участников
                await show_order_participants_menu(callback.message, order_db_id, memo_order_id)
                await callback.answer()
                return

            # Проверяем максимальное количество участников
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND squad_id = ?",
                (order_db_id, squad_id)
            )
            participant_count = (await cursor.fetchone())[0]
            
            if participant_count >= 4:
                await callback.answer(" Достигнуто максимальное количество участников (4)!", show_alert=True)
                return

            # Добавляем пользователя к заказу
            await conn.execute(
                "INSERT INTO order_applications (order_id, escort_id, squad_id, pubg_id) VALUES (?, ?, ?, ?)",
                (order_db_id, escort_id, squad_id, pubg_id)
            )
            await conn.commit()
        
        # Отображаем динамическое меню участников
        await show_order_participants_menu(callback.message, order_db_id, memo_order_id)
        await log_action("join_order", user_id, order_db_id, f"Пользователь {user_id} присоединился к заказу #{memo_order_id}")
        await callback.answer(" Вы присоединились к набору!")
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в join_recruit для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в join_recruit для {user_id}: {e}\n\n{traceback.format_exc()}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("join_order_"))
async def join_order(callback: types.CallbackQuery):
    """Обработчик для кнопки присоединиться в меню участников"""
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id, squad_id, pubg_id, _, _, _, _, _, _, _, _, _, _ = escort
        if not pubg_id:
            await callback.message.answer(" Укажите PUBG ID!", reply_markup=await get_menu_keyboard(user_id))
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

            memo_order_id = order[1]

            # Проверяем, не присоединился ли пользователь уже
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                await callback.answer(" Вы уже присоединились к этому заказу!")
                return

            # Проверяем максимальное количество участников
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND squad_id = ?",
                (order_db_id, squad_id)
            )
            participant_count = (await cursor.fetchone())[0]
            
            if participant_count >= 4:
                await callback.answer(" Достигнуто максимальное количество участников (4)!", show_alert=True)
                return

            # Добавляем пользователя к заказу
            await conn.execute(
                "INSERT INTO order_applications (order_id, escort_id, squad_id, pubg_id) VALUES (?, ?, ?, ?)",
                (order_db_id, escort_id, squad_id, pubg_id)
            )
            await conn.commit()
        
        # Обновляем динамическое меню участников
        await show_order_participants_menu(callback.message, order_db_id, memo_order_id)
        await log_action("join_order", user_id, order_db_id, f"Пользователь {user_id} присоединился к заказу #{memo_order_id}")
        await callback.answer(" Вы присоединились к набору!")
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

            if len(squad_applications) < 2:
                await callback.answer(" Недостаточно участников для начала выполнения заказа! Минимум: 2")
                await show_order_participants_menu(callback.message, order_db_id, order[0])
                return
            
            if len(squad_applications) > 4:
                await callback.answer(" Слишком много участников! Максимум: 4")
                await show_order_participants_menu(callback.message, order_db_id, order[0])
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
            f"заказ {memo_order_id} завершен\n"
            f"юзер - {user_id} - {pubg_id or 'не указан'}",
            reply_markup=None
        )
        await notify_admins(
            f"заказ {memo_order_id} завершен\n"
            f"юзер - {user_id} - {pubg_id or 'не указан'}"
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

@dp.message(F.text == "админ-панель")
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

@dp.message(F.text == "сквады")
async def squads_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'squads_submenu'
        await message.answer(" Управление сквадами:", reply_markup=get_squads_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в squads_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "сопровождающие")
async def escorts_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'escorts_submenu'
        await message.answer(" Управление сопровождающими:", reply_markup=get_escorts_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в escorts_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

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

@dp.message(F.text == "баны/ограничения")
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

@dp.message(F.text == "балансы")
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

@dp.message(F.text == "пользователи")
async def users_submenu_handler(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'users_submenu'
        await message.answer("Управление пользователями:", reply_markup=get_users_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в users_submenu_handler для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "список пользователей")
async def users_list_handler(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id, s.name as squad_name, 
                       e.balance, e.completed_orders, e.rating
                FROM escorts e
                LEFT JOIN squads s ON e.squad_id = s.id
                ORDER BY e.username
                '''
            )
            users = await cursor.fetchall()
        
        if not users:
            await message.answer("Пользователи не найдены", reply_markup=get_users_submenu_keyboard())
            return
        
        response = "Список всех пользователей:\n\n"
        for i, (tg_id, username, pubg_id, squad_name, balance, orders, rating) in enumerate(users, 1):
            username_display = f"@{username}" if username else "Не указан"
            pubg_display = pubg_id if pubg_id else "Не указан"
            squad_display = squad_name if squad_name else "Без команды"
            rating_display = f"{rating:.1f}" if rating else "0.0"
            
            response += (
                f"{i}. {username_display}\n"
                f"   ID: {tg_id}\n"
                f"   PUBG: {pubg_display}\n"
                f"   Команда: {squad_display}\n"
                f"   Баланс: {balance:.2f} руб\n"
                f"   Заказы: {orders}, Рейтинг: {rating_display}\n\n"
            )
        
        # Разбиваем длинное сообщение на части если нужно
        max_length = 4000
        if len(response) > max_length:
            parts = []
            current_part = "Список всех пользователей:\n\n"
            lines = response.split('\n')[2:]  # Skip the header
            
            for line in lines:
                if len(current_part + line + '\n') > max_length:
                    parts.append(current_part)
                    current_part = line + '\n'
                else:
                    current_part += line + '\n'
            
            if current_part.strip():
                parts.append(current_part)
            
            for part in parts:
                await message.answer(part, reply_markup=get_users_submenu_keyboard() if part == parts[-1] else None)
        else:
            await message.answer(response, reply_markup=get_users_submenu_keyboard())
            
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в users_list_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())


@dp.message(F.text == "управление лидерами")
async def leaders_submenu(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'leaders_submenu'
        await message.answer(" Управление лидерами:", reply_markup=get_leaders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в leaders_submenu для {user_id}: {e}\n\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "связь")
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

@dp.message(F.text == "связаться с пользователем")
async def contact_user_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите Telegram ID пользователя для связи:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.contact_user_id)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в contact_user_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_communication_submenu_keyboard())

@dp.message(Form.contact_user_id)
async def process_contact_user_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Обработка кнопки отмены
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_communication_submenu_keyboard())
        await state.clear()
        return
    
    # Обработка обычных кнопок меню - очищаем состояние и перенаправляем
    if message.text in ["заказы", "личный кабинет", "админ-панель", "назад", "поддержка", "информация", "найти команду", "рейтинг пользователей", "рейтинг сквадов", "получить выплату"]:
        await state.clear()
        user_context[user_id] = 'main_menu'
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        return
    
    try:
        target_user_id = int(message.text.strip())
        await state.update_data(target_user_id=target_user_id)
        await message.answer("Введите сообщение для пользователя:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.contact_user_message)
    except ValueError:
        await message.answer("Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))

@dp.message(Form.contact_user_message)
async def process_contact_user_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_communication_submenu_keyboard())
        await state.clear()
        return
    
    contact_message = message.text.strip()
    if not contact_message:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        return
    
    try:
        data = await state.get_data()
        target_user_id = data.get('target_user_id')
        
        if not target_user_id:
            await message.answer(" Ошибка: не найден получатель сообщения.", reply_markup=get_communication_submenu_keyboard())
            await state.clear()
            return
        
        # Отправляем сообщение пользователю
        try:
            await bot.send_message(
                target_user_id,
                contact_message
            )
            await message.answer(" Сообщение отправлено пользователю!", reply_markup=get_communication_submenu_keyboard())
        except TelegramAPIError:
            await message.answer(" Не удалось отправить сообщение пользователю.", reply_markup=get_communication_submenu_keyboard())
        
        await log_action("contact_user", user_id, None, f"Сообщение пользователю {target_user_id}: {contact_message}")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_contact_user_message для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_communication_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "сделать объявление")
async def broadcast_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("📢 Введите текст объявления для всех пользователей:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.broadcast_message)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в broadcast_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_communication_submenu_keyboard())

@dp.message(Form.broadcast_message)
async def process_broadcast_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_communication_submenu_keyboard())
        await state.clear()
        return
    
    broadcast_text = message.text.strip()
    if not broadcast_text:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
        return
    
    try:
        # Получаем всех пользователей
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id FROM escorts")
            users = await cursor.fetchall()
        
        sent_count = 0
        failed_count = 0
        
        for (telegram_id,) in users:
            try:
                await bot.send_message(telegram_id, broadcast_text)
                sent_count += 1
            except TelegramAPIError:
                failed_count += 1
        
        await message.answer(
            f"📢 Объявление отправлено!\n"
            f" Успешно: {sent_count}\n"
            f" Не удалось: {failed_count}",
            reply_markup=get_communication_submenu_keyboard()
        )
        
        await log_action("broadcast", user_id, None, f"Объявление отправлено {sent_count} пользователям")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_broadcast_message для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_communication_submenu_keyboard())
        await state.clear()


@dp.message(F.text == "выйти из админ-панели")
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
        payout_method = parts[4] if len(parts) > 4 else "unknown"
        
        method_text = "UC" if payout_method == "uc" else "банковскую карту" if payout_method == "card" else "неизвестный способ"

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username, balance FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await callback.message.edit_text(" Пользователь не найден.")
                await callback.answer()
                return

            username, balance = user
            if balance < payout_amount:
                await callback.message.edit_text(f" Недостаточно средств на балансе. Доступно: {balance:.2f} руб.")
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
                f" Ваша выплата на сумму {payout_amount:.2f} руб. через {method_text} одобрена и выплачена!"
            )
        except TelegramAPIError:
            pass

        await callback.message.edit_text(
            f" Выплата одобрена:\n"
            f" @{username or 'Unknown'} (ID: {target_user_id})\n"
            f" Сумма: {payout_amount:.2f} руб.\n"
            f"💳 Способ: {method_text}"
        )

        await log_action(
            "approve_payout",
            user_id,
            None,
            f"Одобрена выплата {payout_amount:.2f} руб. через {method_text} для @{username or 'Unknown'}"
        )
        await callback.answer()

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка в approve_payout для {user_id}: {e}")
        await callback.answer("Произошла ошибка")
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в approve_payout для {user_id}: {e}")
        await callback.answer("Произошла ошибка")

@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        if await check_subscription(user_id):
            await callback.message.edit_text(" Спасибо за подписку! Теперь вы можете пользоваться ботом.")
            await callback.message.answer(f"{MESSAGES['welcome']}\n\nВыберите действие:", reply_markup=await get_menu_keyboard(user_id))
        else:
            await callback.answer(" Вы еще не подписались на канал!")
    except Exception as e:
        logger.error(f"Ошибка в check_subscription_callback для {user_id}: {e}")
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
        payout_method = parts[4] if len(parts) > 4 else "unknown"
        
        method_text = "UC" if payout_method == "uc" else "банковскую карту" if payout_method == "card" else "неизвестный способ"

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user = await cursor.fetchone()
            if not user:
                await callback.message.edit_text(" Пользователь не найден.")
                await callback.answer()
                return

            username = user[0]

        # Уведомляем пользователя
        try:
            await bot.send_message(
                target_user_id,
                f" Ваша выплата на сумму {payout_amount:.2f} руб. через {method_text} отклонена администратором."
            )
        except TelegramAPIError:
            pass

        await callback.message.edit_text(
            f" Выплата отклонена:\n"
            f" @{username or 'Unknown'} (ID: {target_user_id})\n"
            f" Сумма: {payout_amount:.2f} руб.\n"
            f"💳 Способ: {method_text}"
        )

        await log_action(
            "reject_payout",
            user_id,
            None,
            f"Отклонена выплата {payout_amount:.2f} руб. через {method_text} для @{username or 'Unknown'}"
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
            await callback.message.edit_text(" Информация о лидере не найдена.")
            await callback.answer()
            return

        leader_username, squad_name = leader_info

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="связаться с лидером", callback_data=f"contact_leader_{leader_telegram_id}")]
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

    except (ValueError, TelegramAPIError) as e:
        logger.error(f"Ошибка в contact_leader_callback для {user_id}: {e}\n\n{traceback.format_exc()}")
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
    if message.text == "отмена":
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
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
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
            await message.answer(" Ошибка: не найден получатель сообщения.", reply_markup=await get_menu_keyboard(user_id))
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
            await message.answer(" Ваше сообщение отправлено лидеру!", reply_markup=await get_menu_keyboard(user_id))
        except TelegramAPIError:
            await message.answer(" Не удалось отправить сообщение лидеру.", reply_markup=await get_menu_keyboard(user_id))

        await log_action("contact_leader", user_id, None, f"Сообщение лидеру {target_leader_id}: {contact_message}")
        await state.clear()

    except Exception as e:
        logger.error(f"Ошибка в process_contact_leader_message для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

# --- Обработчики для управления лидерами ---

@dp.message(F.text == "добавить лидера команды")
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
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
        return
    try:
        leader_telegram_id = int(message.text.strip())
        
        # Получаем информацию о пользователе из Telegram API
        try:
            user_info = await bot.get_chat(leader_telegram_id)
            username = user_info.username
        except Exception:
            username = "Unknown"
            
        escort_record = await find_or_create_user(leader_telegram_id, username)
        if not escort_record:
            await message.answer(f" Не удалось получить информацию о пользователе с Telegram ID {leader_telegram_id}.", reply_markup=get_cancel_keyboard(True))
            return
            
        escort_id = escort_record[0]  # ID пользователя
        current_squad_id = escort_record[1]  # squad_id
        
        async with aiosqlite.connect(DB_PATH) as conn:
            # Проверяем, не является ли пользователь уже лидером
            cursor = await conn.execute("SELECT squad_id FROM squad_leaders WHERE leader_id = ?", (escort_id,))
            existing_leader = await cursor.fetchone()
            if existing_leader:
                await message.answer(" Этот пользователь уже является лидером.", reply_markup=get_cancel_keyboard(True))
                return
            
            # Если пользователь уже в скваде, делаем его лидером этого сквада
            if current_squad_id:
                # Назначаем пользователя лидером его текущего сквада
                await conn.execute("INSERT INTO squad_leaders (leader_id, squad_id) VALUES (?, ?)", (escort_id, current_squad_id))
                await conn.commit()
                
                # Получаем название сквада
                cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (current_squad_id,))
                squad_result = await cursor.fetchone()
                squad_name = squad_result[0] if squad_result else "Unknown"
                
                await message.answer(f" Пользователь {leader_telegram_id} назначен лидером сквада '{squad_name}'!", reply_markup=get_leaders_submenu_keyboard())
                await log_action("add_leader", user_id, None, f"Назначен лидер {leader_telegram_id} для сквада '{squad_name}'")
                await state.clear()
            else:
                # Если пользователь не в скваде, предлагаем создать новый
                await state.update_data(leader_telegram_id=leader_telegram_id)
                await message.answer("Пользователь не состоит в скваде. Введите название нового сквада:", reply_markup=get_cancel_keyboard(True))
                await state.set_state(Form.leader_squad_name)
                
    except ValueError:
        await message.answer(" Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_leader_user_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()

@dp.message(Form.leader_squad_name)
async def process_leader_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer(" Название сквада не может быть пустым.", reply_markup=get_cancel_keyboard(True))
        return

    try:
        data = await state.get_data()
        leader_telegram_id = data.get('leader_telegram_id')

        async with aiosqlite.connect(DB_PATH) as conn:
            # Проверяем, существует ли пользователь
            cursor = await conn.execute("SELECT id FROM escorts WHERE telegram_id = ?", (leader_telegram_id,))
            escort_record = await cursor.fetchone()
            if not escort_record:
                await message.answer(f" Пользователь с Telegram ID {leader_telegram_id} не найден.", reply_markup=get_admin_keyboard())
                await state.clear()
                return
            escort_id = escort_record[0]

            # Проверяем, не является ли пользователь уже лидером
            cursor = await conn.execute("SELECT squad_id FROM squad_leaders WHERE leader_id = ?", (escort_id,))
            existing_leader = await cursor.fetchone()
            if existing_leader:
                await message.answer(" Этот пользователь уже является лидером.", reply_markup=get_admin_keyboard())
                await state.clear()
                return

            # Создаем новый сквад
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            squad_id = cursor.lastrowid
            if squad_id is None:
                cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
                result = await cursor.fetchone()
                squad_id = result[0] if result else None

            # Назначаем пользователя лидером
            await conn.execute("INSERT INTO squad_leaders (leader_id, squad_id) VALUES (?, ?)", (escort_id, squad_id))

            # Обновляем информацию о пользователе (связываем с новым сквадом)
            await conn.execute("UPDATE escorts SET squad_id = ? WHERE id = ?", (squad_id, escort_id))

            await conn.commit()

        await message.answer(f" Пользователь {leader_telegram_id} назначен лидером сквада '{squad_name}'!", reply_markup=get_admin_keyboard())
        await log_action("add_leader", user_id, None, f"Назначен лидер {leader_telegram_id} для сквада '{squad_name}'")
        await state.clear()

    except ValueError:
        await message.answer(" Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_leader_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_leader_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "список лидеров")
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
            await message.answer(" Список лидеров пуст.", reply_markup=get_leaders_submenu_keyboard())
            return

        response = " Список лидеров:\n\n"
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

@dp.message(F.text == "убрать лидера")
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
            await message.answer(" Нет назначенных лидеров для удаления.", reply_markup=get_leaders_submenu_keyboard())
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
    if message.text == "отмена":
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
            await message.answer(" Пользователь с таким Telegram ID не найден в списке лидеров.", reply_markup=get_cancel_keyboard(True))
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
                await message.answer(" Не удалось получить информацию о лидере.", reply_markup=get_leaders_submenu_keyboard())
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

        await message.answer(f" Лидер @{leader_username or 'Unknown'} (ID: {target_telegram_id}) удален, сквад '{squad_name}' расформирован.", reply_markup=get_leaders_submenu_keyboard())
        await log_action("remove_leader", user_id, None, f"Удален лидер {target_telegram_id} (сквад: {squad_name})")
        await state.clear()

    except ValueError:
        await message.answer(" Неверный формат Telegram ID. Введите числовое значение:", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в process_remove_leader_user_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в process_remove_leader_user_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "написать лидеру")
async def admin_contact_leader(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, s.name
                FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                JOIN squads s ON sl.squad_id = s.id
                ORDER BY s.name
                '''
            )
            leaders = await cursor.fetchall()

        if not leaders:
            await message.answer(" Нет доступных лидеров для связи.", reply_markup=get_leaders_submenu_keyboard())
            return

        keyboard_buttons = []
        for telegram_id, username, squad_name in leaders:
            button_text = f"@{username or 'Unknown'} ({squad_name})"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"select_leader_{telegram_id}")])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        await message.answer(" Выберите лидера для связи:", reply_markup=keyboard)

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в admin_contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в admin_contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_leaders_submenu_keyboard())

@dp.message(F.text == "управление участниками")
async def members_management_menu(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'members_management'
        await message.answer(" Управление участниками сквада:", reply_markup=get_members_management_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка в members_management_menu для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "управление сквадом")
async def squad_management_menu(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        user_context[user_id] = 'squad_management'
        await message.answer(" Управление сквадом:", reply_markup=get_squad_management_keyboard())
    except TelegramAPIError as e:
        logger.error(f"Ошибка в squad_management_menu для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "добавить участника")
async def add_member_handler(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer("У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("Введите Telegram ID пользователя для добавления в сквад:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.add_member_telegram_id)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в add_member_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_members_management_keyboard())

@dp.message(Form.add_member_telegram_id)
async def process_add_member_telegram_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Обработка кнопки отмены
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_members_management_keyboard())
        await state.clear()
        return
    
    # Обработка обычных кнопок меню - очищаем состояние и перенаправляем
    if message.text in ["заказы", "личный кабинет", "админ-панель", "назад", "поддержка", "информация", "найти команду", "рейтинг пользователей", "рейтинг сквадов", "получить выплату", "управление участниками", "управление сквадом"]:
        await state.clear()
        user_context[user_id] = 'main_menu'
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        # Проверяем, что лидер существует и имеет сквад
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_squad = await cursor.fetchone()
            if not leader_squad:
                await message.answer("Вы не являетесь лидером сквада.", reply_markup=get_members_management_keyboard())
                await state.clear()
                return
            
            squad_id, squad_name = leader_squad
            
            # Проверяем текущее количество участников
            cursor = await conn.execute("SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad_id,))
            current_count = (await cursor.fetchone())[0]
            
            if current_count >= 10:
                await message.answer("В скваде уже максимальное количество участников (10).", reply_markup=get_cancel_keyboard())
                return
            
            # Проверяем, существует ли пользователь (создаем если нет)
            user_data = await find_or_create_user(target_user_id)
            if not user_data:
                await message.answer("Не удалось получить информацию о пользователе.", reply_markup=get_cancel_keyboard())
                return
            
            escort_id, current_squad_id, _, _, _, _, username = user_data[:7]
            
            if current_squad_id == squad_id:
                await message.answer("Пользователь уже состоит в вашем скваде.", reply_markup=get_cancel_keyboard())
                return
            
            if current_squad_id:
                await message.answer("Пользователь уже состоит в другом скваде.", reply_markup=get_cancel_keyboard())
                return
        
        # Сохраняем данные и переходим к следующему шагу
        await state.update_data(target_user_id=target_user_id, squad_id=squad_id, squad_name=squad_name, escort_id=escort_id, username=username)
        await message.answer("Введите PUBG ID участника:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.add_member_pubg_id)
            
    except ValueError:
        await message.answer("Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в process_add_member_telegram_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_members_management_keyboard())
        await state.clear()

@dp.message(Form.add_member_pubg_id)
async def process_add_member_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_members_management_keyboard())
        await state.clear()
        return
    
    pubg_id = message.text.strip()
    if not pubg_id:
        await message.answer("Введите PUBG ID участника:", reply_markup=get_cancel_keyboard())
        return
    
    # Сохраняем PUBG ID и переходим к следующему шагу
    await state.update_data(pubg_id=pubg_id)
    await message.answer("Введите город участника:", reply_markup=get_cancel_keyboard())
    await state.set_state(Form.add_member_city)

@dp.message(Form.add_member_city)
async def process_add_member_city(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_members_management_keyboard())
        await state.clear()
        return
    
    city = message.text.strip()
    if not city:
        await message.answer("Введите город участника:", reply_markup=get_cancel_keyboard())
        return
    
    # Сохраняем город и переходим к следующему шагу
    await state.update_data(city=city)
    await message.answer("Введите КД (коэффициент добычи) участника:", reply_markup=get_cancel_keyboard())
    await state.set_state(Form.add_member_cd)

@dp.message(Form.add_member_cd)
async def process_add_member_cd(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_members_management_keyboard())
        await state.clear()
        return
    
    cd = message.text.strip()
    if not cd:
        await message.answer("Введите КД участника:", reply_markup=get_cancel_keyboard())
        return
    
    # Сохраняем КД и переходим к следующему шагу
    await state.update_data(cd=cd)
    await message.answer("Введите возраст участника:", reply_markup=get_cancel_keyboard())
    await state.set_state(Form.add_member_age)

@dp.message(Form.add_member_age)
async def process_add_member_age(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_members_management_keyboard())
        await state.clear()
        return
    
    age = message.text.strip()
    if not age:
        await message.answer("Введите возраст участника:", reply_markup=get_cancel_keyboard())
        return
    
    try:
        # Получаем все сохраненные данные
        data = await state.get_data()
        target_user_id = data['target_user_id']
        squad_id = data['squad_id']
        squad_name = data['squad_name']
        escort_id = data['escort_id']
        username = data['username']
        pubg_id = data['pubg_id']
        city = data['city']
        cd = data['cd']
        
        # Добавляем пользователя в сквад и сохраняем информацию
        async with aiosqlite.connect(DB_PATH) as conn:
            # Обновляем основную информацию пользователя
            await conn.execute(
                "UPDATE escorts SET squad_id = ?, pubg_id = ? WHERE id = ?", 
                (squad_id, pubg_id, escort_id)
            )
            
            # Создаем запись в squad_applications с данными, введенными лидером
            await conn.execute(
                """INSERT INTO squad_applications (user_id, squad_id, city, pubg_id, cd, age, status) 
                   VALUES (?, ?, ?, ?, ?, ?, 'approved')""",
                (escort_id, squad_id, city, pubg_id, cd, age)
            )
            await conn.commit()
        
        await message.answer(f"Пользователь @{username or 'Unknown'} добавлен в сквад '{squad_name}'!", reply_markup=get_members_management_keyboard())
        await log_action("add_member", user_id, description=f"Добавлен участник {target_user_id} в сквад {squad_name}")
        await state.clear()
        
        # Уведомляем добавленного пользователя
        try:
            await bot.send_message(target_user_id, f"Вы добавлены в сквад '{squad_name}'!")
        except TelegramAPIError:
            pass
            
    except Exception as e:
        logger.error(f"Ошибка в process_add_member_age для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_members_management_keyboard())
        await state.clear()

@dp.message(F.text == "удалить участника")
async def remove_member_handler(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите Telegram ID участника для удаления из сквада:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.remove_member)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в remove_member_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_members_management_keyboard())

@dp.message(Form.remove_member)
async def process_remove_member(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_members_management_keyboard())
        await state.clear()
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        # Получаем информацию о лидере и его сквад
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_squad = await cursor.fetchone()
            if not leader_squad:
                await message.answer(" Вы не являетесь лидером сквада.", reply_markup=get_members_management_keyboard())
                await state.clear()
                return
            
            squad_id, squad_name = leader_squad
            
            # Проверяем, является ли пользователь участником сквада
            cursor = await conn.execute("SELECT id, username FROM escorts WHERE telegram_id = ? AND squad_id = ?", (target_user_id, squad_id))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не состоит в вашем скваде.", reply_markup=get_cancel_keyboard())
                return
            
            escort_id, username = user_data
            
            # Нельзя удалить самого себя
            if target_user_id == user_id:
                await message.answer(" Вы не можете удалить себя из сквада.", reply_markup=get_cancel_keyboard())
                return
            
            # Удаляем пользователя из сквада
            await conn.execute("UPDATE escorts SET squad_id = NULL WHERE id = ?", (escort_id,))
            await conn.commit()
        
        await message.answer(f" Пользователь @{username or 'Unknown'} удален из сквада '{squad_name}'!", reply_markup=get_members_management_keyboard())
        await log_action("remove_member", user_id, None, f"Удален участник {target_user_id} из сквада {squad_name}")
        await state.clear()
        
        # Уведомляем удаленного пользователя
        try:
            await bot.send_message(target_user_id, f" Вы исключены из сквада '{squad_name}'.")
        except TelegramAPIError:
            pass
            
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_remove_member для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_members_management_keyboard())
        await state.clear()

@dp.message(F.text == "список участников")
async def list_members(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id, e.completed_orders, e.balance, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_info = await cursor.fetchone()
            if not leader_info:
                await message.answer(" Вы не являетесь лидером сквада.", reply_markup=get_members_management_keyboard())
                return
            
            squad_name = leader_info[5]
            
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id, e.completed_orders, e.balance
                FROM squad_leaders sl
                JOIN escorts e2 ON sl.leader_id = e2.id
                JOIN escorts e ON e.squad_id = sl.squad_id
                WHERE e2.telegram_id = ?
                ORDER BY e.username
                ''', (user_id,)
            )
            members = await cursor.fetchall()
        
        if not members:
            await message.answer(f" В скваде '{squad_name}' пока нет участников.", reply_markup=get_members_management_keyboard())
            return
        
        response = f" Участники сквада '{squad_name}':\n\n"
        for telegram_id, username, pubg_id, completed_orders, balance in members:
            role = " (Лидер)" if telegram_id == user_id else ""
            response += f" @{username or 'Unknown'} (ID: {telegram_id}){role}\n"
            response += f" PUBG ID: {pubg_id or 'не указан'}\n"
            response += f" Заказов: {completed_orders}\n"
            response += f" Баланс: {balance:.2f} руб.\n\n"
        
        await message.answer(response, reply_markup=get_members_management_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в list_members для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_members_management_keyboard())

@dp.message(F.text == "переименовать сквад")
async def rename_squad_handler(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите новое название сквада:", reply_markup=get_cancel_keyboard())
        await state.set_state(Form.rename_squad)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в rename_squad_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squad_management_keyboard())

@dp.message(Form.rename_squad)
async def process_rename_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_squad_management_keyboard())
        await state.clear()
        return
    
    new_name = message.text.strip()
    if not new_name:
        await message.answer(" Название сквада не может быть пустым.", reply_markup=get_cancel_keyboard())
        return
    
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_squad = await cursor.fetchone()
            if not leader_squad:
                await message.answer(" Вы не являетесь лидером сквада.", reply_markup=get_squad_management_keyboard())
                await state.clear()
                return
            
            squad_id, old_name = leader_squad
            
            await conn.execute("UPDATE squads SET name = ? WHERE id = ?", (new_name, squad_id))
            await conn.commit()
        
        await message.answer(f" Сквад '{old_name}' переименован в '{new_name}'!", reply_markup=get_squad_management_keyboard())
        await log_action("rename_squad", user_id, None, f"Переименован сквад '{old_name}' в '{new_name}'")
        await state.clear()
        
    except aiosqlite.IntegrityError:
        await message.answer(" Сквад с таким названием уже существует.", reply_markup=get_cancel_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_rename_squad для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squad_management_keyboard())
        await state.clear()

@dp.message(F.text == "критерии команды")
async def set_criteria_handler(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        # Получаем текущие критерии
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sc.criteria_text
                FROM squad_leaders sl
                JOIN squad_criteria sc ON sl.squad_id = sc.squad_id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            current_criteria = await cursor.fetchone()
        
        current_text = current_criteria[0] if current_criteria else "Критерии не установлены"
        
        await message.answer(
            f"⚙️ Текущие критерии для вступления в команду:\n\n{current_text}\n\n"
            f"Введите новые критерии для вступления в вашу команду:",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(Form.set_criteria)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в set_criteria_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squad_management_keyboard())

@dp.message(Form.set_criteria)
async def process_set_criteria(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_squad_management_keyboard())
        await state.clear()
        return
    
    criteria_text = message.text.strip()
    if not criteria_text:
        await message.answer(" Критерии не могут быть пустыми.", reply_markup=get_cancel_keyboard())
        return
    
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем squad_id лидера
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_squad = await cursor.fetchone()
            if not leader_squad:
                await message.answer(" Вы не являетесь лидером сквада.", reply_markup=get_squad_management_keyboard())
                await state.clear()
                return
            
            squad_id, squad_name = leader_squad
            
            # Сохраняем или обновляем критерии
            await conn.execute(
                '''
                INSERT OR REPLACE INTO squad_criteria (squad_id, criteria_text, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (squad_id, criteria_text)
            )
            await conn.commit()
        
        await message.answer(
            f" Критерии для команды '{squad_name}' успешно обновлены!",
            reply_markup=get_squad_management_keyboard()
        )
        await log_action("set_criteria", user_id, None, f"Обновлены критерии для команды '{squad_name}'")
        await state.clear()
        
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_set_criteria для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squad_management_keyboard())
        await state.clear()

@dp.message(F.text == "список заказов")
async def squad_orders_list(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_squad = await cursor.fetchone()
            if not leader_squad:
                await message.answer(" Вы не являетесь лидером сквада.", reply_markup=get_squad_management_keyboard())
                return
            
            squad_id, squad_name = leader_squad
            
            cursor = await conn.execute(
                '''
                SELECT o.memo_order_id, o.customer_info, o.amount, o.status, o.created_at, o.completed_at
                FROM orders o
                WHERE o.squad_id = ?
                ORDER BY o.created_at DESC
                LIMIT 10
                ''', (squad_id,)
            )
            orders = await cursor.fetchall()
        
        if not orders:
            await message.answer(f" У сквада '{squad_name}' пока нет заказов.", reply_markup=get_squad_management_keyboard())
            return
        
        response = f" Последние заказы сквада '{squad_name}':\n\n"
        for memo_order_id, customer_info, amount, status, created_at, completed_at in orders:
            status_text = {
                'pending': '⏳ Ожидает',
                'in_progress': '🔄 В процессе', 
                'completed': ' Завершен'
            }.get(status, status)
            
            response += f"#{memo_order_id} - {customer_info}\n"
            response += f" {amount:.2f} руб. | {status_text}\n"
            if completed_at:
                response += f" Завершен: {datetime.fromisoformat(completed_at).strftime('%d.%m %H:%M')}\n"
            response += "\n"
        
        await message.answer(response, reply_markup=get_squad_management_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в squad_orders_list для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squad_management_keyboard())

@dp.message(F.text == "статус сопровождений")
async def escort_status_handler(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    if not await is_leader(user_id):
        await message.answer(" У вас нет доступа к этой функции.", reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT sl.squad_id, s.name
                FROM squad_leaders sl
                JOIN squads s ON sl.squad_id = s.id
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            leader_squad = await cursor.fetchone()
            if not leader_squad:
                await message.answer(" Вы не являетесь лидером сквада.", reply_markup=get_squad_management_keyboard())
                return
            
            squad_id, squad_name = leader_squad
            
            # Получаем все активные заказы команды (в процессе выполнения)
            cursor = await conn.execute(
                '''
                SELECT o.id, o.memo_order_id, o.customer_info, o.amount, o.created_at
                FROM orders o
                WHERE o.squad_id = ? AND o.status = 'in_progress'
                ORDER BY o.created_at ASC
                ''', (squad_id,)
            )
            active_orders = await cursor.fetchall()
        
        if not active_orders:
            await message.answer(f"📊 У команды '{squad_name}' сейчас нет активных сопровождений.", reply_markup=get_squad_management_keyboard())
            return
        
        response = f"📊 Статус сопровождений команды '{squad_name}':\n\n"
        current_time = datetime.now()
        
        for order_id, memo_order_id, customer_info, amount, created_at in active_orders:
            # Парсим время создания заказа
            try:
                start_time = datetime.fromisoformat(created_at)
                duration = current_time - start_time
                
                # Форматируем длительность
                hours = int(duration.total_seconds() // 3600)
                minutes = int((duration.total_seconds() % 3600) // 60)
                duration_text = f"{hours}ч {minutes}м"
                
                start_time_text = start_time.strftime('%d.%m %H:%M')
            except (ValueError, TypeError):
                duration_text = "неизвестно"
                start_time_text = "неизвестно"
            
            # Получаем участников заказа
            async with aiosqlite.connect(DB_PATH) as conn:
                cursor = await conn.execute(
                    '''
                    SELECT e.username, e.telegram_id, oe.pubg_id
                    FROM order_escorts oe
                    JOIN escorts e ON oe.escort_id = e.id
                    WHERE oe.order_id = ?
                    ORDER BY e.username
                    ''', (order_id,)
                )
                participants = await cursor.fetchall()
            
            response += f"🔄 Заказ #{memo_order_id}\n"
            response += f" Клиент: {customer_info}\n"
            response += f" Сумма: {amount:.2f} руб.\n"
            response += f"🕐 Начат: {start_time_text}\n"
            response += f"⏱ Длительность: {duration_text}\n"
            response += f" Участники ({len(participants)}):\n"
            
            for username, telegram_id, pubg_id in participants:
                response += f"   • @{username or 'Unknown'} (PUBG: {pubg_id or 'не указан'})\n"
            
            response += "\n"
        
        await message.answer(response, reply_markup=get_squad_management_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в escort_status_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squad_management_keyboard())

@dp.message(F.text == "связаться с лидером")
async def user_contact_leader(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort or not escort[1]:  # squad_id
            await message.answer(" Вы не состоите в скваде.", reply_markup=await get_menu_keyboard(user_id))
            return

        squad_id = escort[1]

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username
                FROM escorts e
                JOIN squad_leaders sl ON e.id = sl.leader_id
                WHERE sl.squad_id = ?
                ''', (squad_id,)
            )
            leader = await cursor.fetchone()

        if not leader:
            await message.answer(" У вашего сквада нет назначенного лидера.", reply_markup=await get_menu_keyboard(user_id))
            return

        leader_telegram_id, leader_username = leader

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="отправить сообщение", callback_data=f"contact_leader_{leader_telegram_id}")]
        ])

        await message.answer(
            f"👨‍💼 Ваш лидер: @{leader_username or 'Unknown'}\n"
            f"ID: {leader_telegram_id}",
            reply_markup=keyboard
        )

    except aiosqlite.Error as e:
        logger.error(f"Ошибка базы данных в user_contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка Telegram API в user_contact_leader для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))


# --- Недостающие обработчики кнопок ---

@dp.message(F.text == "назад")
async def back_button(message: types.Message):
    user_id = message.from_user.id
    try:
        context = user_context.get(user_id, 'main_menu')
        
        # Для админских разделов возвращаем в админ-панель
        if context in ['squads_submenu', 'escorts_submenu', 'admin_orders_submenu', 
                      'bans_submenu', 'balances_submenu', 'users_submenu', 
                      'leaders_submenu', 'communication_submenu']:
            if is_admin(user_id):
                user_context[user_id] = 'admin_panel'
                await message.answer("🚪 Админ-панель:", reply_markup=get_admin_keyboard())
            else:
                user_context[user_id] = 'main_menu'
                await message.answer("📌 Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        # Из разделов заказов, личного кабинета и управления сквадом возвращаем в главное меню
        elif context in ['orders_submenu', 'personal_cabinet', 'squad_management', 'members_management']:
            user_context[user_id] = 'main_menu'
            await message.answer("📌 Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        # Из админ-панели выходим в главное меню
        elif context == 'admin_panel':
            user_context[user_id] = 'main_menu'
            await message.answer("📌 Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        else:
            user_context[user_id] = 'main_menu'
            await message.answer("📌 Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка в back_button для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "отмена")
async def cancel_button(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        # Очищаем состояние FSM и возвращаем в главное меню
        await state.clear()
        user_context[user_id] = 'main_menu'
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка в cancel_button для {user_id}: {e}")
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "добавить сквад")
async def add_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите название нового сквада:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.squad_name)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в add_squad для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_submenu_keyboard())

@dp.message(Form.squad_name)
async def process_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_squads_submenu_keyboard())
        await state.clear()
        return
    
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer(" Название сквада не может быть пустым.", reply_markup=get_cancel_keyboard(True))
        return
    
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        
        await message.answer(f" Сквад '{squad_name}' успешно создан!", reply_markup=get_squads_submenu_keyboard())
        await log_action("add_squad", user_id, None, f"Создан сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.IntegrityError:
        await message.answer(" Сквад с таким названием уже существует.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "список сквадов")
async def list_squads(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT s.name, COUNT(e.id) as member_count, 
                       COALESCE(AVG(e.rating), 0) as avg_rating,
                       COALESCE(SUM(e.completed_orders), 0) as total_orders
                FROM squads s
                LEFT JOIN escorts e ON s.id = e.squad_id
                GROUP BY s.id, s.name
                ORDER BY s.name
                '''
            )
            squads = await cursor.fetchall()
        
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_squads_submenu_keyboard())
            return
        
        response = " Список сквадов:\n\n"
        for name, member_count, avg_rating, total_orders in squads:
            response += f" {name}\n"
            response += f" Участников: {member_count}\n"
            response += f"Рейтинг: {avg_rating:.2f}\n"
            response += f" Заказов: {total_orders}\n\n"
        
        await message.answer(response, reply_markup=get_squads_submenu_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в list_squads для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_submenu_keyboard())

@dp.message(F.text == "расформировать сквад")
async def delete_squad_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads ORDER BY name")
            squads = await cursor.fetchall()
        
        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_squads_submenu_keyboard())
            return
        
        response = "🗑️ Выберите сквад для расформирования:\n\n"
        for (name,) in squads:
            response += f"• {name}\n"
        response += "\nВведите точное название сквада:"
        
        await message.answer(response, reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.delete_squad)
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в delete_squad_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_submenu_keyboard())

@dp.message(Form.delete_squad)
async def process_delete_squad(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_squads_submenu_keyboard())
        await state.clear()
        return
    
    squad_name = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(" Сквад не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            squad_id = squad[0]
            await conn.execute("UPDATE escorts SET squad_id = NULL WHERE squad_id = ?", (squad_id,))
            await conn.execute("DELETE FROM squad_leaders WHERE squad_id = ?", (squad_id,))
            await conn.execute("DELETE FROM squads WHERE id = ?", (squad_id,))
            await conn.commit()
        
        await message.answer(MESSAGES["squad_deleted"].format(squad_name=squad_name), reply_markup=get_squads_submenu_keyboard())
        await log_action("delete_squad", user_id, None, f"Расформирован сквад '{squad_name}'")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_delete_squad для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_squads_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "добавить сопровождающего")
async def add_escort_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("Введите информацию о сопровождающем в формате:\nTelegram ID:Username:PUBG ID:Название сквада", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.escort_info)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в add_escort_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_submenu_keyboard())

@dp.message(Form.escort_info)
async def process_escort_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_escorts_submenu_keyboard())
        await state.clear()
        return
    
    try:
        parts = message.text.strip().split(":")
        if len(parts) != 4:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        
        telegram_id_str, username, pubg_id, squad_name = [part.strip() for part in parts]
        telegram_id = int(telegram_id_str)
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f" Сквад '{squad_name}' не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            squad_id = squad[0]
            
            await conn.execute(
                '''
                INSERT OR REPLACE INTO escorts (telegram_id, username, pubg_id, squad_id, rules_accepted)
                VALUES (?, ?, ?, ?, 1)
                ''', (telegram_id, username, pubg_id, squad_id)
            )
            await conn.commit()
        
        await message.answer(f"Сопровождающий @{username} добавлен в сквад '{squad_name}'!", reply_markup=get_escorts_submenu_keyboard())
        await log_action("add_escort", user_id, None, f"Добавлен сопровождающий @{username} в сквад '{squad_name}'")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_escort_info для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "удалить сопровождающего")
async def remove_escort_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("🗑️ Введите Telegram ID сопровождающего для удаления:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.remove_escort)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в remove_escort_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_submenu_keyboard())

@dp.message(Form.remove_escort)
async def process_remove_escort(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_escorts_submenu_keyboard())
        await state.clear()
        return
    
    try:
        target_telegram_id = int(message.text.strip())
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_telegram_id,))
            escort = await cursor.fetchone()
            if not escort:
                await message.answer(" Сопровождающий не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = escort[0]
            await conn.execute("DELETE FROM escorts WHERE telegram_id = ?", (target_telegram_id,))
            await conn.commit()
        
        await message.answer(f" Сопровождающий @{username or 'Unknown'} удален!", reply_markup=get_escorts_submenu_keyboard())
        await log_action("remove_escort", user_id, None, f"Удален сопровождающий @{username or 'Unknown'}")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_remove_escort для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "балансы сопровождающих")
async def escorts_balances(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username, balance, telegram_id FROM escorts ORDER BY balance DESC"
            )
            escorts = await cursor.fetchall()
        
        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_escorts_submenu_keyboard())
            return
        
        response = " Балансы сопровождающих:\n\n"
        for username, balance, telegram_id in escorts:
            response += f"@{username or 'Unknown'} (ID: {telegram_id}): {balance:.2f} руб.\n"
        
        await message.answer(response, reply_markup=get_escorts_submenu_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в escorts_balances для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_escorts_submenu_keyboard())

@dp.message(F.text == "добавить заказ")
async def add_order_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите информацию о заказе в формате:\nID заказа:Информация о клиенте:Сумма", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.add_order)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в add_order_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_orders_submenu_keyboard())

@dp.message(Form.add_order)
async def process_add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_orders_submenu_keyboard())
        await state.clear()
        return
    
    try:
        parts = message.text.strip().split(":")
        if len(parts) != 3:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        
        order_id, customer_info, amount_str = [part.strip() for part in parts]
        amount = float(amount_str)
        
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO orders (memo_order_id, customer_info, amount) VALUES (?, ?, ?)",
                (order_id, customer_info, amount)
            )
            await conn.commit()
        
        await message.answer(
            MESSAGES["order_added"].format(
                order_id=order_id, amount=amount, description=customer_info, customer=customer_info
            ), 
            reply_markup=get_admin_orders_submenu_keyboard()
        )
        
        # Отправляем уведомления всем пользователям о новом заказе
        await notify_all_users_about_new_order(order_id, customer_info, amount)
        
        await log_action("add_order", user_id, None, f"Добавлен заказ #{order_id} на сумму {amount} руб.")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат суммы.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.IntegrityError:
        await message.answer(" Заказ с таким ID уже существует.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_add_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_orders_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "удалить заказ")
async def delete_order_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите ID заказа для удаления:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.delete_order)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в delete_order_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_orders_submenu_keyboard())

@dp.message(Form.delete_order)
async def process_delete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_admin_orders_submenu_keyboard())
        await state.clear()
        return
    
    order_id = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM orders WHERE memo_order_id = ?", (order_id,))
            order = await cursor.fetchone()
            if not order:
                await message.answer(" Заказ не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            order_db_id = order[0]
            await conn.execute("DELETE FROM order_escorts WHERE order_id = ?", (order_db_id,))
            await conn.execute("DELETE FROM order_applications WHERE order_id = ?", (order_db_id,))
            await conn.execute("DELETE FROM orders WHERE id = ?", (order_db_id,))
            await conn.commit()
        
        await message.answer(f" Заказ #{order_id} удален!", reply_markup=get_admin_orders_submenu_keyboard())
        await log_action("delete_order", user_id, order_db_id, f"Удален заказ #{order_id}")
        await state.clear()
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_delete_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_orders_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "поддержка")
async def support_handler(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        await message.answer(MESSAGES["support_request"], reply_markup=get_cancel_keyboard())
        await state.set_state(Form.support_message)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в support_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(Form.support_message)
async def process_support_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    
    support_text = message.text.strip()
    if not support_text:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard())
        return
    
    try:
        escort = await get_escort(user_id)
        username = escort[6] if escort else "Unknown"
        
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="ответить", callback_data=f"reply_support_{user_id}")]
        ])
        
        await notify_admins(
            f"📩 Сообщение в поддержку от @{username} (ID: {user_id}):\n\n{support_text}",
            reply_markup=admin_keyboard
        )
        await message.answer(MESSAGES["support_sent"], reply_markup=await get_menu_keyboard(user_id))
        await log_action("support_message", user_id, None, f"Сообщение в поддержку: {support_text}")
        await state.clear()
    except TelegramAPIError as e:
        logger.error(f"Ошибка в process_support_message для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.message(F.text == "рейтинг пользователей")
async def user_rating(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT username, total_rating, rating_count, completed_orders, telegram_id
                FROM escorts 
                WHERE rating_count > 0
                ORDER BY (total_rating / rating_count) DESC
                LIMIT 10
                '''
            )
            top_users = await cursor.fetchall()
        
        if not top_users:
            await message.answer("Пока нет пользователей с рейтингом.", reply_markup=await get_menu_keyboard(user_id))
            return
        
        response = "Топ-10 пользователей по рейтингу:\n\n"
        for i, (username, total_rating, rating_count, completed_orders, telegram_id) in enumerate(top_users, 1):
            avg_rating = total_rating / rating_count
            is_current_user = telegram_id == user_id
            marker = " 👈 ВЫ" if is_current_user else ""
            response += f"{i}. @{username or 'Unknown'} - ★ {avg_rating:.2f} ({rating_count} оценок, {completed_orders} заказов){marker}\n"
        
        # Показываем позицию текущего пользователя, если он не в топ-10
        user_position, user_rating_value = await get_user_rating_position(user_id)
        if user_position and user_position > 10:
            response += f"\n📍 Ваша позиция: {user_position} место (★ {user_rating_value:.2f})"
        
        await message.answer(response, reply_markup=await get_menu_keyboard(user_id))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в user_rating для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.message(F.text == "рейтинг сквадов")
async def squad_rating(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT s.name, AVG(e.total_rating / e.rating_count) as avg_rating,
                       COUNT(e.id) as member_count,
                       SUM(e.completed_orders) as total_orders
                FROM squads s
                JOIN escorts e ON s.id = e.squad_id
                WHERE e.rating_count > 0
                GROUP BY s.id
                HAVING COUNT(e.id) > 0
                ORDER BY avg_rating DESC
                LIMIT 10
                '''
            )
            top_squads = await cursor.fetchall()
        
        if not top_squads:
            await message.answer("🏆 Пока нет сквадов с рейтингом.", reply_markup=await get_menu_keyboard(user_id))
            return
        
        response = "🏆 Топ-10 сквадов по рейтингу:\n\n"
        user_squad_position, user_squad_name, user_squad_rating = await get_squad_rating_position(user_id)
        
        for i, (squad_name, avg_rating, member_count, total_orders) in enumerate(top_squads, 1):
            is_user_squad = squad_name == user_squad_name
            marker = " 👈 ВАШ СКВАД" if is_user_squad else ""
            response += f"{i}. {squad_name} - ★ {avg_rating:.2f} ({member_count} чел., {total_orders} заказов){marker}\n"
        
        # Показываем позицию сквада пользователя, если он не в топ-10
        if user_squad_position and user_squad_position > 10:
            response += f"\n📍 Позиция вашего сквада '{user_squad_name}': {user_squad_position} место (★ {user_squad_rating:.2f})"
        
        await message.answer(response, reply_markup=await get_menu_keyboard(user_id))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в squad_rating для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))



@dp.message(F.text == "найти команду")
async def find_squad(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        # Проверяем, не состоит ли пользователь уже в скваде
        escort = await get_escort(user_id)
        if escort and escort[1]:  # squad_id
            await message.answer(" Вы уже состоите в скваде!", reply_markup=await get_menu_keyboard(user_id))
            return
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT s.id, s.name, COUNT(e.id) as member_count
                FROM squads s
                LEFT JOIN escorts e ON s.id = e.squad_id
                GROUP BY s.id, s.name
                HAVING COUNT(e.id) < 10
                ORDER BY s.name
                '''
            )
            squads = await cursor.fetchall()
        
        if not squads:
            await message.answer("🔍 Сейчас нет доступных команд для вступления.", reply_markup=await get_menu_keyboard(user_id))
            return
        
        keyboard_buttons = []
        for squad_id, squad_name, member_count in squads:
            button_text = f"{squad_name} ({member_count}/10 чел.)"
            keyboard_buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"view_squad_{squad_id}")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        await message.answer("🔍 Выберите команду для просмотра критериев вступления:", reply_markup=keyboard)
        
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в find_squad для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))

@dp.callback_query(F.data.startswith("view_squad_"))
async def view_squad_criteria(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        squad_id = int(callback.data.split("_")[-1])
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad_data = await cursor.fetchone()
            if not squad_data:
                await callback.answer(" Команда не найдена.")
                return
            
            squad_name = squad_data[0]
            
            # Получаем критерии команды
            cursor = await conn.execute("SELECT criteria_text FROM squad_criteria WHERE squad_id = ?", (squad_id,))
            criteria_data = await cursor.fetchone()
            criteria_text = criteria_data[0] if criteria_data else "Критерии для вступления в команду не установлены лидером."
            
            # Получаем количество участников
            cursor = await conn.execute("SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad_id,))
            member_count = (await cursor.fetchone())[0]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="подать заявку", callback_data=f"apply_squad_{squad_id}")],
            [InlineKeyboardButton(text="назад", callback_data="back_to_squads")]
        ])
        
        response = (
            f" Команда: {squad_name}\n"
            f" Участников: {member_count}/10\n\n"
            f" Критерии для вступления:\n{criteria_text}"
        )
        await callback.message.edit_text(response, reply_markup=keyboard)
        await callback.answer()
        
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в view_squad_criteria для {user_id}: {e}")
        await callback.answer(" Произошла ошибка")

@dp.callback_query(F.data == "back_to_squads")
async def back_to_squads(callback: types.CallbackQuery):
    await find_squad(callback.message)
    await callback.answer()

@dp.callback_query(F.data.startswith("apply_squad_"))
async def apply_to_squad(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        squad_id = int(callback.data.split("_")[-1])
        
        await state.update_data(target_squad_id=squad_id)
        await callback.message.answer(
            " Для вступления пожалуйста заполните небольшую анкету:\n\n"
            "1️⃣ Введите ваш город (можно просто Москва или Подмосковье):",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(Form.application_city)
        await callback.answer()
        
    except (ValueError, TelegramAPIError) as e:
        logger.error(f"Ошибка в apply_to_squad для {user_id}: {e}")
        await callback.answer(" Произошла ошибка")

@dp.message(Form.application_city)
async def process_application_city(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    
    city = message.text.strip()
    if not city:
        await message.answer(" Пожалуйста, введите ваш город:", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(city=city)
    await message.answer("2️⃣ Введите ваше PUBG ID:", reply_markup=get_cancel_keyboard())
    await state.set_state(Form.application_pubg_id)

@dp.message(Form.application_pubg_id)
async def process_application_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    
    pubg_id = message.text.strip()
    if not pubg_id:
        await message.answer(" Пожалуйста, введите ваше PUBG ID:", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(pubg_id=pubg_id)
    await message.answer("3️⃣ Введите ваше КД (коэффициент добычи):", reply_markup=get_cancel_keyboard())
    await state.set_state(Form.application_cd)

@dp.message(Form.application_cd)
async def process_application_cd(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    
    cd = message.text.strip()
    if not cd:
        await message.answer(" Пожалуйста, введите ваше КД:", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(cd=cd)
    await message.answer("4️⃣ Введите ваш возраст:", reply_markup=get_cancel_keyboard())
    await state.set_state(Form.application_age)

@dp.message(Form.application_age)
async def process_application_age(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        await state.clear()
        return
    
    age = message.text.strip()
    if not age:
        await message.answer(" Пожалуйста, введите ваш возраст:", reply_markup=get_cancel_keyboard())
        return
    
    try:
        data = await state.get_data()
        target_squad_id = data.get('target_squad_id')
        city = data.get('city')
        pubg_id = data.get('pubg_id')
        cd = data.get('cd')
        
        # Получаем информацию о пользователе
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await state.clear()
            return
        
        username = escort[6] or "Unknown"
        
        # Сохраняем анкету в базу данных
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (target_squad_id,))
            squad_data = await cursor.fetchone()
            if not squad_data:
                await message.answer(" Команда не найдена.", reply_markup=await get_menu_keyboard(user_id))
                await state.clear()
                return
            
            squad_name = squad_data[0]
            
            cursor = await conn.execute(
                '''
                INSERT OR REPLACE INTO squad_applications 
                (user_id, squad_id, city, pubg_id, cd, age, status) 
                VALUES ((SELECT id FROM escorts WHERE telegram_id = ?), ?, ?, ?, ?, ?, 'pending')
                ''',
                (user_id, target_squad_id, city, pubg_id, cd, age)
            )
            
            # Получаем ID заявки
            application_id = cursor.lastrowid
            await conn.commit()
            
            # Если lastrowid не сработал, получаем ID через отдельный запрос
            if application_id is None:
                cursor = await conn.execute(
                    '''
                    SELECT id FROM squad_applications 
                    WHERE user_id = (SELECT id FROM escorts WHERE telegram_id = ?) 
                    AND squad_id = ? 
                    ORDER BY created_at DESC 
                    LIMIT 1
                    ''',
                    (user_id, target_squad_id)
                )
                result = await cursor.fetchone()
                application_id = result[0] if result else None
            
            # Находим лидера команды
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id 
                FROM squad_leaders sl
                JOIN escorts e ON sl.leader_id = e.id
                WHERE sl.squad_id = ?
                ''', (target_squad_id,)
            )
            leader_data = await cursor.fetchone()
        
        # Отправляем заявку лидеру
        if leader_data:
            leader_telegram_id = leader_data[0]
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="принять", callback_data=f"accept_app_{application_id}")],
                [InlineKeyboardButton(text="отклонить", callback_data=f"reject_app_{application_id}")]
            ])
            
            leader_message = (
                f" Новая заявка в команду '{squad_name}'\n\n"
                f" Пользователь: @{username} (ID: {user_id})\n\n"
                f" Анкета:\n"
                f"1. Город: {city}\n"
                f"2. PUBG ID: {pubg_id}\n"
                f"3. КД: {cd}\n"
                f"4. Возраст: {age}"
            )
            
            try:
                await bot.send_message(leader_telegram_id, leader_message, reply_markup=keyboard)
                logger.info(f"Заявка {application_id} отправлена лидеру {leader_telegram_id}")
            except TelegramAPIError as e:
                logger.error(f"Не удалось отправить заявку лидеру {leader_telegram_id}: {e}")
                # Уведомляем админов о проблеме
                await notify_admins(f" Не удалось доставить заявку в команду '{squad_name}' от @{username} лидеру {leader_telegram_id}")
        else:
            # Если нет лидера, уведомляем админов
            await notify_admins(f" Заявка в команду '{squad_name}' от @{username}, но у команды нет лидера!")
        
        await message.answer(
            f" Ваша заявка в команду '{squad_name}' отправлена лидеру!\n"
            f"Ожидайте ответа.",
            reply_markup=await get_menu_keyboard(user_id)
        )
        
        await log_action("squad_application", user_id, None, f"Подана заявка в команду '{squad_name}'")
        await state.clear()
        
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_application_age для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await state.clear()

@dp.callback_query(F.data.startswith("accept_app_"))
async def accept_application(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        application_id = int(callback.data.split("_")[-1])
        
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем данные заявки
            cursor = await conn.execute(
                '''
                SELECT sa.user_id, sa.squad_id, e.telegram_id, e.username, s.name, sa.status
                FROM squad_applications sa
                JOIN escorts e ON sa.user_id = e.id
                JOIN squads s ON sa.squad_id = s.id
                WHERE sa.id = ?
                ''', (application_id,)
            )
            app_data = await cursor.fetchone()
            
            if not app_data:
                await callback.message.edit_text(" Заявка не найдена.")
                await callback.answer()
                return
                
            user_escort_id, squad_id, applicant_telegram_id, applicant_username, squad_name, status = app_data
            
            if status != 'pending':
                await callback.message.edit_text(f" Заявка уже обработана (статус: {status}).")
                await callback.answer()
                return
            
            
            
            # Проверяем, что пользователь действительно лидер этого сквада
            cursor = await conn.execute(
                '''
                SELECT COUNT(*) FROM squad_leaders sl
                JOIN escorts e ON sl.leader_id = e.id
                WHERE e.telegram_id = ? AND sl.squad_id = ?
                ''', (user_id, squad_id)
            )
            is_squad_leader = (await cursor.fetchone())[0] > 0
            
            if not is_squad_leader and not is_admin(user_id):
                await callback.message.edit_text(" У вас нет прав для принятия заявок в этот сквад.")
                await callback.answer()
                return
            
            # Проверяем, что команда не переполнена
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad_id,)
            )
            current_members = (await cursor.fetchone())[0]
            
            if current_members >= 10:
                await callback.message.edit_text(" Команда уже заполнена (максимум 10 участников).")
                await callback.answer()
                return
            
            # Проверяем, что пользователь еще не в другом сквадрэ
            cursor = await conn.execute("SELECT squad_id FROM escorts WHERE id = ?", (user_escort_id,))
            current_squad = await cursor.fetchone()
            if current_squad and current_squad[0]:
                await callback.message.edit_text(" Пользователь уже состоит в другом скваде.")
                await callback.answer()
                return
            
            # Принимаем заявку
            await conn.execute(
                "UPDATE squad_applications SET status = 'accepted' WHERE id = ?",
                (application_id,)
            )
            
            # Добавляем пользователя в команду
            await conn.execute(
                "UPDATE escorts SET squad_id = ? WHERE id = ?",
                (squad_id, user_escort_id)
            )
            
            await conn.commit()
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                applicant_telegram_id,
                f" Ваша заявка в команду '{squad_name}' принята! Добро пожаловать!"
            )
        except TelegramAPIError:
            pass
        
        await callback.message.edit_text(
            f" Заявка пользователя @{applicant_username or 'Unknown'} принята!\n"
            f"Пользователь добавлен в команду '{squad_name}'."
        )
        
        await log_action("accept_application", user_id, None, f"Принята заявка пользователя {applicant_telegram_id} в команду '{squad_name}'")
        await callback.answer()
        
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в accept_application для {user_id}: {e}")
        await callback.answer(" Произошла ошибка")

@dp.callback_query(F.data.startswith("reject_app_"))
async def reject_application(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        application_id = int(callback.data.split("_")[-1])
        
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем данные заявки
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, s.name, sa.status
                FROM squad_applications sa
                JOIN escorts e ON sa.user_id = e.id
                JOIN squads s ON sa.squad_id = s.id
                WHERE sa.id = ?
                ''', (application_id,)
            )
            app_data = await cursor.fetchone()
            
            if not app_data:
                await callback.message.edit_text(" Заявка не найдена.")
                await callback.answer()
                return
                
            applicant_telegram_id, applicant_username, squad_name, status = app_data
            
            if status != 'pending':
                await callback.message.edit_text(f" Заявка уже обработана (статус: {status}).")
                await callback.answer()
                return
            
            # Отклоняем заявку
            await conn.execute(
                "UPDATE squad_applications SET status = 'rejected' WHERE id = ?",
                (application_id,)
            )
            await conn.commit()
        
        # Уведомляем пользователя
        try:
            await bot.send_message(
                applicant_telegram_id,
                f" Ваша заявка в команду '{squad_name}' отклонена."
            )
        except TelegramAPIError:
            pass
        
        await callback.message.edit_text(
            f" Заявка пользователя @{applicant_username or 'Unknown'} отклонена."
        )
        
        await log_action("reject_application", user_id, None, f"Отклонена заявка пользователя {applicant_telegram_id} в команду '{squad_name}'")
        await callback.answer()
        
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в reject_application для {user_id}: {e}")
        await callback.answer(" Произошла ошибка")

# --- Обработчики оставшихся кнопок админ-панели ---

@dp.message(F.text == "бан навсегда")
async def ban_permanent_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите Telegram ID пользователя для постоянного бана:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.ban_permanent)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в ban_permanent_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())

@dp.message(Form.ban_permanent)
async def process_ban_permanent(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user_data[0]
            await conn.execute("UPDATE escorts SET is_banned = 1 WHERE telegram_id = ?", (target_user_id,))
            await conn.commit()
        
        await message.answer(f" Пользователь @{username or 'Unknown'} (ID: {target_user_id}) заблокирован навсегда!", reply_markup=get_bans_submenu_keyboard())
        await log_action("ban_permanent", user_id, None, f"Постоянный бан пользователя {target_user_id}")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_ban_permanent для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "бан на время")
async def ban_duration_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("⏰ Введите данные в формате:\nTelegram ID:Часы бана", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.ban_duration)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в ban_duration_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())

@dp.message(Form.ban_duration)
async def process_ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()
        return
    
    try:
        parts = message.text.strip().split(":")
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        
        target_user_id = int(parts[0])
        hours = int(parts[1])
        ban_until = datetime.now() + timedelta(hours=hours)
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user_data[0]
            await conn.execute("UPDATE escorts SET ban_until = ? WHERE telegram_id = ?", (ban_until.isoformat(), target_user_id))
            await conn.commit()
        
        await message.answer(f"⏰ Пользователь @{username or 'Unknown'} (ID: {target_user_id}) заблокирован до {ban_until.strftime('%d.%m.%Y %H:%M')}!", reply_markup=get_bans_submenu_keyboard())
        await log_action("ban_duration", user_id, None, f"Временный бан пользователя {target_user_id} на {hours} часов")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат данных.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_ban_duration для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "снять бан")
async def unban_user_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("🔓 Введите Telegram ID пользователя для снятия бана:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.unban_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в unban_user_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())

@dp.message(Form.unban_user)
async def process_unban_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user_data[0]
            await conn.execute("UPDATE escorts SET is_banned = 0, ban_until = NULL WHERE telegram_id = ?", (target_user_id,))
            await conn.commit()
        
        await message.answer(MESSAGES["user_unbanned"].format(username=username or "Unknown"), reply_markup=get_bans_submenu_keyboard())
        await log_action("unban_user", user_id, None, f"Разбан пользователя {target_user_id}")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_unban_user для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "снять ограничение")
async def unrestrict_user_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("🔓 Введите Telegram ID пользователя для снятия ограничения:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.unrestrict_user)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в unrestrict_user_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())

@dp.message(Form.unrestrict_user)
async def process_unrestrict_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user_data[0]
            await conn.execute("UPDATE escorts SET restrict_until = NULL WHERE telegram_id = ?", (target_user_id,))
            await conn.commit()
        
        await message.answer(MESSAGES["user_unrestricted"].format(username=username or "Unknown"), reply_markup=get_bans_submenu_keyboard())
        await log_action("unrestrict_user", user_id, None, f"Снято ограничение с пользователя {target_user_id}")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_unrestrict_user для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "ограничить")
async def restrict_user_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("⛔ Введите данные в формате:\nTelegram ID:Часы ограничения", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.restrict_duration)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в restrict_user_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())

@dp.message(Form.restrict_duration)
async def process_restrict_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()
        return
    
    try:
        parts = message.text.strip().split(":")
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        
        target_user_id = int(parts[0])
        hours = int(parts[1])
        restrict_until = datetime.now() + timedelta(hours=hours)
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            username = user_data[0]
            await conn.execute("UPDATE escorts SET restrict_until = ? WHERE telegram_id = ?", (restrict_until.isoformat(), target_user_id))
            await conn.commit()
        
        await message.answer(f"⛔ Пользователь @{username or 'Unknown'} (ID: {target_user_id}) ограничен до {restrict_until.strftime('%d.%m.%Y %H:%M')}!", reply_markup=get_bans_submenu_keyboard())
        await log_action("restrict_user", user_id, None, f"Ограничение пользователя {target_user_id} на {hours} часов")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат данных.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_restrict_duration для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_bans_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "начислить")
async def add_balance_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("💸 Введите данные в формате:\nTelegram ID:Сумма", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.balance_amount)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в add_balance_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_submenu_keyboard())

@dp.message(Form.balance_amount)
async def process_balance_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_balances_submenu_keyboard())
        await state.clear()
        return
    
    try:
        parts = message.text.strip().split(":")
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_cancel_keyboard(True))
            return
        
        target_user_id = int(parts[0])
        amount = float(parts[1])
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            await conn.execute("UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?", (amount, target_user_id))
            await conn.commit()
        
        await message.answer(MESSAGES["balance_added"].format(amount=amount, user_id=target_user_id), reply_markup=get_balances_submenu_keyboard())
        await log_action("add_balance", user_id, None, f"Начислено {amount} руб. пользователю {target_user_id}")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат данных.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_balance_amount для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "обнулить баланс")
async def zero_balance_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer(" Введите Telegram ID пользователя для обнуления баланса:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.zero_balance)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в zero_balance_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_submenu_keyboard())

@dp.message(Form.zero_balance)
async def process_zero_balance(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_balances_submenu_keyboard())
        await state.clear()
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT username FROM escorts WHERE telegram_id = ?", (target_user_id,))
            user_data = await cursor.fetchone()
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_cancel_keyboard(True))
                return
            
            await conn.execute("UPDATE escorts SET balance = 0 WHERE telegram_id = ?", (target_user_id,))
            await conn.commit()
        
        await message.answer(MESSAGES["balance_zeroed"].format(user_id=target_user_id), reply_markup=get_balances_submenu_keyboard())
        await log_action("zero_balance", user_id, None, f"Обнулен баланс пользователя {target_user_id}")
        await state.clear()
    except ValueError:
        await message.answer(" Неверный формат Telegram ID.", reply_markup=get_cancel_keyboard(True))
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в process_zero_balance для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_submenu_keyboard())
        await state.clear()

@dp.message(F.text == "все балансы")
async def all_balances(message: types.Message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username, balance, telegram_id FROM escorts WHERE balance > 0 ORDER BY balance DESC"
            )
            balances = await cursor.fetchall()
        
        if not balances:
            await message.answer(" Пользователей с положительным балансом нет.", reply_markup=get_balances_submenu_keyboard())
            return
        
        response = " Все балансы:\n\n"
        total_balance = 0
        for username, balance, telegram_id in balances:
            response += f"@{username or 'Unknown'} (ID: {telegram_id}): {balance:.2f} руб.\n"
            total_balance += balance
        
        response += f"\n💎 Общий баланс: {total_balance:.2f} руб."
        await message.answer(response, reply_markup=get_balances_submenu_keyboard())
    except aiosqlite.Error as e:
        logger.error(f"Ошибка в all_balances для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_balances_submenu_keyboard())

@dp.message(F.text == "информация о пользователе")
async def user_info_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if not is_admin(user_id):
        await message.answer(MESSAGES["no_access"], reply_markup=await get_menu_keyboard(user_id))
        return
    try:
        await message.answer("Введите Telegram ID пользователя для получения информации:", reply_markup=get_cancel_keyboard(True))
        await state.set_state(Form.user_info_id)
    except TelegramAPIError as e:
        logger.error(f"Ошибка в user_info_handler для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())

@dp.message(Form.user_info_id)
async def process_user_info_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == "отмена":
        await message.answer(MESSAGES["cancel_action"], reply_markup=get_users_submenu_keyboard())
        await state.clear()
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем основную информацию о пользователе
            cursor = await conn.execute(
                "SELECT id, username, pubg_id FROM escorts WHERE telegram_id = ?", 
                (target_user_id,)
            )
            user_data = await cursor.fetchone()
            
            if not user_data:
                await message.answer(" Пользователь не найден.", reply_markup=get_users_submenu_keyboard())
                await state.clear()
                return
            
            escort_id, username, pubg_id = user_data
            
            # Получаем информацию из анкеты (последняя заявка)
            cursor = await conn.execute(
                '''
                SELECT city, pubg_id as app_pubg_id, cd, age
                FROM squad_applications
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                ''', (escort_id,)
            )
            application_data = await cursor.fetchone()
        
        if application_data:
            city, app_pubg_id, cd, age = application_data
            response = (
                f" Информация о пользователе @{username or 'Unknown'} (ID: {target_user_id})\n\n"
                f"1. Город: {city or 'не указан'}\n"
                f"2. PUBG ID: {app_pubg_id or pubg_id or 'не указан'}\n"
                f"3. КД: {cd or 'не указан'}\n"
                f"4. Возраст: {age or 'не указан'}"
            )
        else:
            response = (
                f" Информация о пользователе @{username or 'Unknown'} (ID: {target_user_id})\n\n"
                f" Пользователь не заполнял анкету\n"
                f"PUBG ID: {pubg_id or 'не указан'}"
            )
        
        await message.answer(response, reply_markup=get_users_submenu_keyboard())
        await log_action("view_user_info", user_id, description=f"Просмотр информации о пользователе {target_user_id}")
        await state.clear()
        
    except ValueError:
        await message.answer(" Неверный формат Telegram ID. Введите числовой ID.", reply_markup=get_cancel_keyboard(True))
    except Exception as e:
        logger.error(f"Ошибка в process_user_info_id для {user_id}: {e}\n{traceback.format_exc()}")
        await message.answer(MESSAGES["error"], reply_markup=get_users_submenu_keyboard())
        await state.clear()

@dp.callback_query(F.data.startswith("leave_order_"))
async def leave_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("Ваш профиль не найден.", reply_markup=await get_menu_keyboard(user_id))
            await callback.answer()
            return
        
        escort_id = escort[0]
        
        async with aiosqlite.connect(DB_PATH) as conn:
            # Получаем информацию о заказе до удаления
            cursor = await conn.execute("SELECT memo_order_id FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order:
                await callback.answer(" Заказ не найден.")
                return
            
            memo_order_id = order[0]
            
            # Удаляем пользователя из заявок
            await conn.execute(
                "DELETE FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            await conn.commit()
        
        # Обновляем меню участников
        await show_order_participants_menu(callback.message, order_db_id, memo_order_id)
        await callback.answer(" Вы покинули заказ")
        
        await callback.answer()
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в leave_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("refresh_order_"))
async def refresh_order_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        
        # Получаем информацию о заказе
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT memo_order_id FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order:
                await callback.answer(" Заказ не найден.")
                return
        
        memo_order_id = order[0]
        await show_order_participants_menu(callback.message, order_db_id, memo_order_id)
        await callback.answer("🔄 Меню обновлено!")
        
    except (ValueError, aiosqlite.Error) as e:
        logger.error(f"Ошибка в refresh_order_menu для {user_id}: {e}")
        await callback.answer(" Произошла ошибка при обновлении.")

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        await callback.message.delete()
        await callback.message.answer("Выберите действие:", reply_markup=await get_menu_keyboard(user_id))
        await callback.answer()
    except TelegramAPIError as e:
        logger.error(f"Ошибка в cancel_order для {user_id}: {e}")
        await callback.answer()

# Добавляем обработчик для неизвестных команд
@dp.message()
async def unknown_command(message: types.Message):
    user_id = message.from_user.id
    try:
        if not await check_access(message):
            return
        await message.answer("❓ Неизвестная команда. Используйте кнопки меню.", reply_markup=await get_menu_keyboard(user_id))
    except TelegramAPIError as e:
        logger.error(f"Ошибка в unknown_command для {user_id}: {e}")

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