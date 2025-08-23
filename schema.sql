-- Пользователи/эскорты
CREATE TABLE IF NOT EXISTS escorts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    squad_id INTEGER,
    total_rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    FOREIGN KEY (squad_id) REFERENCES squads(squad_id)
);

-- Сквады
CREATE TABLE IF NOT EXISTS squads (
    squad_id INTEGER PRIMARY KEY AUTOINCREMENT,
    squad_name TEXT UNIQUE NOT NULL,
    leader_id INTEGER NOT NULL,
    rating REAL DEFAULT 0.0,
    FOREIGN KEY (leader_id) REFERENCES escorts(id)
);

-- Заказы
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memo_order_id TEXT UNIQUE NOT NULL,
    customer_info TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    assigned_squad_id INTEGER,
    creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completion_date TIMESTAMP,
    rating INTEGER DEFAULT 0,
    FOREIGN KEY (assigned_squad_id) REFERENCES squads(squad_id)
);

-- Участники заказов
CREATE TABLE IF NOT EXISTS order_escorts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    escort_id INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGNKEY (escort_id) REFERENCES escorts(id)
);

-- Логи действий
CREATE TABLE IF NOT EXISTS action_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    target_id INTEGER,
    details TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Лидеры сквадов (если нужна отдельная таблица)
CREATE TABLE IF NOT EXISTS squad_leaders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    squad_id INTEGER NOT NULL,
    leader_id INTEGER NOT NULL,
    appointed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (squad_id) REFERENCES squads(squad_id),
    FOREIGN KEY (leader_id) REFERENCES escorts(id)
);

-- Пользователи (таблица для всех пользователей бота)
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    telegram_id INTEGER UNIQUE NOT NULL,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    squad_id INTEGER,
    total_rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    is_subscribed BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (squad_id) REFERENCES squads(squad_id)
);