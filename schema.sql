
-- Таблица сквадов
CREATE TABLE IF NOT EXISTS squads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    total_orders INTEGER DEFAULT 0,
    total_balance REAL DEFAULT 0.0,
    rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица сопровождающих
CREATE TABLE IF NOT EXISTS escorts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER NOT NULL UNIQUE,
    username TEXT NOT NULL,
    pubg_id TEXT NOT NULL,
    squad_id INTEGER,
    balance REAL DEFAULT 0.0,
    reputation INTEGER DEFAULT 0,
    completed_orders INTEGER DEFAULT 0,
    rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    is_banned BOOLEAN DEFAULT 0,
    ban_until TIMESTAMP NULL,
    restrict_until TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE SET NULL
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memo_order_id TEXT NOT NULL UNIQUE,
    customer_info TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    escort_id INTEGER,
    commission_amount REAL DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    FOREIGN KEY (escort_id) REFERENCES escorts (telegram_id) ON DELETE SET NULL
);

-- Таблица выплат
CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    payout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT,
    FOREIGN KEY (user_id) REFERENCES escorts (telegram_id) ON DELETE CASCADE
);

-- Таблица логов действий
CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    order_id TEXT,
    description TEXT NOT NULL,
    action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts(telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders(memo_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_escort_id ON orders(escort_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_action_log_user_id ON action_log(user_id);
CREATE INDEX IF NOT EXISTS idx_action_log_date ON action_log(action_date);
-- Таблица сквадов
CREATE TABLE IF NOT EXISTS squads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_balance REAL DEFAULT 0.0,
    rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Таблица сопровождающих
CREATE TABLE IF NOT EXISTS escorts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id INTEGER UNIQUE NOT NULL,
    username TEXT NOT NULL,
    pubg_id TEXT NOT NULL,
    squad_id INTEGER,
    balance REAL DEFAULT 0.0,
    reputation INTEGER DEFAULT 0,
    completed_orders INTEGER DEFAULT 0,
    rating REAL DEFAULT 0.0,
    rating_count INTEGER DEFAULT 0,
    is_banned BOOLEAN DEFAULT FALSE,
    ban_until DATETIME,
    restrict_until DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (squad_id) REFERENCES squads (id)
);

-- Таблица заказов
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    memo_order_id TEXT UNIQUE NOT NULL,
    customer_info TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    commission_amount REAL DEFAULT 0.0,
    escort_id INTEGER,
    FOREIGN KEY (escort_id) REFERENCES escorts (telegram_id)
);

-- Таблица журнала действий
CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    order_id TEXT,
    description TEXT NOT NULL,
    action_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Таблица выплат
CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    payout_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES escorts (telegram_id)
);

-- Индексы для улучшения производительности
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts(telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_escort_id ON orders(escort_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_action_log_user_id ON action_log(user_id);
CREATE INDEX IF NOT EXISTS idx_action_log_date ON action_log(action_date);
