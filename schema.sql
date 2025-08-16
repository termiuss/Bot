
-- Схема базы данных для Telegram бота
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

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id);
CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id);
CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id);
CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id);
CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date);
