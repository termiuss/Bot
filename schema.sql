
-- Сквады
CREATE TABLE IF NOT EXISTS squads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rating REAL DEFAULT 0,
    rating_count INTEGER DEFAULT 0,
    leader_id INTEGER,
    FOREIGN KEY (leader_id) REFERENCES escorts (id) ON DELETE SET NULL
);

-- Пользователи/эскорты
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

-- Заказы
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

-- Участники заказов
CREATE TABLE IF NOT EXISTS order_escorts (
    order_id INTEGER,
    escort_id INTEGER,
    pubg_id TEXT,
    PRIMARY KEY (order_id, escort_id),
    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
    FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE CASCADE
);

-- Заявки на заказы
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

-- Выплаты
CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    escort_id INTEGER,
    amount REAL,
    payout_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE SET NULL,
    FOREIGN KEY (escort_id) REFERENCES escorts (id) ON DELETE SET NULL
);

-- Логи действий
CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT,
    user_id INTEGER,
    order_id INTEGER,
    description TEXT,
    action_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Лидеры сквадов
CREATE TABLE IF NOT EXISTS squad_leaders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    leader_id INTEGER NOT NULL,
    squad_id INTEGER NOT NULL,
    appointed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (leader_id) REFERENCES escorts (id) ON DELETE CASCADE,
    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE CASCADE,
    UNIQUE(leader_id, squad_id)
);

-- Анкеты вступления в команды
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
);

-- Критерии команд
CREATE TABLE IF NOT EXISTS squad_criteria (
    squad_id INTEGER PRIMARY KEY,
    criteria_text TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (squad_id) REFERENCES squads (id) ON DELETE CASCADE
);

-- Индексы для производительности
CREATE INDEX IF NOT EXISTS idx_escorts_telegram_id ON escorts (telegram_id);
CREATE INDEX IF NOT EXISTS idx_orders_memo_order_id ON orders (memo_order_id);
CREATE INDEX IF NOT EXISTS idx_order_escorts_order_id ON order_escorts (order_id);
CREATE INDEX IF NOT EXISTS idx_order_applications_order_id ON order_applications (order_id);
CREATE INDEX IF NOT EXISTS idx_payouts_order_id ON payouts (order_id);
CREATE INDEX IF NOT EXISTS idx_action_log_action_date ON action_log (action_date);
CREATE INDEX IF NOT EXISTS idx_squad_leaders_leader_id ON squad_leaders (leader_id);
CREATE INDEX IF NOT EXISTS idx_squad_leaders_squad_id ON squad_leaders (squad_id);
CREATE INDEX IF NOT EXISTS idx_squad_applications_user_id ON squad_applications (user_id);
CREATE INDEX IF NOT EXISTS idx_squad_applications_squad_id ON squad_applications (squad_id);
