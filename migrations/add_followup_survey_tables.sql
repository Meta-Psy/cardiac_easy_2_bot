-- Миграция для добавления таблиц опроса ТОЧКА 3 (3+ месяца после вебинара)
-- Дата создания: 2025-01-XX
-- Описание: Добавляет таблицы followup_surveys и followup_status для опроса через 3+ месяца

-- Таблица для хранения ответов опроса ТОЧКА 3
CREATE TABLE IF NOT EXISTS followup_surveys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id BIGINT NOT NULL,

    -- Вопрос 1: Обращение к врачу
    doctor_visit VARCHAR(255),

    -- Вопрос 2: К какому врачу обращались (множественный выбор, до 2)
    doctor_type TEXT,

    -- Вопрос 3: Отношение врача
    doctor_attitude VARCHAR(255),

    -- Вопрос 4: Что было сделано на приеме (множественный выбор)
    visit_actions TEXT,
    visit_actions_other TEXT,

    -- Вопрос 5: Серьезность врача (шкала 1-10)
    doctor_seriousness INTEGER,

    -- Вопрос 6: Следование рекомендациям
    following_recommendations VARCHAR(255),
    following_barriers TEXT,

    -- Вопрос 7: Шаги по снижению рисков (множественный выбор)
    risk_reduction_steps TEXT,
    risk_reduction_other TEXT,

    -- Вопрос 8: Стабильность изменений (шкала 0-10)
    changes_stability INTEGER,

    -- Вопрос 9: Трудности (множественный выбор)
    difficulties TEXT,
    difficulties_other TEXT,

    -- Вопрос 10: Изменение отношения к профилактике
    prevention_attitude_change VARCHAR(255),

    -- Вопрос 11: Уверенность в понимании (шкала 0-10)
    understanding_confidence INTEGER,

    -- Вопрос 12: Потребность в дополнительной информации (множественный выбор)
    additional_info_need TEXT,
    additional_info_other TEXT,

    -- Вопрос 13: Главное изменение (свободный ответ)
    main_change TEXT,

    -- Временные метки
    completed_at DATETIME,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
);

-- Таблица для отслеживания статуса рассылки ТОЧКА 3
CREATE TABLE IF NOT EXISTS followup_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id BIGINT NOT NULL,

    -- Статусы рассылки
    initial_message_sent BOOLEAN NOT NULL DEFAULT 0,
    initial_message_sent_at DATETIME,

    -- Начало опроса
    survey_started BOOLEAN NOT NULL DEFAULT 0,
    survey_started_at DATETIME,

    -- Завершение опроса
    survey_completed BOOLEAN NOT NULL DEFAULT 0,
    survey_completed_at DATETIME,

    -- Получение бонуса
    bonus_sent BOOLEAN NOT NULL DEFAULT 0,
    bonus_sent_at DATETIME,

    -- Временные метки
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_followup_surveys_telegram_id ON followup_surveys(telegram_id);
CREATE INDEX IF NOT EXISTS idx_followup_status_telegram_id ON followup_status(telegram_id);

-- Комментарий о миграции
-- Эта миграция добавляет поддержку опроса ТОЧКА 3 согласно ТЗ в файле punchline.txt
-- После выполнения этой миграции бот сможет:
-- 1. Рассылать начальное сообщение через админ-панель
-- 2. Собирать ответы на 13 вопросов
-- 3. Отправлять методичку ЗОЖ после завершения опроса
