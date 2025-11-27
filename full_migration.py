"""
Полная миграция базы данных
Добавляет недостающие колонки и таблицы
"""
import sys
import os
import io

# Настройка кодировки для Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Добавляем bot в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from database import engine
from sqlalchemy import text

def apply_full_migration():
    """Применение полной миграции"""
    print(">> Начинаю полную миграцию базы данных...")

    try:
        with engine.connect() as conn:
            # Проверяем существующие колонки в таблице users
            result = conn.execute(text("PRAGMA table_info(users)"))
            existing_columns = {row[1] for row in result}

            print(f"\nНайдено колонок в users: {len(existing_columns)}")

            # Добавляем недостающие колонки в users (для ТОЧКА 2)
            columns_to_add = [
                ("webinar_watched", "BOOLEAN NOT NULL DEFAULT 0"),
                ("webinar_survey_completed", "BOOLEAN NOT NULL DEFAULT 0"),
                ("bonus_received", "BOOLEAN NOT NULL DEFAULT 0"),
            ]

            for col_name, col_type in columns_to_add:
                if col_name not in existing_columns:
                    print(f"  Добавляю колонку users.{col_name}...")
                    conn.execute(text(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                else:
                    print(f"  Колонка users.{col_name} уже существует")

            # Создаем таблицу followup_surveys если не существует
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS followup_surveys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id BIGINT NOT NULL,
                    doctor_visit VARCHAR(255),
                    doctor_type TEXT,
                    doctor_attitude VARCHAR(255),
                    visit_actions TEXT,
                    visit_actions_other TEXT,
                    doctor_seriousness INTEGER,
                    following_recommendations VARCHAR(255),
                    following_barriers TEXT,
                    risk_reduction_steps TEXT,
                    risk_reduction_other TEXT,
                    changes_stability INTEGER,
                    difficulties TEXT,
                    difficulties_other TEXT,
                    prevention_attitude_change VARCHAR(255),
                    understanding_confidence INTEGER,
                    additional_info_need TEXT,
                    additional_info_other TEXT,
                    main_change TEXT,
                    completed_at DATETIME,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
                )
            """))
            conn.commit()
            print("  Таблица followup_surveys создана/проверена")

            # Создаем таблицу followup_status если не существует
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS followup_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id BIGINT NOT NULL,
                    initial_message_sent BOOLEAN NOT NULL DEFAULT 0,
                    initial_message_sent_at DATETIME,
                    survey_started BOOLEAN NOT NULL DEFAULT 0,
                    survey_started_at DATETIME,
                    survey_completed BOOLEAN NOT NULL DEFAULT 0,
                    survey_completed_at DATETIME,
                    bonus_sent BOOLEAN NOT NULL DEFAULT 0,
                    bonus_sent_at DATETIME,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id)
                )
            """))
            conn.commit()
            print("  Таблица followup_status создана/проверена")

            # Создаем индексы
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_followup_surveys_telegram_id ON followup_surveys(telegram_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_followup_status_telegram_id ON followup_status(telegram_id)"))
            conn.commit()
            print("  Индексы созданы")

        print("\n[SUCCESS] Полная миграция завершена!")
        print("\nБот готов к работе с ТОЧКА 2 и ТОЧКА 3")

    except Exception as e:
        print(f"\n[ERROR] Ошибка при применении миграции: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    apply_full_migration()
