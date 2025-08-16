"""
Миграция БД для добавления полей вебинара (ТОЧКА 2)
"""
import logging
import sqlite3
from .connection import get_db_sync

logger = logging.getLogger(__name__)

def migrate_webinar_fields():
    """Добавляет новые поля для функционала вебинара"""
    db = get_db_sync()
    
    try:
        # Получаем подключение к SQLite
        connection = db.get_bind().raw_connection()
        cursor = connection.cursor()
        
        # Проверяем какие столбцы уже существуют
        cursor.execute("PRAGMA table_info(users)")
        existing_columns = [column[1] for column in cursor.fetchall()]
        
        logger.info(f"Существующие столбцы в таблице users: {existing_columns}")
        
        # Список новых столбцов для добавления
        new_columns = [
            ("webinar_watched", "BOOLEAN DEFAULT 0 NOT NULL"),
            ("webinar_survey_completed", "BOOLEAN DEFAULT 0 NOT NULL"),
            ("bonus_received", "BOOLEAN DEFAULT 0 NOT NULL")
        ]
        
        # Добавляем недостающие столбцы
        for column_name, column_def in new_columns:
            if column_name not in existing_columns:
                try:
                    sql = f"ALTER TABLE users ADD COLUMN {column_name} {column_def}"
                    cursor.execute(sql)
                    logger.info(f"✅ Добавлен столбец {column_name}")
                except Exception as e:
                    logger.error(f"❌ Ошибка добавления столбца {column_name}: {e}")
            else:
                logger.info(f"⏭ Столбец {column_name} уже существует")
        
        # Создаем таблицу webinar_status если её нет
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS webinar_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id BIGINT NOT NULL,
                    initial_message_sent BOOLEAN DEFAULT 0 NOT NULL,
                    initial_message_sent_at DATETIME,
                    watched_webinar BOOLEAN,
                    user_response_at DATETIME,
                    reminder_scheduled BOOLEAN DEFAULT 0 NOT NULL,
                    reminder_sent BOOLEAN DEFAULT 0 NOT NULL,
                    reminder_sent_at DATETIME,
                    link_clicked BOOLEAN DEFAULT 0 NOT NULL,
                    link_clicked_at DATETIME,
                    auto_survey_scheduled BOOLEAN DEFAULT 0 NOT NULL,
                    auto_survey_sent BOOLEAN DEFAULT 0 NOT NULL,
                    auto_survey_sent_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
                )
            """)
            
            # Создаем индекс
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_webinar_status_telegram_id ON webinar_status (telegram_id)")
            logger.info("✅ Таблица webinar_status создана/проверена")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы webinar_status: {e}")
        
        # Создаем таблицу webinar_surveys если её нет
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS webinar_surveys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id BIGINT NOT NULL,
                    understanding_cardio_checkup TEXT,
                    attitude_change TEXT,
                    screening_problems TEXT,
                    cv_risk_assessment TEXT,
                    lifestyle_actions TEXT,
                    doctor_consultation_plans TEXT,
                    checkup_motivation TEXT,
                    webinar_influence TEXT,
                    most_useful_from_webinar TEXT,
                    completed_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    FOREIGN KEY (telegram_id) REFERENCES users (telegram_id)
                )
            """)
            
            # Создаем индекс
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_webinar_surveys_telegram_id ON webinar_surveys (telegram_id)")
            logger.info("✅ Таблица webinar_surveys создана/проверена")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы webinar_surveys: {e}")
        
        # Сохраняем изменения
        connection.commit()
        logger.info("✅ Миграция вебинара завершена успешно")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка миграции: {e}")
        if 'connection' in locals():
            connection.rollback()
        return False
        
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        db.close()

if __name__ == "__main__":
    # Для тестирования
    migrate_webinar_fields()