"""
Подключение к базе данных
"""
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .models import Base

logger = logging.getLogger(__name__)

# Создаем движок базы данных
DATABASE_URL = "sqlite:///cardio_bot.db"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def ensure_database_exists() -> bool:
    """Проверяет существование базы данных и создает файл при необходимости"""
    try:
        # Проверяем подключение к базе данных
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
        return False

def init_db():
    """Инициализация базы данных - создание всех таблиц"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("База данных инициализирована успешно")
        
        # Запускаем миграцию для добавления полей вебинара
        try:
            from .migration_webinar import migrate_webinar_fields
            if migrate_webinar_fields():
                logger.info("Миграция вебинара выполнена успешно")
            else:
                logger.warning("Миграция вебинара завершилась с ошибками")
        except Exception as e:
            logger.warning(f"Ошибка миграции вебинара: {e}")
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")
        return False

def get_db():
    """Получение сессии базы данных (для async использования)"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_sync():
    """Получение синхронной сессии базы данных"""
    return SessionLocal()