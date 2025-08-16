#!/usr/bin/env python3
"""
Скрипт для ручного запуска миграции БД
"""
import sys
import os

# Добавляем путь к боту
sys.path.append(os.path.join(os.path.dirname(__file__), 'bot'))

from bot.database.migration_webinar import migrate_webinar_fields

if __name__ == "__main__":
    print("Запуск миграции для добавления полей вебинара...")
    
    try:
        success = migrate_webinar_fields()
        if success:
            print("✅ Миграция выполнена успешно!")
        else:
            print("❌ Миграция завершилась с ошибками!")
    except Exception as e:
        print(f"❌ Ошибка миграции: {e}")