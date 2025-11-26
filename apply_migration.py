"""
Скрипт для применения миграции базы данных
Добавляет таблицы для опроса ТОЧКА 3
"""
import sys
import os

# Добавляем bot в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from database import engine, Base, FollowUpSurvey, FollowUpStatus
from sqlalchemy import text

def apply_migration():
    """Применение миграции для добавления таблиц опроса ТОЧКА 3"""
    print("🔄 Начинаю миграцию базы данных...")

    try:
        # Создаем все таблицы, которых нет
        Base.metadata.create_all(engine)

        print("✅ Миграция успешно применена!")
        print("\nДобавлены следующие таблицы:")
        print("  - followup_surveys (опрос ТОЧКА 3, 13 вопросов)")
        print("  - followup_status (статус рассылки и прохождения)")
        print("\n📊 Проверяю создание таблиц...")

        # Проверяем, что таблицы созданы
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('followup_surveys', 'followup_status')"
            ))
            tables = [row[0] for row in result]

            if 'followup_surveys' in tables and 'followup_status' in tables:
                print("✅ Обе таблицы успешно созданы!")
            else:
                print("⚠️ Предупреждение: не все таблицы были созданы")
                print(f"   Найденные таблицы: {tables}")

        print("\n🎉 Миграция завершена! Бот готов к работе с опросом ТОЧКА 3")
        print("\nТеперь вы можете:")
        print("  1. Запустить бота")
        print("  2. Зайти в админ-панель")
        print("  3. Выбрать 'Рассылки' → 'РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)'")
        print("  4. Запустить рассылку начального сообщения")

    except Exception as e:
        print(f"❌ Ошибка при применении миграции: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    apply_migration()
