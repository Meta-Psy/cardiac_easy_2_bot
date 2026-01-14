"""
Тестовый скрипт для проверки функционала бота
Проверяет:
1. Корректность импортов
2. Наличие обработчиков для новых кнопок
3. Структуру БД
4. Возможность рассылки
"""
import os
import sys
import sqlite3

# Fix encoding for Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Add bot directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

def test_imports():
    """Проверка импортов"""
    print("\n" + "=" * 60)
    print("ТЕСТ 1: Проверка импортов")
    print("=" * 60)

    errors = []

    try:
        from handlers.tests import router as tests_router
        print("  ✅ handlers.tests - OK")
    except Exception as e:
        print(f"  ❌ handlers.tests - {e}")
        errors.append(str(e))

    try:
        from handlers.followup_survey import router as followup_router
        print("  ✅ handlers.followup_survey - OK")
    except Exception as e:
        print(f"  ❌ handlers.followup_survey - {e}")
        errors.append(str(e))

    try:
        from handlers.followup_broadcast import broadcast_followup_to_all
        print("  ✅ handlers.followup_broadcast - OK")
    except Exception as e:
        print(f"  ❌ handlers.followup_broadcast - {e}")
        errors.append(str(e))

    try:
        from ui.keyboards import get_test_selection_keyboard
        print("  ✅ ui.keyboards - OK")
    except Exception as e:
        print(f"  ❌ ui.keyboards - {e}")
        errors.append(str(e))

    try:
        from database.models import User, FollowUpSurvey, TestResult
        print("  ✅ database.models - OK")
    except Exception as e:
        print(f"  ❌ database.models - {e}")
        errors.append(str(e))

    return len(errors) == 0

def test_new_handlers():
    """Проверка новых обработчиков"""
    print("\n" + "=" * 60)
    print("ТЕСТ 2: Проверка новых обработчиков")
    print("=" * 60)

    from handlers.tests import router as tests_router

    # Получаем все callback обработчики
    callback_handlers = []
    for handler in tests_router.callback_query.handlers:
        if hasattr(handler, 'callback'):
            callback_handlers.append(handler)

    print(f"  Найдено callback handlers: {len(callback_handlers)}")

    # Проверяем наличие нужных обработчиков
    try:
        from handlers.tests import handle_fagerstrom_not_smoking, handle_audit_not_drinking
        print("  ✅ handle_fagerstrom_not_smoking - OK")
        print("  ✅ handle_audit_not_drinking - OK")
    except ImportError as e:
        print(f"  ❌ Не найдены обработчики: {e}")
        return False

    # Проверяем обработчики для вопросов 1-7
    try:
        from handlers.followup_survey import (
            handle_q1_understanding,
            handle_q2_attitude,
            handle_q3_problems,
            handle_q4_risk,
            handle_q5_plans,
            handle_q6_doctor_plan,
            handle_q7_webinar_influence
        )
        print("  ✅ Обработчики вопросов 1-7 (ТОЧКА 2) - OK")
    except ImportError as e:
        print(f"  ❌ Не найдены обработчики ТОЧКА 2: {e}")
        return False

    return True

def test_database_structure():
    """Проверка структуры БД"""
    print("\n" + "=" * 60)
    print("ТЕСТ 3: Проверка структуры БД")
    print("=" * 60)

    conn = sqlite3.connect('cardio_bot.db')
    cursor = conn.cursor()

    # Проверяем таблицу followup_surveys
    cursor.execute("PRAGMA table_info(followup_surveys)")
    columns = {row[1] for row in cursor.fetchall()}

    required_columns = [
        'understanding_cardio', 'attitude_change', 'screening_problems',
        'cv_risk_assessment', 'lifestyle_plans', 'doctor_plan', 'webinar_influence'
    ]

    missing = [col for col in required_columns if col not in columns]

    if missing:
        print(f"  ❌ Отсутствуют столбцы: {missing}")
        return False

    print(f"  ✅ Все необходимые столбцы присутствуют ({len(required_columns)})")

    # Проверяем таблицу test_results
    cursor.execute("PRAGMA table_info(test_results)")
    columns = {row[1] for row in cursor.fetchall()}

    if 'fagerstrom_skipped' not in columns or 'audit_skipped' not in columns:
        print("  ❌ Отсутствуют столбцы для пропущенных тестов")
        return False

    print("  ✅ Столбцы fagerstrom_skipped и audit_skipped присутствуют")

    conn.close()
    return True

def test_user_data():
    """Проверка данных пользователей"""
    print("\n" + "=" * 60)
    print("ТЕСТ 4: Проверка данных пользователей")
    print("=" * 60)

    conn = sqlite3.connect('cardio_bot.db')
    cursor = conn.cursor()

    # Общее количество
    cursor.execute("SELECT COUNT(*) FROM users")
    total = cursor.fetchone()[0]
    print(f"  Всего пользователей: {total}")

    # Валидные telegram_id
    cursor.execute("SELECT COUNT(*) FROM users WHERE telegram_id >= 100000 AND telegram_id <= 9999999999")
    valid = cursor.fetchone()[0]
    print(f"  С валидным telegram_id: {valid}")

    # Пользователи с контактными данными
    cursor.execute("SELECT COUNT(*) FROM users WHERE email IS NOT NULL AND email != ''")
    with_email = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM users WHERE phone IS NOT NULL AND phone != ''")
    with_phone = cursor.fetchone()[0]
    print(f"  С email: {with_email}")
    print(f"  С телефоном: {with_phone}")

    conn.close()
    return valid == total

def test_broadcast_availability():
    """Проверка доступности рассылки"""
    print("\n" + "=" * 60)
    print("ТЕСТ 5: Проверка доступности рассылки")
    print("=" * 60)

    conn = sqlite3.connect('cardio_bot.db')
    cursor = conn.cursor()

    # Пользователи для рассылки ТОЧКА 3
    cursor.execute("""
        SELECT COUNT(*) FROM users u
        WHERE u.registration_completed = 1
        AND u.telegram_id >= 100000
        AND u.telegram_id <= 9999999999
        AND NOT EXISTS (
            SELECT 1 FROM followup_status fs
            WHERE fs.telegram_id = u.telegram_id
            AND fs.initial_message_sent = 1
        )
    """)
    available = cursor.fetchone()[0]
    print(f"  Доступно для рассылки ТОЧКА 3: {available}")

    # Уже отправлено
    cursor.execute("SELECT COUNT(*) FROM followup_status WHERE initial_message_sent = 1")
    sent = cursor.fetchone()[0]
    print(f"  Уже получили рассылку: {sent}")

    conn.close()
    return True

def main():
    print("\n" + "=" * 60)
    print("ТЕСТИРОВАНИЕ ФУНКЦИОНАЛА БОТА")
    print("=" * 60)

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    results = {
        "Импорты": test_imports(),
        "Обработчики": test_new_handlers(),
        "Структура БД": test_database_structure(),
        "Данные пользователей": test_user_data(),
        "Рассылка": test_broadcast_availability()
    }

    print("\n" + "=" * 60)
    print("ИТОГИ ТЕСТИРОВАНИЯ")
    print("=" * 60)

    all_passed = True
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО")
    else:
        print("❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ")
    print("=" * 60)

    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
