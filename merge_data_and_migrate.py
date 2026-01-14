"""
Script for merging xlsx data files and database migration
1. Merges 'Old data.xlsx' and 'New data.xlsx'
2. Removes duplicates by telegram_id
3. Imports merged data to DB
4. Adds new columns to followup_surveys table
"""
import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Добавляем путь к bot для импорта
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

# Настройки
OLD_DATA_FILE = "Старые данные.xlsx"
NEW_DATA_FILE = "Последние данные.xlsx"
DB_PATH = "cardio_bot.db"
MERGED_OUTPUT = "merged_data.xlsx"

def merge_xlsx_files():
    """Объединение xlsx файлов и удаление дубликатов"""
    print("=" * 60)
    print("ЭТАП 1: Объединение xlsx файлов")
    print("=" * 60)

    # Загрузка данных
    print(f"\nЗагрузка {OLD_DATA_FILE}...")
    old_data = pd.read_excel(OLD_DATA_FILE)
    print(f"  Загружено {len(old_data)} записей")

    print(f"\nЗагрузка {NEW_DATA_FILE}...")
    new_data = pd.read_excel(NEW_DATA_FILE)
    print(f"  Загружено {len(new_data)} записей")

    # Объединение
    print("\nОбъединение данных...")
    merged = pd.concat([old_data, new_data], ignore_index=True)
    print(f"  Всего записей после объединения: {len(merged)}")

    # Удаление дубликатов по telegram_id (оставляем последнюю запись)
    before_dedup = len(merged)
    merged = merged.drop_duplicates(subset=['telegram_id'], keep='last')
    after_dedup = len(merged)

    print(f"  Удалено дубликатов: {before_dedup - after_dedup}")
    print(f"  Уникальных записей: {after_dedup}")

    # Сохранение объединённых данных
    merged.to_excel(MERGED_OUTPUT, index=False)
    print(f"\n✅ Объединённые данные сохранены в {MERGED_OUTPUT}")

    return merged

def migrate_followup_surveys_table():
    """Добавление новых столбцов в таблицу followup_surveys"""
    print("\n" + "=" * 60)
    print("ЭТАП 2: Миграция таблицы followup_surveys")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем текущие столбцы таблицы
    cursor.execute("PRAGMA table_info(followup_surveys)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    print(f"\nСуществующие столбцы: {len(existing_columns)}")

    # Новые столбцы для добавления (вопросы 1-7 ТОЧКА 2)
    new_columns = [
        ("understanding_cardio", "VARCHAR(255)"),
        ("attitude_change", "VARCHAR(255)"),
        ("screening_problems", "TEXT"),
        ("cv_risk_assessment", "VARCHAR(255)"),
        ("lifestyle_plans", "TEXT"),
        ("doctor_plan", "VARCHAR(255)"),
        ("webinar_influence", "VARCHAR(255)")
    ]

    added = 0
    for col_name, col_type in new_columns:
        if col_name not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE followup_surveys ADD COLUMN {col_name} {col_type}")
                print(f"  ✅ Добавлен столбец: {col_name}")
                added += 1
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    print(f"  ❌ Ошибка добавления {col_name}: {e}")
        else:
            print(f"  ⏭ Столбец {col_name} уже существует")

    conn.commit()
    conn.close()

    print(f"\n✅ Добавлено новых столбцов: {added}")

def verify_telegram_ids():
    """Проверка корректности telegram_id в БД"""
    print("\n" + "=" * 60)
    print("ЭТАП 3: Проверка telegram_id")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Проверка пользователей
    cursor.execute("SELECT COUNT(*) FROM users WHERE telegram_id >= 100000 AND telegram_id <= 9999999999")
    valid_users = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM users WHERE telegram_id < 100000 OR telegram_id > 9999999999")
    invalid_users = cursor.fetchone()[0]

    print(f"\nUsers:")
    print(f"  ✅ Валидных telegram_id: {valid_users}")
    print(f"  ⚠️ Невалидных telegram_id: {invalid_users}")

    # Проверка surveys
    cursor.execute("SELECT COUNT(*) FROM surveys WHERE telegram_id >= 100000 AND telegram_id <= 9999999999")
    valid_surveys = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM surveys WHERE telegram_id < 100000 OR telegram_id > 9999999999")
    invalid_surveys = cursor.fetchone()[0]

    print(f"\nSurveys:")
    print(f"  ✅ Валидных telegram_id: {valid_surveys}")
    print(f"  ⚠️ Невалидных telegram_id: {invalid_surveys}")

    # Проверка test_results
    cursor.execute("SELECT COUNT(*) FROM test_results WHERE telegram_id >= 100000 AND telegram_id <= 9999999999")
    valid_tests = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM test_results WHERE telegram_id < 100000 OR telegram_id > 9999999999")
    invalid_tests = cursor.fetchone()[0]

    print(f"\nTest Results:")
    print(f"  ✅ Валидных telegram_id: {valid_tests}")
    print(f"  ⚠️ Невалидных telegram_id: {invalid_tests}")

    conn.close()

def import_merged_data_to_db(merged_df: pd.DataFrame):
    """Импорт объединённых данных в БД (users, surveys, test_results)"""
    print("\n" + "=" * 60)
    print("ЭТАП 4: Импорт данных в БД")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем существующих пользователей
    cursor.execute("SELECT telegram_id FROM users")
    existing_users = {row[0] for row in cursor.fetchall()}
    print(f"\nСуществующих пользователей в БД: {len(existing_users)}")

    # Получаем существующие surveys
    cursor.execute("SELECT telegram_id FROM surveys")
    existing_surveys = {row[0] for row in cursor.fetchall()}

    # Получаем существующие test_results
    cursor.execute("SELECT telegram_id FROM test_results")
    existing_tests = {row[0] for row in cursor.fetchall()}

    new_users = 0
    updated_users = 0
    new_surveys = 0
    new_tests = 0

    for _, row in merged_df.iterrows():
        telegram_id = row.get('telegram_id')
        if pd.isna(telegram_id):
            continue

        telegram_id = int(telegram_id)

        # Проверяем валидность telegram_id
        if telegram_id < 100000 or telegram_id > 9999999999:
            continue

        # === USERS ===
        if telegram_id in existing_users:
            update_fields = []
            update_values = []
            for col in ['name', 'email', 'phone']:
                val = row.get(col)
                if pd.notna(val) and val:
                    update_fields.append(f"{col} = ?")
                    update_values.append(str(val))
            if update_fields:
                update_values.append(telegram_id)
                cursor.execute(
                    f"UPDATE users SET {', '.join(update_fields)} WHERE telegram_id = ?",
                    update_values
                )
                updated_users += 1
        else:
            cursor.execute("""
                INSERT INTO users (telegram_id, name, email, phone,
                    completed_diagnostic, registration_completed, survey_completed, tests_completed,
                    created_at, updated_at, last_activity,
                    webinar_watched, webinar_survey_completed, bonus_received)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                telegram_id,
                str(row.get('name', '')) if pd.notna(row.get('name')) else None,
                str(row.get('email', '')) if pd.notna(row.get('email')) else None,
                str(row.get('phone', '')) if pd.notna(row.get('phone')) else None,
                bool(row.get('completed_diagnostic', False)),
                bool(row.get('registration_completed', False)),
                bool(row.get('survey_completed', False)),
                bool(row.get('tests_completed', False)),
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                datetime.now().isoformat(),
                False, False, False
            ))
            new_users += 1
            existing_users.add(telegram_id)

        # === SURVEYS ===
        if telegram_id not in existing_surveys and pd.notna(row.get('age')):
            cursor.execute("""
                INSERT INTO surveys (telegram_id, age, gender, location, education,
                    family_status, children, income, health_rating, death_cause,
                    heart_disease, cv_risk, cv_knowledge, heart_danger, health_importance,
                    checkup_history, checkup_content, prevention_barriers, prevention_barriers_other,
                    health_advice, completed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                telegram_id,
                int(row.get('age')) if pd.notna(row.get('age')) else None,
                str(row.get('gender', '')) if pd.notna(row.get('gender')) else None,
                str(row.get('location', '')) if pd.notna(row.get('location')) else None,
                str(row.get('education', '')) if pd.notna(row.get('education')) else None,
                str(row.get('family_status', '')) if pd.notna(row.get('family_status')) else None,
                str(row.get('children', '')) if pd.notna(row.get('children')) else None,
                str(row.get('income', '')) if pd.notna(row.get('income')) else None,
                int(row.get('health_rating')) if pd.notna(row.get('health_rating')) else None,
                str(row.get('death_cause', '')) if pd.notna(row.get('death_cause')) else None,
                str(row.get('heart_disease', '')) if pd.notna(row.get('heart_disease')) else None,
                str(row.get('cv_risk', '')) if pd.notna(row.get('cv_risk')) else None,
                str(row.get('cv_knowledge', '')) if pd.notna(row.get('cv_knowledge')) else None,
                str(row.get('heart_danger', '')) if pd.notna(row.get('heart_danger')) else None,
                str(row.get('health_importance', '')) if pd.notna(row.get('health_importance')) else None,
                str(row.get('checkup_history', '')) if pd.notna(row.get('checkup_history')) else None,
                str(row.get('checkup_content', '')) if pd.notna(row.get('checkup_content')) else None,
                str(row.get('prevention_barriers', '')) if pd.notna(row.get('prevention_barriers')) else None,
                str(row.get('prevention_barriers_other', '')) if pd.notna(row.get('prevention_barriers_other')) else None,
                str(row.get('health_advice', '')) if pd.notna(row.get('health_advice')) else None,
                str(row.get('survey_completed_at', '')) if pd.notna(row.get('survey_completed_at')) else None,
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            new_surveys += 1
            existing_surveys.add(telegram_id)

        # === TEST_RESULTS ===
        if telegram_id not in existing_tests and pd.notna(row.get('hads_anxiety_score')):
            cursor.execute("""
                INSERT INTO test_results (telegram_id,
                    hads_anxiety_score, hads_depression_score, hads_total_score,
                    hads_anxiety_level, hads_depression_level,
                    burns_score, burns_level, isi_score, isi_level,
                    stop_bang_score, stop_bang_risk, ess_score, ess_level,
                    fagerstrom_score, fagerstrom_level, fagerstrom_skipped,
                    audit_score, audit_level, audit_skipped,
                    overall_cv_risk_score, overall_cv_risk_level, risk_factors_count,
                    completed_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                telegram_id,
                int(row.get('hads_anxiety_score')) if pd.notna(row.get('hads_anxiety_score')) else None,
                int(row.get('hads_depression_score')) if pd.notna(row.get('hads_depression_score')) else None,
                int(row.get('hads_total_score')) if pd.notna(row.get('hads_total_score')) else None,
                str(row.get('hads_anxiety_level', '')) if pd.notna(row.get('hads_anxiety_level')) else None,
                str(row.get('hads_depression_level', '')) if pd.notna(row.get('hads_depression_level')) else None,
                int(row.get('burns_score')) if pd.notna(row.get('burns_score')) else None,
                str(row.get('burns_level', '')) if pd.notna(row.get('burns_level')) else None,
                int(row.get('isi_score')) if pd.notna(row.get('isi_score')) else None,
                str(row.get('isi_level', '')) if pd.notna(row.get('isi_level')) else None,
                int(row.get('stop_bang_score')) if pd.notna(row.get('stop_bang_score')) else None,
                str(row.get('stop_bang_risk', '')) if pd.notna(row.get('stop_bang_risk')) else None,
                int(row.get('ess_score')) if pd.notna(row.get('ess_score')) else None,
                str(row.get('ess_level', '')) if pd.notna(row.get('ess_level')) else None,
                int(row.get('fagerstrom_score')) if pd.notna(row.get('fagerstrom_score')) else None,
                str(row.get('fagerstrom_level', '')) if pd.notna(row.get('fagerstrom_level')) else None,
                bool(row.get('fagerstrom_skipped', False)),
                int(row.get('audit_score')) if pd.notna(row.get('audit_score')) else None,
                str(row.get('audit_level', '')) if pd.notna(row.get('audit_level')) else None,
                bool(row.get('audit_skipped', False)),
                int(row.get('overall_cv_risk_score')) if pd.notna(row.get('overall_cv_risk_score')) else None,
                str(row.get('overall_cv_risk_level', '')) if pd.notna(row.get('overall_cv_risk_level')) else None,
                int(row.get('risk_factors_count')) if pd.notna(row.get('risk_factors_count')) else None,
                str(row.get('tests_completed_at', '')) if pd.notna(row.get('tests_completed_at')) else None,
                datetime.now().isoformat(),
                datetime.now().isoformat()
            ))
            new_tests += 1
            existing_tests.add(telegram_id)

    conn.commit()
    conn.close()

    print(f"\n✅ Добавлено новых пользователей: {new_users}")
    print(f"✅ Обновлено существующих: {updated_users}")
    print(f"✅ Добавлено surveys: {new_surveys}")
    print(f"✅ Добавлено test_results: {new_tests}")

def show_db_stats():
    """Показать статистику БД"""
    print("\n" + "=" * 60)
    print("СТАТИСТИКА БАЗЫ ДАННЫХ")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = ['users', 'surveys', 'test_results', 'followup_surveys', 'followup_status', 'webinar_status', 'webinar_surveys']

    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count} записей")
        except sqlite3.OperationalError:
            print(f"  {table}: таблица не найдена")

    conn.close()

def main():
    print("\n" + "=" * 60)
    print("СКРИПТ ОБЪЕДИНЕНИЯ ДАННЫХ И МИГРАЦИИ БД")
    print("=" * 60)
    print(f"Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Проверка наличия файлов
    if not os.path.exists(OLD_DATA_FILE):
        print(f"❌ Файл {OLD_DATA_FILE} не найден!")
        return

    if not os.path.exists(NEW_DATA_FILE):
        print(f"❌ Файл {NEW_DATA_FILE} не найден!")
        return

    if not os.path.exists(DB_PATH):
        print(f"❌ База данных {DB_PATH} не найдена!")
        return

    # Выполнение этапов
    merged_data = merge_xlsx_files()
    migrate_followup_surveys_table()
    verify_telegram_ids()
    import_merged_data_to_db(merged_data)
    show_db_stats()

    print("\n" + "=" * 60)
    print("✅ МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО")
    print("=" * 60)

if __name__ == "__main__":
    main()
