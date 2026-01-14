"""
Модуль аналитики и статистики для Cardio Bot
Содержит функции для анализа данных, генерации отчетов и экспорта статистики
"""

import asyncio
import json
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any

from sqlalchemy import func, or_, text
from sqlalchemy.orm import sessionmaker

from .models import (
    Base, User, Survey, TestResult, ActivityLog, BroadcastLog, SystemStats
)
from .connection import get_db_sync, engine

# Настройка логирования
logger = logging.getLogger(__name__)


# ============================================================================
# ФУНКЦИИ СТАТИСТИКИ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================================


def get_user_data(telegram_id: int) -> Dict[str, Any]:
    """Получить полные данные пользователя"""
    db = get_db_sync()
    try:
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        survey = db.query(Survey).filter(Survey.telegram_id == telegram_id).first()
        tests = (
            db.query(TestResult).filter(TestResult.telegram_id == telegram_id).first()
        )

        return {"user": user, "survey": survey, "tests": tests}
    finally:
        db.close()


def get_user_stats() -> Dict[str, int]:
    """Получить базовую статистику пользователей"""
    db = get_db_sync()
    try:
        total_users = db.query(User).count()
        completed_registration = (
            db.query(User).filter(User.registration_completed == True).count()
        )
        completed_surveys = db.query(User).filter(User.survey_completed == True).count()
        completed_tests = db.query(User).filter(User.tests_completed == True).count()
        completed_diagnostic = (
            db.query(User).filter(User.completed_diagnostic == True).count()
        )

        return {
            "total_users": total_users,
            "completed_registration": completed_registration,
            "completed_surveys": completed_surveys,
            "completed_tests": completed_tests,
            "completed_diagnostic": completed_diagnostic,
        }
    finally:
        db.close()


def get_detailed_stats() -> Dict[str, Any]:
    """Получить детальную статистику"""
    db = get_db_sync()
    try:
        # Базовая статистика
        basic_stats = get_user_stats()

        # Статистика рисков
        risk_stats = (
            db.query(
                TestResult.overall_cv_risk_level,
                func.count(TestResult.overall_cv_risk_level).label("count"),
            )
            .group_by(TestResult.overall_cv_risk_level)
            .all()
        )

        risk_distribution = {level: count for level, count in risk_stats}

        # Демографическая статистика
        surveys = db.query(Survey).all()
        gender_stats = {}
        age_stats = []
        education_stats = {}

        for survey in surveys:
            # Пол
            if survey.gender:
                gender_stats[survey.gender] = gender_stats.get(survey.gender, 0) + 1

            # Возраст
            if survey.age:
                age_stats.append(survey.age)

            # Образование
            if survey.education:
                education_stats[survey.education] = (
                    education_stats.get(survey.education, 0) + 1
                )

        # Статистика тестов (клинически значимые результаты)
        test_stats = {
            "hads_high_anxiety": db.query(TestResult)
            .filter(TestResult.hads_anxiety_score >= 11)
            .count(),
            "hads_high_depression": db.query(TestResult)
            .filter(TestResult.hads_depression_score >= 11)
            .count(),
            "burns_moderate_plus": db.query(TestResult)
            .filter(TestResult.burns_score >= 11)
            .count(),
            "isi_clinical_insomnia": db.query(TestResult)
            .filter(TestResult.isi_score >= 15)
            .count(),
            "stop_bang_high_risk": db.query(TestResult)
            .filter(TestResult.stop_bang_score >= 5)
            .count(),
            "ess_excessive": db.query(TestResult)
            .filter(TestResult.ess_score >= 16)
            .count(),
            "fagerstrom_dependent": db.query(TestResult)
            .filter(TestResult.fagerstrom_score >= 5)
            .count(),
            "audit_risky": db.query(TestResult)
            .filter(TestResult.audit_score >= 8)
            .count(),
        }

        # Активность по дням (последние 30 дней)
        thirty_days_ago = datetime.now() - timedelta(days=30)
        daily_activity = (
            db.query(
                func.date(ActivityLog.timestamp).label("date"),
                func.count(func.distinct(ActivityLog.telegram_id)).label(
                    "active_users"
                ),
            )
            .filter(ActivityLog.timestamp >= thirty_days_ago)
            .group_by(func.date(ActivityLog.timestamp))
            .all()
        )

        return {
            "basic": basic_stats,
            "risk_distribution": risk_distribution,
            "demographics": {
                "gender": gender_stats,
                "age": {
                    "mean": sum(age_stats) / len(age_stats) if age_stats else 0,
                    "min": min(age_stats) if age_stats else 0,
                    "max": max(age_stats) if age_stats else 0,
                    "count": len(age_stats),
                },
                "education": education_stats,
            },
            "test_results": test_stats,
            "daily_activity": [(date, count) for date, count in daily_activity],
        }
    finally:
        db.close()


def get_comprehensive_user_stats() -> Dict[str, Any]:
    """Получить всестороннюю статистику пользователей"""
    db = get_db_sync()
    try:
        # Базовая статистика
        total_users = db.query(User).count()
        completed_registration = (
            db.query(User).filter(User.registration_completed == True).count()
        )
        completed_surveys = db.query(User).filter(User.survey_completed == True).count()
        completed_tests = db.query(User).filter(User.tests_completed == True).count()
        completed_diagnostic = (
            db.query(User).filter(User.completed_diagnostic == True).count()
        )

        # Статистика по времени
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)

        new_users_today = (
            db.query(User).filter(func.date(User.created_at) == today).count()
        )

        new_users_week = (
            db.query(User)
            .filter(User.created_at >= datetime.combine(week_ago, datetime.min.time()))
            .count()
        )

        new_users_month = (
            db.query(User)
            .filter(User.created_at >= datetime.combine(month_ago, datetime.min.time()))
            .count()
        )

        # Активность пользователей
        active_today = (
            db.query(ActivityLog)
            .filter(func.date(ActivityLog.timestamp) == today)
            .distinct(ActivityLog.telegram_id)
            .count()
        )

        active_week = (
            db.query(ActivityLog)
            .filter(
                ActivityLog.timestamp >= datetime.combine(week_ago, datetime.min.time())
            )
            .distinct(ActivityLog.telegram_id)
            .count()
        )

        # Конверсия по этапам
        registration_conversion = (completed_registration / max(total_users, 1)) * 100
        survey_conversion = (completed_surveys / max(completed_registration, 1)) * 100
        tests_conversion = (completed_tests / max(completed_surveys, 1)) * 100
        diagnostic_conversion = (completed_diagnostic / max(completed_tests, 1)) * 100

        # Время до завершения (среднее)
        completed_users = (
            db.query(User)
            .filter(User.completed_diagnostic == True, User.created_at.isnot(None))
            .all()
        )

        completion_times = []
        for user in completed_users:
            # Находим время завершения диагностики
            completion_log = (
                db.query(ActivityLog)
                .filter(
                    ActivityLog.telegram_id == user.telegram_id,
                    ActivityLog.action == "diagnostic_completed",
                )
                .first()
            )

            if completion_log and user.created_at:
                time_diff = completion_log.timestamp - user.created_at
                completion_times.append(time_diff.total_seconds() / 3600)  # в часах

        avg_completion_time = (
            sum(completion_times) / len(completion_times) if completion_times else 0
        )

        return {
            "total_users": total_users,
            "completed_registration": completed_registration,
            "completed_surveys": completed_surveys,
            "completed_tests": completed_tests,
            "completed_diagnostic": completed_diagnostic,
            "new_users": {
                "today": new_users_today,
                "week": new_users_week,
                "month": new_users_month,
            },
            "active_users": {"today": active_today, "week": active_week},
            "conversion_rates": {
                "registration": round(registration_conversion, 2),
                "survey": round(survey_conversion, 2),
                "tests": round(tests_conversion, 2),
                "diagnostic": round(diagnostic_conversion, 2),
            },
            "engagement": {
                "avg_completion_time_hours": round(avg_completion_time, 2),
                "completion_rate": round(
                    (completed_diagnostic / max(total_users, 1)) * 100, 2
                ),
            },
        }

    finally:
        db.close()


# ============================================================================
# ФУНКЦИИ ЭКСПОРТА ДАННЫХ
# ============================================================================


def export_to_excel(filename: str = "cardio_bot_data.xlsx") -> str:
    """Экспорт данных в Excel"""
    db = get_db_sync()
    try:
        # Основной запрос с объединением таблиц
        main_query = """
        SELECT 
            u.telegram_id,
            u.name,
            u.email,
            u.phone,
            u.completed_diagnostic,
            u.registration_completed,
            u.survey_completed,
            u.tests_completed,
            u.created_at as registration_date,
            u.last_activity,
            
            -- Данные опроса
            s.age,
            s.gender,
            s.location,
            s.education,
            s.family_status,
            s.children,
            s.income,
            s.health_rating,
            s.death_cause,
            s.heart_disease,
            s.cv_risk,
            s.cv_knowledge,
            s.heart_danger,
            s.health_importance,
            s.checkup_history,
            s.checkup_content,
            s.prevention_barriers,
            s.prevention_barriers_other,
            s.health_advice,
            s.completed_at as survey_completed_at,
            
            -- Результаты тестов
            t.hads_anxiety_score,
            t.hads_depression_score,
            t.hads_total_score,
            t.hads_anxiety_level,
            t.hads_depression_level,
            t.burns_score,
            t.burns_level,
            t.isi_score,
            t.isi_level,
            t.stop_bang_score,
            t.stop_bang_risk,
            t.ess_score,
            t.ess_level,
            t.fagerstrom_score,
            t.fagerstrom_level,
            t.fagerstrom_skipped,
            t.audit_score,
            t.audit_level,
            t.audit_skipped,
            t.overall_cv_risk_score,
            t.overall_cv_risk_level,
            t.risk_factors_count,
            t.completed_at as tests_completed_at
            
        FROM users u
        LEFT JOIN surveys s ON u.telegram_id = s.telegram_id
        LEFT JOIN test_results t ON u.telegram_id = t.telegram_id
        ORDER BY u.created_at DESC
        """

        # Читаем данные
        full_data = pd.read_sql(main_query, engine)

        # Обрабатываем JSON поля
        def parse_json_field(value):
            if pd.isna(value) or value == "":
                return ""
            try:
                data = json.loads(value)
                if isinstance(data, list):
                    return "; ".join(str(item) for item in data)
                return str(data)
            except:
                return str(value)

        json_columns = [
            "heart_danger",
            "checkup_content",
            "prevention_barriers",
            "health_advice",
        ]
        for col in json_columns:
            if col in full_data.columns:
                full_data[col] = full_data[col].apply(parse_json_field)

        # Получаем статистику рассылок
        broadcast_query = """
        SELECT
            broadcast_id,
            message_text,
            sender_id,
            total_users,
            sent_successfully,
            failed_to_send,
            status,
            created_at
        FROM broadcast_logs
        ORDER BY created_at DESC
        """
        broadcast_data = pd.read_sql(broadcast_query, engine)

        # Получаем данные ТОЧКА 2 (webinar_surveys - 9 вопросов)
        webinar_survey_query = """
        SELECT
            ws.telegram_id,
            u.name,
            ws.understanding_cardio_checkup,
            ws.attitude_change,
            ws.screening_problems,
            ws.cv_risk_assessment,
            ws.lifestyle_actions,
            ws.doctor_consultation_plans,
            ws.checkup_motivation,
            ws.webinar_influence,
            ws.most_useful_from_webinar,
            ws.completed_at,
            ws.created_at
        FROM webinar_surveys ws
        LEFT JOIN users u ON ws.telegram_id = u.telegram_id
        ORDER BY ws.created_at DESC
        """
        webinar_survey_data = pd.read_sql(webinar_survey_query, engine)

        # Получаем данные ТОЧКА 3 (followup_surveys - 20 вопросов)
        followup_survey_query = """
        SELECT
            fs.telegram_id,
            u.name,
            fs.understanding_cardio,
            fs.attitude_change,
            fs.screening_problems,
            fs.cv_risk_assessment,
            fs.lifestyle_plans,
            fs.doctor_plan,
            fs.webinar_influence,
            fs.doctor_visit,
            fs.doctor_type,
            fs.doctor_attitude,
            fs.visit_actions,
            fs.visit_actions_other,
            fs.doctor_seriousness,
            fs.following_recommendations,
            fs.following_barriers,
            fs.risk_reduction_steps,
            fs.risk_reduction_other,
            fs.changes_stability,
            fs.difficulties,
            fs.difficulties_other,
            fs.prevention_attitude_change,
            fs.understanding_confidence,
            fs.additional_info_need,
            fs.additional_info_other,
            fs.main_change,
            fs.completed_at,
            fs.created_at
        FROM followup_surveys fs
        LEFT JOIN users u ON fs.telegram_id = u.telegram_id
        ORDER BY fs.created_at DESC
        """
        followup_survey_data = pd.read_sql(followup_survey_query, engine)

        # Обрабатываем JSON поля в webinar_survey_data
        webinar_json_columns = [
            "screening_problems",
            "lifestyle_actions",
            "checkup_motivation",
        ]
        for col in webinar_json_columns:
            if col in webinar_survey_data.columns:
                webinar_survey_data[col] = webinar_survey_data[col].apply(parse_json_field)

        # Обрабатываем JSON поля в followup_survey_data
        followup_json_columns = [
            "screening_problems",
            "lifestyle_plans",
            "doctor_type",
            "visit_actions",
            "risk_reduction_steps",
            "difficulties",
            "additional_info_need",
        ]
        for col in followup_json_columns:
            if col in followup_survey_data.columns:
                followup_survey_data[col] = followup_survey_data[col].apply(parse_json_field)

        # Получаем логи активности
        activity_query = """
        SELECT 
            telegram_id,
            action,
            details,
            state,
            message_text,
            success,
            timestamp
        FROM activity_logs
        ORDER BY timestamp DESC
        LIMIT 10000
        """
        activity_data = pd.read_sql(activity_query, engine)

        # Создаем Excel файл с несколькими листами
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            # Основные данные
            full_data.to_excel(writer, sheet_name="Все данные", index=False)

            # Только пользователи
            users_data = full_data[
                [
                    "telegram_id",
                    "name",
                    "email",
                    "phone",
                    "completed_diagnostic",
                    "registration_completed",
                    "survey_completed",
                    "tests_completed",
                    "registration_date",
                    "last_activity",
                ]
            ].copy()
            users_data.to_excel(writer, sheet_name="Пользователи", index=False)

            # Результаты опросов
            survey_data = full_data[
                [
                    "telegram_id",
                    "name",
                    "age",
                    "gender",
                    "location",
                    "education",
                    "family_status",
                    "children",
                    "income",
                    "health_rating",
                    "death_cause",
                    "heart_disease",
                    "cv_risk",
                    "cv_knowledge",
                    "health_importance",
                    "survey_completed_at",
                ]
            ].copy()
            survey_data.to_excel(writer, sheet_name="Опросы", index=False)

            # Результаты тестов
            test_data = full_data[
                [
                    "telegram_id",
                    "name",
                    "hads_anxiety_score",
                    "hads_depression_score",
                    "burns_score",
                    "isi_score",
                    "stop_bang_score",
                    "ess_score",
                    "fagerstrom_score",
                    "audit_score",
                    "overall_cv_risk_level",
                    "risk_factors_count",
                    "tests_completed_at",
                ]
            ].copy()
            test_data.to_excel(writer, sheet_name="Результаты тестов", index=False)

            # Рассылки
            broadcast_data.to_excel(writer, sheet_name="Рассылки", index=False)

            # Активность
            activity_data.to_excel(writer, sheet_name="Активность", index=False)

            # ТОЧКА 2 (webinar_surveys - 9 вопросов)
            webinar_survey_data.to_excel(writer, sheet_name="ТОЧКА 2", index=False)

            # ТОЧКА 3 (followup_surveys - 20 вопросов)
            followup_survey_data.to_excel(writer, sheet_name="ТОЧКА 3", index=False)

            # Статистика
            stats = get_detailed_stats()
            stats_data = []

            # Общая статистика
            stats_data.append(["Показатель", "Значение"])
            stats_data.append(
                ["Общее количество пользователей", stats["basic"]["total_users"]]
            )
            stats_data.append(
                ["Завершили регистрацию", stats["basic"]["completed_registration"]]
            )
            stats_data.append(["Завершили опрос", stats["basic"]["completed_surveys"]])
            stats_data.append(["Прошли тесты", stats["basic"]["completed_tests"]])
            stats_data.append(
                ["Завершили диагностику", stats["basic"]["completed_diagnostic"]]
            )
            stats_data.append(["", ""])

            # Статистика рисков
            stats_data.append(["РАСПРЕДЕЛЕНИЕ ПО РИСКАМ", ""])
            for risk_level, count in stats["risk_distribution"].items():
                if risk_level:  # Проверяем, что уровень не None
                    percentage = (
                        (count / stats["basic"]["completed_tests"] * 100)
                        if stats["basic"]["completed_tests"] > 0
                        else 0
                    )
                    stats_data.append(
                        [f"{risk_level} риск", f"{count} ({percentage:.1f}%)"]
                    )

            stats_data.append(["", ""])

            # Демографическая статистика
            stats_data.append(["ДЕМОГРАФИЯ", ""])
            for gender, count in stats["demographics"]["gender"].items():
                percentage = (
                    (count / stats["basic"]["completed_surveys"] * 100)
                    if stats["basic"]["completed_surveys"] > 0
                    else 0
                )
                stats_data.append([f"Пол - {gender}", f"{count} ({percentage:.1f}%)"])

            age_data = stats["demographics"]["age"]
            if age_data["count"] > 0:
                stats_data.append(["Средний возраст", f"{age_data['mean']:.1f} лет"])
                stats_data.append(
                    ["Возрастной диапазон", f"{age_data['min']}-{age_data['max']} лет"]
                )

            stats_data.append(["", ""])

            # Статистика тестов
            stats_data.append(["КЛИНИЧЕСКИ ЗНАЧИМЫЕ РЕЗУЛЬТАТЫ", ""])
            test_labels = {
                "hads_high_anxiety": "Клиническая тревога (≥11 баллов)",
                "hads_high_depression": "Клиническая депрессия (≥11 баллов)",
                "burns_moderate_plus": "Умеренная+ депрессия (≥11 баллов)",
                "isi_clinical_insomnia": "Клиническая бессонница (≥15 баллов)",
                "stop_bang_high_risk": "Высокий риск апноэ (≥5 баллов)",
                "ess_excessive": "Чрезмерная сонливость (≥16 баллов)",
                "fagerstrom_dependent": "Никотиновая зависимость (≥5 баллов)",
                "audit_risky": "Проблемы с алкоголем (≥8 баллов)",
            }

            for test_key, count in stats["test_results"].items():
                if count > 0:
                    label = test_labels.get(test_key, test_key)
                    percentage = (
                        (count / stats["basic"]["completed_tests"] * 100)
                        if stats["basic"]["completed_tests"] > 0
                        else 0
                    )
                    stats_data.append([label, f"{count} ({percentage:.1f}%)"])

            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(
                writer, sheet_name="Статистика", index=False, header=False
            )

        return filename

    except Exception as e:
        logger.error(f"Ошибка экспорта в Excel: {e}")
        raise e
    finally:
        db.close()


def export_single_table_csv(table_name: str, output_file: str = None) -> str:
    """Экспорт одной таблицы в CSV"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{table_name}_{timestamp}.csv"

    db = get_db_sync()
    try:
        # Экспортируем конкретную таблицу
        if table_name == "users":
            query = """
            SELECT 
                id, telegram_id, name, email, phone,
                completed_diagnostic, registration_completed, 
                survey_completed, tests_completed,
                created_at, updated_at, last_activity
            FROM users 
            ORDER BY created_at DESC
            """
        elif table_name == "surveys":
            query = """
            SELECT * FROM surveys ORDER BY created_at DESC
            """
        elif table_name == "test_results":
            query = """
            SELECT * FROM test_results ORDER BY created_at DESC
            """
        else:
            raise ValueError(f"Неизвестная таблица: {table_name}")

        df = pd.read_sql(query, engine)
        df.to_csv(output_file, index=False, encoding="utf-8")

        logger.info(f"Экспорт таблицы {table_name} в {output_file} завершен")
        return output_file

    finally:
        db.close()


def export_users_for_external_system(format_type: str = "crm") -> str:
    """Экспорт пользователей в формате для внешних систем"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    db = get_db_sync()
    try:
        if format_type == "crm":
            # Формат для CRM систем
            query = """
            SELECT 
                u.telegram_id as external_id,
                u.name as full_name,
                u.email,
                u.phone,
                u.created_at as registration_date,
                u.last_activity,
                CASE 
                    WHEN u.completed_diagnostic = 1 THEN 'completed'
                    WHEN u.tests_completed = 1 THEN 'tests_done'
                    WHEN u.survey_completed = 1 THEN 'survey_done'
                    ELSE 'registered'
                END as status,
                s.age,
                s.gender,
                s.location,
                t.overall_cv_risk_level as risk_level
            FROM users u
            LEFT JOIN surveys s ON u.telegram_id = s.telegram_id
            LEFT JOIN test_results t ON u.telegram_id = t.telegram_id
            WHERE u.registration_completed = 1
            ORDER BY u.created_at DESC
            """
            filename = f"crm_export_{timestamp}.csv"

        elif format_type == "analytics":
            # Формат для аналитических систем
            query = """
            SELECT 
                u.telegram_id,
                date(u.created_at) as reg_date,
                u.completed_diagnostic,
                u.survey_completed,
                u.tests_completed,
                s.age,
                s.gender,
                s.health_rating,
                t.overall_cv_risk_level,
                t.hads_anxiety_score,
                t.hads_depression_score,
                julianday('now') - julianday(u.last_activity) as days_since_activity
            FROM users u
            LEFT JOIN surveys s ON u.telegram_id = s.telegram_id
            LEFT JOIN test_results t ON u.telegram_id = t.telegram_id
            WHERE u.registration_completed = 1
            """
            filename = f"analytics_export_{timestamp}.csv"

        else:
            raise ValueError(f"Неподдерживаемый тип экспорта: {format_type}")

        df = pd.read_sql(query, engine)
        df.to_csv(filename, index=False, encoding="utf-8")

        logger.info(f"Экспорт для {format_type} создан: {filename}")
        return filename

    except Exception as e:
        logger.error(f"Ошибка экспорта для {format_type}: {e}")
        raise e
    finally:
        db.close()


# ============================================================================
# АДМИНИСТРАТИВНЫЕ ФУНКЦИИ
# ============================================================================


async def admin_export_data() -> str:
    """Экспорт данных для администратора"""

    def _export():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"cardio_bot_export_{timestamp}.xlsx"

        try:
            return export_to_excel(filename)
        except Exception as e:
            if os.path.exists(filename):
                os.remove(filename)
            raise e

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _export)


async def admin_get_stats() -> Dict[str, Any]:
    """Получить статистику для администратора"""

    def _get_stats():
        return get_user_stats()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_stats)


async def admin_get_detailed_stats() -> Dict[str, Any]:
    """Получить детальную статистику для администратора"""

    def _get_detailed():
        return get_detailed_stats()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_detailed)


# ============================================================================
# ФУНКЦИИ СИСТЕМНОЙ СТАТИСТИКИ
# ============================================================================


def update_daily_stats():
    """Обновить ежедневную статистику"""
    db = get_db_sync()
    try:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        # Получаем текущую статистику
        basic_stats = get_user_stats()
        detailed_stats = get_detailed_stats()

        # Новые пользователи за сегодня
        new_users_today = (
            db.query(User).filter(func.date(User.created_at) == today).count()
        )

        # Активные пользователи за сегодня
        active_users_today = (
            db.query(ActivityLog)
            .filter(func.date(ActivityLog.timestamp) == today)
            .distinct(ActivityLog.telegram_id)
            .count()
        )

        # Сохраняем метрики в системную статистику
        metrics_to_save = {
            "total_users": basic_stats["total_users"],
            "new_users_today": new_users_today,
            "active_users_today": active_users_today,
            "completed_registration": basic_stats["completed_registration"],
            "completed_surveys": basic_stats["completed_surveys"],
            "completed_tests": basic_stats["completed_tests"],
            "completed_diagnostic": basic_stats["completed_diagnostic"],
        }

        # Распределение рисков
        risk_dist = detailed_stats["risk_distribution"]
        metrics_to_save.update({
            "low_risk_users": risk_dist.get("НИЗКИЙ", 0),
            "moderate_risk_users": risk_dist.get("УМЕРЕННЫЙ", 0),
            "high_risk_users": risk_dist.get("ВЫСОКИЙ", 0),
            "very_high_risk_users": risk_dist.get("ОЧЕНЬ ВЫСОКИЙ", 0),
        })

        # Клинически значимые результаты
        test_stats = detailed_stats["test_results"]
        metrics_to_save.update({
            "clinical_anxiety": test_stats["hads_high_anxiety"],
            "clinical_depression": test_stats["hads_high_depression"],
            "severe_insomnia": test_stats["isi_clinical_insomnia"],
            "high_apnea_risk": test_stats["stop_bang_high_risk"],
            "nicotine_dependence": test_stats["fagerstrom_dependent"],
            "alcohol_problems": test_stats["audit_risky"],
        })

        # Сохраняем все метрики
        current_time = datetime.now()
        saved_count = 0
        
        for metric_name, metric_value in metrics_to_save.items():
            stats_entry = SystemStats(
                metric_name=metric_name,
                metric_value=str(metric_value),
                metric_type="gauge",
                tags=json.dumps({"date": today.isoformat(), "type": "daily_stats"}),
                timestamp=current_time
            )
            db.add(stats_entry)
            saved_count += 1

        db.commit()
        logger.info(f"Обновлена ежедневная статистика за {today}, сохранено {saved_count} метрик")

        return saved_count

    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка обновления ежедневной статистики: {e}")
        raise e
    finally:
        db.close()


def get_daily_stats_range(
    start_date: datetime, end_date: datetime
) -> List[SystemStats]:
    """Получить статистику за период"""
    db = get_db_sync()
    try:
        return (
            db.query(SystemStats)
            .filter(SystemStats.timestamp >= start_date, SystemStats.timestamp <= end_date)
            .filter(SystemStats.tags.like('%"type": "daily_stats"%'))
            .order_by(SystemStats.timestamp)
            .all()
        )
    finally:
        db.close()


def get_database_statistics() -> Dict[str, Any]:
    """Получение детальной статистики базы данных"""
    db = get_db_sync()
    try:
        stats = {}

        # Основная статистика таблиц
        tables_info = {
            "users": "SELECT COUNT(*) as count FROM users",
            "surveys": "SELECT COUNT(*) as count FROM surveys",
            "test_results": "SELECT COUNT(*) as count FROM test_results",
            "activity_logs": "SELECT COUNT(*) as count FROM activity_logs",
            "broadcast_logs": "SELECT COUNT(*) as count FROM broadcast_logs",
        }

        for table, query in tables_info.items():
            try:
                result = db.execute(text(query)).fetchone()
                stats[f"{table}_count"] = result[0] if result else 0
            except:
                stats[f"{table}_count"] = 0

        # Статистика по статусам
        status_queries = {
            "registered_users": "SELECT COUNT(*) FROM users WHERE registration_completed = TRUE",
            "completed_surveys": "SELECT COUNT(*) FROM users WHERE survey_completed = TRUE",
            "completed_tests": "SELECT COUNT(*) FROM users WHERE tests_completed = TRUE",
            "completed_diagnostic": "SELECT COUNT(*) FROM users WHERE completed_diagnostic = TRUE",
        }

        for stat_name, query in status_queries.items():
            try:
                result = db.execute(text(query)).fetchone()
                stats[stat_name] = result[0] if result else 0
            except:
                stats[stat_name] = 0

        # Статистика по времени
        try:
            # Пользователи за последние 24 часа
            result = db.execute(
                text(
                    """
                SELECT COUNT(*) FROM users 
                WHERE created_at >= datetime('now', '-1 day')
            """
                )
            ).fetchone()
            stats["new_users_24h"] = result[0] if result else 0

            # Активность за последние 24 часа
            result = db.execute(
                text(
                    """
                SELECT COUNT(DISTINCT telegram_id) FROM activity_logs 
                WHERE timestamp >= datetime('now', '-1 day')
            """
                )
            ).fetchone()
            stats["active_users_24h"] = result[0] if result else 0
        except:
            stats["new_users_24h"] = 0
            stats["active_users_24h"] = 0

        # Размер базы данных
        if os.path.exists("cardio_bot.db"):
            stats["db_size_mb"] = round(
                os.path.getsize("cardio_bot.db") / 1024 / 1024, 2
            )
        else:
            stats["db_size_mb"] = 0

        # Проверка целостности
        integrity_issues = []

        # Пользователи без соответствующих опросов
        try:
            result = db.execute(
                text(
                    """
                SELECT COUNT(*) FROM users u
                LEFT JOIN surveys s ON u.telegram_id = s.telegram_id
                WHERE u.survey_completed = TRUE AND s.telegram_id IS NULL
            """
                )
            ).fetchone()

            if result and result[0] > 0:
                integrity_issues.append(
                    f"Пользователей с флагом опроса без данных: {result[0]}"
                )
        except:
            pass

        # Пользователи без соответствующих тестов
        try:
            result = db.execute(
                text(
                    """
                SELECT COUNT(*) FROM users u
                LEFT JOIN test_results t ON u.telegram_id = t.telegram_id
                WHERE u.tests_completed = TRUE AND t.telegram_id IS NULL
            """
                )
            ).fetchone()

            if result and result[0] > 0:
                integrity_issues.append(
                    f"Пользователей с флагом тестов без данных: {result[0]}"
                )
        except:
            pass

        stats["integrity_issues"] = integrity_issues
        stats["integrity_healthy"] = len(integrity_issues) == 0

        return stats

    except Exception as e:
        logger.error(f"Ошибка получения статистики БД: {e}")
        return {"error": str(e)}
    finally:
        db.close()


# ============================================================================
# ФУНКЦИИ ВАЛИДАЦИИ ДАННЫХ
# ============================================================================


def validate_database_integrity() -> Dict[str, Any]:
    """Проверка целостности базы данных"""
    db = get_db_sync()
    try:
        issues = []

        # Проверяем пользователей без обязательных данных
        users_without_data = (
            db.query(User)
            .filter(
                User.registration_completed == True,
                or_(User.name == None, User.email == None, User.phone == None),
            )
            .count()
        )

        if users_without_data > 0:
            issues.append(
                f"Пользователей с незаполненными данными: {users_without_data}"
            )

        # Проверяем тесты без соответствующих опросов
        tests_without_surveys = (
            db.query(TestResult).outerjoin(Survey).filter(Survey.id == None).count()
        )

        if tests_without_surveys > 0:
            issues.append(f"Результатов тестов без опросов: {tests_without_surveys}")

        # Проверяем консистентность статусов
        inconsistent_statuses = (
            db.query(User)
            .filter(
                User.completed_diagnostic == True,
                or_(User.survey_completed == False, User.tests_completed == False),
            )
            .count()
        )

        if inconsistent_statuses > 0:
            issues.append(
                f"Пользователей с некорректными статусами: {inconsistent_statuses}"
            )

        return {
            "healthy": len(issues) == 0,
            "issues": issues,
            "checked_at": datetime.now().isoformat(),
        }

    finally:
        db.close()


def validate_data_integrity():
    """Проверка целостности данных"""
    db = get_db_sync()
    try:
        issues = []

        # Пользователи с незавершенной регистрацией, но отмеченные как завершившие
        inconsistent_registration = (
            db.query(User)
            .filter(
                User.registration_completed == True,
                or_(User.name.is_(None), User.email.is_(None), User.phone.is_(None)),
            )
            .count()
        )

        if inconsistent_registration > 0:
            issues.append(
                f"Пользователей с некорректным статусом регистрации: {inconsistent_registration}"
            )

        # Пользователи, отмеченные как завершившие опрос, но без записи в surveys
        users_survey_mismatch = (
            db.query(User)
            .outerjoin(Survey)
            .filter(User.survey_completed == True, Survey.id.is_(None))
            .count()
        )

        if users_survey_mismatch > 0:
            issues.append(f"Пользователей без записи опроса: {users_survey_mismatch}")

        # Пользователи, отмеченные как завершившие тесты, но без записи в test_results
        users_tests_mismatch = (
            db.query(User)
            .outerjoin(TestResult)
            .filter(User.tests_completed == True, TestResult.id.is_(None))
            .count()
        )

        if users_tests_mismatch > 0:
            issues.append(f"Пользователей без записи тестов: {users_tests_mismatch}")

        # Пользователи, завершившие диагностику, но не прошедшие все этапы
        diagnostic_incomplete = (
            db.query(User)
            .filter(
                User.completed_diagnostic == True,
                or_(
                    User.registration_completed == False,
                    User.survey_completed == False,
                    User.tests_completed == False,
                ),
            )
            .count()
        )

        if diagnostic_incomplete > 0:
            issues.append(
                f"Пользователей с неполной диагностикой: {diagnostic_incomplete}"
            )

        return {
            "healthy": len(issues) == 0,
            "issues": issues,
            "checked_at": datetime.now().isoformat(),
        }

    finally:
        db.close()


async def validate_telegram_ids(telegram_ids: List[int]) -> Dict[str, List[int]]:
    """Валидация списка Telegram ID"""

    def _validate():
        db = get_db_sync()
        try:
            valid_ids = []
            invalid_ids = []

            # Проверяем каждый ID в базе
            for tid in telegram_ids:
                user = db.query(User).filter(User.telegram_id == tid).first()
                if user:
                    valid_ids.append(tid)
                else:
                    invalid_ids.append(tid)

            return {
                "valid": valid_ids,
                "invalid": invalid_ids,
                "total": len(telegram_ids),
            }

        except Exception as e:
            logger.error(f"Ошибка валидации ID: {e}")
            return {"valid": [], "invalid": telegram_ids, "total": len(telegram_ids)}
        finally:
            db.close()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _validate)


# ============================================================================
# ФУНКЦИИ АНАЛИЗА ПРОИЗВОДИТЕЛЬНОСТИ
# ============================================================================


def analyze_database_performance() -> Dict[str, Any]:
    """Анализ производительности базы данных"""
    db = get_db_sync()
    try:
        analysis = {}

        # Анализ размеров таблиц
        table_sizes = {}
        tables = ["users", "surveys", "test_results", "activity_logs", "broadcast_logs"]

        for table in tables:
            try:
                # Подсчет записей
                result = db.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                count = result[0] if result else 0

                # Примерная оценка размера (SQLite не предоставляет точные размеры таблиц)
                result = db.execute(text(f"SELECT * FROM {table} LIMIT 1")).fetchone()
                if result and count > 0:
                    estimated_size = (
                        len(str(result)) * count / 1024
                    )  # Примерный размер в КБ
                else:
                    estimated_size = 0

                table_sizes[table] = {
                    "records": count,
                    "estimated_size_kb": round(estimated_size, 2),
                }
            except Exception as e:
                table_sizes[table] = {"error": str(e)}

        analysis["table_sizes"] = table_sizes

        # Анализ индексов
        try:
            indices_result = db.execute(
                text("SELECT name, tbl_name FROM sqlite_master WHERE type='index'")
            ).fetchall()
            analysis["indices_count"] = len(indices_result)
            analysis["indices"] = [
                {"name": row[0], "table": row[1]} for row in indices_result
            ]
        except:
            analysis["indices_count"] = 0
            analysis["indices"] = []

        # Статистика запросов (приблизительная)
        try:
            # Проверяем фрагментацию (примерно)
            result = db.execute(text("PRAGMA integrity_check")).fetchone()
            analysis["integrity_check"] = result[0] if result else "unknown"

            # Информация о схеме
            result = db.execute(text("PRAGMA schema_version")).fetchone()
            analysis["schema_version"] = result[0] if result else 0

        except Exception as e:
            analysis["pragma_error"] = str(e)

        # Рекомендации по оптимизации
        recommendations = []

        total_records = sum(
            ts.get("records", 0) for ts in table_sizes.values() if isinstance(ts, dict)
        )
        if total_records > 50000:
            recommendations.append("Рассмотрите архивирование старых данных")

        if table_sizes.get("activity_logs", {}).get("records", 0) > 10000:
            recommendations.append("Очистите старые логи активности")

        if analysis.get("indices_count", 0) < 5:
            recommendations.append("Добавьте индексы для часто используемых полей")

        analysis["recommendations"] = recommendations
        analysis["performance_score"] = calculate_performance_score(
            table_sizes, total_records
        )

        return analysis

    except Exception as e:
        logger.error(f"Ошибка анализа производительности: {e}")
        return {"error": str(e)}
    finally:
        db.close()


def calculate_performance_score(table_sizes: Dict, total_records: int) -> str:
    """Вычисление оценки производительности"""
    try:
        score = 100

        # Штраф за большое количество записей
        if total_records > 100000:
            score -= 30
        elif total_records > 50000:
            score -= 15
        elif total_records > 20000:
            score -= 5

        # Штраф за большие таблицы логов
        activity_logs_size = table_sizes.get("activity_logs", {}).get("records", 0)
        if activity_logs_size > 50000:
            score -= 20
        elif activity_logs_size > 10000:
            score -= 10

        if score >= 90:
            return "Отличная"
        elif score >= 70:
            return "Хорошая"
        elif score >= 50:
            return "Удовлетворительная"
        else:
            return "Требует оптимизации"

    except Exception:
        return "Неизвестна"


# ============================================================================
# НАСТРОЙКА АВТОМАТИЧЕСКИХ ЗАДАЧ
# ============================================================================


def setup_daily_stats_job():
    """Настройка автоматического обновления статистики"""
    try:
        # Только пытаемся обновить статистику, если база уже инициализирована
        if os.path.exists("cardio_bot.db"):
            update_daily_stats()
        else:
            logger.info("База данных еще не создана, пропускаем обновление статистики")
    except Exception as e:
        logger.warning(f"Ошибка при настройке ежедневной статистики: {e}")