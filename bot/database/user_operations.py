"""
Операции с пользователями - модуль для работы с пользователями в базе данных
"""
import asyncio
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any

from sqlalchemy import func, or_
from sqlalchemy.orm import sessionmaker

from .models import Base, User, Survey, TestResult, ActivityLog
from .connection import get_db_sync, SessionLocal

# Настройка логирования
logger = logging.getLogger(__name__)


# ============================================================================
# ОСНОВНЫЕ ФУНКЦИИ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
# ============================================================================


def find_existing_user(telegram_id: int, email: str = None, phone: str = None):
    """Поиск пользователя по всем возможным критериям с обновлением telegram_id"""
    db = get_db_sync()
    try:
        logger.info(
            f"Ищу пользователя: telegram_id={telegram_id}, email={email}, phone={phone}"
        )

        # 1. Поиск по telegram_id (приоритет)
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        if user:
            logger.info(f"✅ Найден пользователь по telegram_id: {user.id}")
            return user

        # 2. Поиск по email
        if email and "@" in email and email != f"user_{telegram_id}@bot.com":
            user = db.query(User).filter(User.email == email).first()
            if user:
                logger.warning(
                    f"🔄 Найден пользователь по email {email}, обновляю telegram_id с {user.telegram_id} на {telegram_id}"
                )

                # Обновляем telegram_id на правильный
                old_telegram_id = user.telegram_id
                user.telegram_id = telegram_id

                # Обновляем связанные записи
                db.query(Survey).filter(Survey.telegram_id == old_telegram_id).update(
                    {Survey.telegram_id: telegram_id}
                )
                db.query(TestResult).filter(
                    TestResult.telegram_id == old_telegram_id
                ).update({TestResult.telegram_id: telegram_id})
                db.query(ActivityLog).filter(
                    ActivityLog.telegram_id == old_telegram_id
                ).update({ActivityLog.telegram_id: telegram_id})

                db.commit()
                logger.info(f"✅ Обновлен telegram_id пользователя {user.id}")
                return user

        # 3. Поиск по телефону (последние 10 цифр)
        if phone and len(phone) >= 10:
            clean_phone = "".join(filter(str.isdigit, phone))[-10:]
            users = db.query(User).all()

            for user in users:
                if user.phone:
                    user_phone = "".join(filter(str.isdigit, user.phone))[-10:]
                    if user_phone == clean_phone and len(user_phone) >= 10:
                        logger.warning(
                            f"🔄 Найден пользователь по телефону {phone}, обновляю telegram_id с {user.telegram_id} на {telegram_id}"
                        )

                        # Обновляем telegram_id на правильный
                        old_telegram_id = user.telegram_id
                        user.telegram_id = telegram_id

                        # Обновляем связанные записи
                        db.query(Survey).filter(
                            Survey.telegram_id == old_telegram_id
                        ).update({Survey.telegram_id: telegram_id})
                        db.query(TestResult).filter(
                            TestResult.telegram_id == old_telegram_id
                        ).update({TestResult.telegram_id: telegram_id})
                        db.query(ActivityLog).filter(
                            ActivityLog.telegram_id == old_telegram_id
                        ).update({ActivityLog.telegram_id: telegram_id})

                        db.commit()
                        logger.info(f"✅ Обновлен telegram_id пользователя {user.id}")
                        return user

        logger.info(f"❌ Пользователь не найден ни по одному критерию")
        return None

    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка поиска пользователя: {e}")
        return None
    finally:
        db.close()


def merge_duplicate_users():
    """Объединение дублированных пользователей"""
    db = get_db_sync()
    try:
        logger.info("=== НАЧАЛО ОБЪЕДИНЕНИЯ ДУБЛИКАТОВ ===")

        # Найти дубликаты по email
        emails_query = (
            db.query(User.email)
            .filter(
                User.email.isnot(None),
                User.email != "",
                ~User.email.like("%@bot.com"),  # Исключаем автогенерированные email
            )
            .group_by(User.email)
            .having(func.count(User.email) > 1)
            .all()
        )

        merged_count = 0

        for email_tuple in emails_query:
            email = email_tuple[0]
            if not email or "@" not in email:
                continue

            # Найти всех пользователей с этим email
            users = (
                db.query(User)
                .filter(User.email == email)
                .order_by(User.created_at)
                .all()
            )

            if len(users) > 1:
                logger.info(f"🔄 Найдено {len(users)} дубликатов для email {email}")

                # Выбираем основного пользователя (самый старый)
                main_user = users[0]
                duplicates = users[1:]

                logger.info(
                    f"Основной пользователь: ID={main_user.id}, telegram_id={main_user.telegram_id}"
                )

                for dup_user in duplicates:
                    logger.info(
                        f"Объединяю дубликат: ID={dup_user.id}, telegram_id={dup_user.telegram_id}"
                    )

                    # Переносим данные опросов
                    surveys = (
                        db.query(Survey)
                        .filter(Survey.telegram_id == dup_user.telegram_id)
                        .all()
                    )
                    for survey in surveys:
                        survey.telegram_id = main_user.telegram_id

                    # Переносим данные тестов
                    tests = (
                        db.query(TestResult)
                        .filter(TestResult.telegram_id == dup_user.telegram_id)
                        .all()
                    )
                    for test in tests:
                        test.telegram_id = main_user.telegram_id

                    # Переносим логи активности
                    activities = (
                        db.query(ActivityLog)
                        .filter(ActivityLog.telegram_id == dup_user.telegram_id)
                        .all()
                    )
                    for activity in activities:
                        activity.telegram_id = main_user.telegram_id

                    # Обновляем статусы основного пользователя
                    if dup_user.survey_completed and not main_user.survey_completed:
                        main_user.survey_completed = True
                    if dup_user.tests_completed and not main_user.tests_completed:
                        main_user.tests_completed = True
                    if (
                        dup_user.completed_diagnostic
                        and not main_user.completed_diagnostic
                    ):
                        main_user.completed_diagnostic = True

                    # Удаляем дубликат
                    db.delete(dup_user)
                    merged_count += 1

        db.commit()
        logger.info(f"✅ Объединение завершено. Удалено дубликатов: {merged_count}")
        return merged_count

    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка объединения дубликатов: {e}")
        return 0
    finally:
        db.close()


def find_existing_user_safe(telegram_id: int, email: str = None, phone: str = None):
    """ИСПРАВЛЕННАЯ функция поиска пользователя - НЕ МЕНЯЕТ telegram_id если он правильный"""
    db = get_db_sync()
    try:
        logger.info(
            f"🔍 ПОИСК пользователя: telegram_id={telegram_id}, email={email}, phone={phone}"
        )

        # 1. СНАЧАЛА точный поиск по telegram_id - ПРИОРИТЕТ!
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        if user:
            logger.info(f"✅ НАЙДЕН точно по telegram_id: {user.id}")
            return user

        # 2. Поиск по email (ТОЛЬКО если это НЕ автогенерированный email)
        if email and "@" in email and not email.endswith("@bot.com"):
            user = db.query(User).filter(User.email == email).first()
            if user:
                logger.warning(
                    f"🔄 НАЙДЕН по email, НО ПРОВЕРЯЮ какой telegram_id ПРАВИЛЬНЫЙ"
                )

                # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Определяем какой ID правильный
                old_telegram_id = user.telegram_id
                current_telegram_id = telegram_id

                # Проверяем, какой из ID выглядит как настоящий telegram_id пользователя
                # Telegram ID пользователей обычно 8-10 цифр, message_id обычно меньше

                def is_real_user_id(user_id: int) -> bool:
                    """Проверяет, является ли ID настоящим telegram_id пользователя"""
                    # Telegram user ID обычно больше 100000 и меньше 10^10
                    return 100000 <= user_id <= 9999999999

                def is_likely_message_id(msg_id: int) -> bool:
                    """Проверяет, похож ли ID на message_id"""
                    # Message ID обычно небольшие числа
                    return 1 <= msg_id <= 999999

                logger.info(f"Анализ ID:")
                logger.info(
                    f"  old_telegram_id: {old_telegram_id} (real_user: {is_real_user_id(old_telegram_id)}, msg_like: {is_likely_message_id(old_telegram_id)})"
                )
                logger.info(
                    f"  current_telegram_id: {current_telegram_id} (real_user: {is_real_user_id(current_telegram_id)}, msg_like: {is_likely_message_id(current_telegram_id)})"
                )

                # ЛОГИКА ВЫБОРА ПРАВИЛЬНОГО ID:
                correct_telegram_id = None

                if is_real_user_id(old_telegram_id) and is_likely_message_id(
                    current_telegram_id
                ):
                    # Старый ID - настоящий, новый - message_id
                    correct_telegram_id = old_telegram_id
                    logger.info("✅ СОХРАНЯЮ старый telegram_id (он правильный)")

                elif is_likely_message_id(old_telegram_id) and is_real_user_id(
                    current_telegram_id
                ):
                    # Старый ID - message_id, новый - настоящий
                    correct_telegram_id = current_telegram_id
                    logger.info("✅ ОБНОВЛЯЮ на новый telegram_id (он правильный)")

                elif is_real_user_id(old_telegram_id) and is_real_user_id(
                    current_telegram_id
                ):
                    # Оба выглядят как настоящие - сохраняем старый (принцип консерватизма)
                    correct_telegram_id = old_telegram_id
                    logger.info("🤔 ОБА ID выглядят настоящими, сохраняю СТАРЫЙ")

                else:
                    # Неопределенная ситуация - логируем и выбираем больший
                    correct_telegram_id = max(old_telegram_id, current_telegram_id)
                    logger.warning(
                        f"⚠️ НЕОПРЕДЕЛЕННАЯ ситуация, выбираю больший ID: {correct_telegram_id}"
                    )

                # Обновляем только если ID действительно изменился
                if user.telegram_id != correct_telegram_id:
                    logger.info(
                        f"🔄 ОБНОВЛЯЮ telegram_id с {user.telegram_id} на {correct_telegram_id}"
                    )

                    # Обновляем связанные записи ПЕРЕД изменением основного ID
                    old_id_for_update = user.telegram_id

                    surveys_updated = (
                        db.query(Survey)
                        .filter(Survey.telegram_id == old_id_for_update)
                        .update({Survey.telegram_id: correct_telegram_id})
                    )
                    tests_updated = (
                        db.query(TestResult)
                        .filter(TestResult.telegram_id == old_id_for_update)
                        .update({TestResult.telegram_id: correct_telegram_id})
                    )
                    activities_updated = (
                        db.query(ActivityLog)
                        .filter(ActivityLog.telegram_id == old_id_for_update)
                        .update({ActivityLog.telegram_id: correct_telegram_id})
                    )

                    logger.info(
                        f"   Обновлено связанных записей: опросы={surveys_updated}, тесты={tests_updated}, активность={activities_updated}"
                    )

                    # ТЕПЕРЬ обновляем основной telegram_id
                    user.telegram_id = correct_telegram_id

                    db.commit()
                    logger.info(f"✅ telegram_id обновлен на {correct_telegram_id}")
                else:
                    logger.info(f"✅ telegram_id уже правильный: {correct_telegram_id}")

                return user

        # 3. Поиск по телефону (аналогично исправляем)
        if phone and len(phone) >= 10:
            clean_phone = "".join(filter(str.isdigit, phone))[-10:]

            users_with_phones = (
                db.query(User)
                .filter(
                    User.phone.isnot(None),
                    ~User.phone.like("%@%"),  # Исключаем автогенерированные
                    User.phone != f"+{telegram_id}",
                )
                .all()
            )

            for user in users_with_phones:
                if user.phone:
                    user_phone = "".join(filter(str.isdigit, user.phone))[-10:]
                    if user_phone == clean_phone and len(user_phone) >= 10:
                        logger.warning(f"🔄 НАЙДЕН по телефону, проверяю telegram_id")

                        # Применяем ту же логику выбора правильного ID
                        old_telegram_id = user.telegram_id
                        current_telegram_id = telegram_id

                        # Определяем правильный ID
                        if (
                            100000 <= old_telegram_id <= 9999999999
                            and 1 <= current_telegram_id <= 999999
                        ):
                            correct_telegram_id = old_telegram_id
                        elif (
                            1 <= old_telegram_id <= 999999
                            and 100000 <= current_telegram_id <= 9999999999
                        ):
                            correct_telegram_id = current_telegram_id
                        else:
                            correct_telegram_id = (
                                old_telegram_id  # Консервативный выбор
                            )

                        if user.telegram_id != correct_telegram_id:
                            # Обновляем связанные записи
                            db.query(Survey).filter(
                                Survey.telegram_id == user.telegram_id
                            ).update({Survey.telegram_id: correct_telegram_id})
                            db.query(TestResult).filter(
                                TestResult.telegram_id == user.telegram_id
                            ).update({TestResult.telegram_id: correct_telegram_id})
                            db.query(ActivityLog).filter(
                                ActivityLog.telegram_id == user.telegram_id
                            ).update({ActivityLog.telegram_id: correct_telegram_id})

                            user.telegram_id = correct_telegram_id
                            db.commit()

                        return user

        logger.info(f"❌ Пользователь НЕ НАЙДЕН")
        return None

    except Exception as e:
        db.rollback()
        logger.error(f"❌ ОШИБКА поиска: {e}")
        return None
    finally:
        db.close()


async def safe_save_user_data(
    telegram_id: int, name: str = None, email: str = None, phone: str = None
):
    """ИСПРАВЛЕННАЯ функция сохранения с правильным определением telegram_id"""

    # ПРОВЕРЯЕМ входящий telegram_id
    if not isinstance(telegram_id, int):
        logger.error(
            f"❌ telegram_id не является int: {telegram_id}, type: {type(telegram_id)}"
        )
        raise ValueError(f"telegram_id должен быть int, получен {type(telegram_id)}")

    if telegram_id <= 0:
        logger.error(f"❌ Некорректный telegram_id: {telegram_id}")
        raise ValueError(
            f"telegram_id должен быть положительным числом, получен {telegram_id}"
        )

    # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: это message_id или user_id?
    def is_likely_user_id(user_id: int) -> bool:
        """Проверяет, является ли ID telegram_id пользователя"""
        return 100000 <= user_id <= 9999999999

    if not is_likely_user_id(telegram_id):
        logger.error(
            f"❌ ПОДОЗРИТЕЛЬНЫЙ telegram_id: {telegram_id} - возможно это message_id!"
        )
        # В этом случае нужно получить правильный user_id из контекста
        raise ValueError(
            f"Подозрительный telegram_id: {telegram_id}. Проверьте, что передается from_user.id, а не message_id"
        )

    print("=" * 80)
    print("💾 ИСПРАВЛЕННОЕ СОХРАНЕНИЕ В БД")
    print(f"💾 ВХОДЯЩИЙ telegram_id: {telegram_id}")
    print(f"💾 Проверка user_id: {is_likely_user_id(telegram_id)}")
    print(f"💾 name: {name}")
    print(f"💾 email: {email}")
    print(f"💾 phone: {phone}")
    print("=" * 80)

    def _save():
        db = get_db_sync()
        try:
            current_time = datetime.now()

            logger.info(f"🔍 ИСПРАВЛЕННОЕ сохранение для telegram_id = {telegram_id}")

            # ПОИСК с исправленной логикой
            existing_user = find_existing_user_safe(telegram_id, email, phone)

            if existing_user:
                logger.info(f"✅ Найден существующий пользователь:")
                logger.info(f"   ID в БД: {existing_user.id}")
                logger.info(f"   ФИНАЛЬНЫЙ telegram_id: {existing_user.telegram_id}")

                # Обновляем данные (НЕ меняем telegram_id - он уже правильный)
                if name and name != f"User_{telegram_id}":
                    existing_user.name = name
                if email and email != f"user_{telegram_id}@bot.com":
                    existing_user.email = email
                if phone and phone != f"+{telegram_id}":
                    existing_user.phone = phone

                existing_user.updated_at = current_time
                existing_user.last_activity = current_time
                existing_user.registration_completed = True
                user = existing_user

            else:
                logger.info(
                    f"🆕 СОЗДАЮ НОВОГО пользователя с telegram_id: {telegram_id}"
                )

                user = User(
                    telegram_id=telegram_id,
                    name=name or f"User_{telegram_id}",
                    email=email or f"user_{telegram_id}@bot.com",
                    phone=phone or f"+{telegram_id}",
                    completed_diagnostic=False,
                    registration_completed=True,
                    survey_completed=False,
                    tests_completed=False,
                    created_at=current_time,
                    updated_at=current_time,
                    last_activity=current_time,
                )
                db.add(user)

            # ФИНАЛЬНАЯ ПРОВЕРКА
            logger.info(f"🔍 ПЕРЕД COMMIT:")
            logger.info(f"   user.telegram_id: {user.telegram_id}")
            logger.info(f"   ожидаемый: {telegram_id}")
            logger.info(f"   корректность: {is_likely_user_id(user.telegram_id)}")

            # Логируем операцию
            log_entry = ActivityLog(
                telegram_id=user.telegram_id,  # Используем финальный правильный ID
                action="user_saved_fixed",
                details=json.dumps(
                    {
                        "method": "fixed_save",
                        "input_telegram_id": telegram_id,
                        "final_telegram_id": user.telegram_id,
                        "is_user_id": is_likely_user_id(user.telegram_id),
                    },
                    ensure_ascii=False,
                ),
                state="fixed_registration",
            )
            db.add(log_entry)

            logger.info("🔍 ВЫПОЛНЯЮ COMMIT...")
            db.commit()
            logger.info("✅ COMMIT ВЫПОЛНЕН")

            # ФИНАЛЬНАЯ ВЕРИФИКАЦИЯ
            verification = (
                db.query(User).filter(User.telegram_id == user.telegram_id).first()
            )
            if verification:
                logger.info(f"✅ ВЕРИФИКАЦИЯ УСПЕШНА:")
                logger.info(f"   Пользователь сохранен с ID: {verification.id}")
                logger.info(f"   telegram_id: {verification.telegram_id}")
                logger.info(f"   name: {verification.name}")
            else:
                logger.error(f"❌ ВЕРИФИКАЦИЯ ПРОВАЛЕНА!")

            return user

        except Exception as e:
            db.rollback()
            logger.error(f"❌ ОШИБКА при сохранении: {e}")
            raise e
        finally:
            db.close()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)


async def mark_user_completed(telegram_id: int):
    """Улучшенная отметка пользователя как завершившего диагностику"""

    def _mark():
        db = get_db_sync()
        try:
            current_time = datetime.now()

            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if user:
                user.completed_diagnostic = True
                user.last_activity = current_time
                user.updated_at = current_time

                # Проверяем полноту данных пользователя
                completion_stats = {
                    "registration_completed": user.registration_completed,
                    "survey_completed": user.survey_completed,
                    "tests_completed": user.tests_completed,
                    "diagnostic_completed": True,
                }

                logger.info(f"✅ Пользователь {telegram_id} завершил диагностику")
                logger.info(f"   Статистика завершения: {completion_stats}")

                # Логируем завершение
                log_entry = ActivityLog(
                    telegram_id=telegram_id,
                    action="diagnostic_completed",
                    details=json.dumps(completion_stats, ensure_ascii=False),
                    state="completed",
                    timestamp=current_time,
                )
                db.add(log_entry)

                db.commit()
                return {"success": True, "completion_stats": completion_stats}
            else:
                logger.warning(f"Пользователь {telegram_id} не найден")
                return {"success": False, "error": "User not found"}

        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка отметки завершения для {telegram_id}: {e}")
            return {"success": False, "error": str(e)}
        finally:
            db.close()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _mark)


# ============================================================================
# ФУНКЦИИ ПОЛУЧЕНИЯ ДАННЫХ
# ============================================================================


def check_user_completed(telegram_id: int) -> bool:
    """Проверить, завершил ли пользователь диагностику"""
    db = get_db_sync()
    try:
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        return user.completed_diagnostic if user else False
    finally:
        db.close()


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


# ============================================================================
# ЛОГИРОВАНИЕ АКТИВНОСТИ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================================


async def log_user_activity(
    telegram_id: int, action: str, details: Dict[str, Any] = None, state: str = None
):
    """Логирование активности пользователя с детальной информацией"""

    def _log():
        db = get_db_sync()
        try:
            current_time = datetime.now()

            # Обновляем последнюю активность пользователя
            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if user:
                user.last_activity = current_time
                user.updated_at = current_time

            # Создаем запись в логе активности
            log_entry = ActivityLog(
                telegram_id=telegram_id,
                action=action,
                details=json.dumps(details or {}, ensure_ascii=False),
                state=state,
                timestamp=current_time,
            )
            db.add(log_entry)

            db.commit()
            return log_entry.id

        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка логирования активности {telegram_id}: {e}")
            raise e
        finally:
            db.close()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _log)


# ============================================================================
# УЛУЧШЕННЫЕ ФУНКЦИИ ПОЛУЧЕНИЯ СТАТИСТИКИ
# ============================================================================


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
            "avg_completion_time_hours": round(avg_completion_time, 2),
        }
    finally:
        db.close()


# ============================================================================
# ВАЛИДАЦИЯ ДАННЫХ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================================


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
# УТИЛИТЫ ДЛЯ РАБОТЫ С ДАННЫМИ
# ============================================================================


def clean_user_data(telegram_id: int) -> Dict[str, Any]:
    """Полная очистка данных пользователя"""
    db = get_db_sync()
    try:
        deleted_records = {}

        # Удаляем записи из всех связанных таблиц
        tables = [
            ("test_results", TestResult),
            ("surveys", Survey),
            ("activity_logs", ActivityLog),
            ("users", User),
        ]

        for table_name, model in tables:
            count = db.query(model).filter(model.telegram_id == telegram_id).count()
            db.query(model).filter(model.telegram_id == telegram_id).delete()
            deleted_records[table_name] = count

        db.commit()

        total_deleted = sum(deleted_records.values())

        return {
            "success": True,
            "telegram_id": telegram_id,
            "deleted_records": deleted_records,
            "total_deleted": total_deleted,
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка очистки данных пользователя {telegram_id}: {e}")
        return {"success": False, "error": str(e)}
    finally:
        db.close()


def anonymize_user_data(days_old: int = 365) -> Dict[str, Any]:
    """Анонимизация старых данных пользователей"""
    db = get_db_sync()
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)

        # Находим пользователей для анонимизации
        old_users = (
            db.query(User)
            .filter(
                User.last_activity < cutoff_date, User.completed_diagnostic == False
            )
            .all()
        )

        anonymized_count = 0

        for user in old_users:
            # Заменяем персональные данные на анонимные
            user.name = f"Anon_{user.id}"
            user.email = f"anon_{user.id}@deleted.com"
            user.phone = f"+000{user.id}"
            anonymized_count += 1

        db.commit()

        return {
            "success": True,
            "anonymized_users": anonymized_count,
            "cutoff_date": cutoff_date.isoformat(),
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка анонимизации данных: {e}")
        return {"success": False, "error": str(e)}
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
                t.overall_risk_level as risk_level
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
                t.overall_risk_level,
                t.hads_anxiety_score,
                t.hads_depression_score
            FROM users u
            LEFT JOIN surveys s ON u.telegram_id = s.telegram_id
            LEFT JOIN test_results t ON u.telegram_id = t.telegram_id
            WHERE u.registration_completed = 1
            ORDER BY u.created_at DESC
            """
            filename = f"analytics_export_{timestamp}.csv"

        else:
            raise ValueError(f"Неподдерживаемый формат экспорта: {format_type}")

        # Выполняем запрос и сохраняем в CSV
        df = pd.read_sql_query(query, db.bind)
        df.to_csv(filename, index=False, encoding="utf-8")

        logger.info(f"✅ Экспорт завершен: {filename} ({len(df)} записей)")
        return filename

    except Exception as e:
        logger.error(f"Ошибка экспорта: {e}")
        raise e
    finally:
        db.close()