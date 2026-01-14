"""
Модуль для рассылки опроса ТОЧКА 3 (через 3+ месяца после вебинара)
Реализует рассылку начального сообщения с кнопкой "Начать опрос"
"""
import asyncio
import logging
from datetime import datetime
from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database import get_db_sync, User, FollowUpStatus

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# ТЕКСТ НАЧАЛЬНОГО СООБЩЕНИЯ ИЗ ТЗ
# ============================================================================

FOLLOWUP_INITIAL_MESSAGE = """Друзья, важное объявление🙏

Скорее всего вы уже принимали участие в нашем опросе, но из-за технического сбоя часть ответов, к сожалению, не сохранилась.
Мы понимаем ценность вашего времени и будем искренне благодарны, если вы сможете заполнить его ещё раз.

🎁 В знак благодарности за ваше время после прохождения опроса вы получите полезный бонус — <b>«Руководство для женщин 40+: как защитить сердце в перименопаузе и менопаузе»</b>, подготовленное на основе самых актуальных научных данных и рекомендаций.

Этот опрос — часть нашей большой миссии.
Мы изучаем, как социальные сети помогают повышать информированность и приверженность к выполнению медицинских рекомендаций, чтобы снижать риски сердечно-сосудистых заболеваний и сохранять жизни.

*Опрос полностью конфиденциален.
Все ответы анонимны и обрабатываются в строгом соответствии с законодательством РФ. Данные не передаются третьим лицам.

⚕️ Ваше участие — это вклад в системные изменения.
Именно на основе таких данных мы можем говорить с медицинским сообществом и системой здравоохранения на языке фактов и влиять на подходы к профилактике и лечению сердечно-сосудистых заболеваний в масштабах страны.

Спасибо, что поддерживаете проект, который действительно работает на здоровье людей 🤍"""

# ============================================================================
# КЛАВИАТУРА ДЛЯ НАЧАЛЬНОГО СООБЩЕНИЯ
# ============================================================================

def get_followup_start_keyboard():
    """Клавиатура с кнопкой 'Начать опрос'"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Начать опрос", callback_data="start_followup_survey")]
    ])
    return keyboard

# ============================================================================
# ФУНКЦИЯ РАССЫЛКИ
# ============================================================================

async def broadcast_followup_initial_message(bot: Bot, user_id: int) -> bool:
    """
    Отправка начального сообщения ТОЧКА 3 конкретному пользователю

    Args:
        bot: Экземпляр бота
        user_id: Telegram ID пользователя

    Returns:
        bool: True если отправка успешна, False иначе
    """
    try:
        # Проверяем, не отправляли ли уже этому пользователю
        def _check_status():
            db = get_db_sync()
            try:
                status = db.query(FollowUpStatus).filter(FollowUpStatus.telegram_id == user_id).first()
                if status and status.initial_message_sent:
                    return False  # Уже отправляли
                return True  # Можно отправлять
            finally:
                db.close()

        can_send = await asyncio.get_event_loop().run_in_executor(None, _check_status)

        if not can_send:
            logger.info(f"Пользователю {user_id} уже отправлено начальное сообщение ТОЧКА 3")
            return False

        # Отправляем сообщение
        keyboard = get_followup_start_keyboard()
        await bot.send_message(
            user_id,
            FOLLOWUP_INITIAL_MESSAGE,
            parse_mode="HTML",
            reply_markup=keyboard
        )

        # Отмечаем отправку в БД
        def _mark_sent():
            db = get_db_sync()
            try:
                status = db.query(FollowUpStatus).filter(FollowUpStatus.telegram_id == user_id).first()
                if status:
                    status.initial_message_sent = True
                    status.initial_message_sent_at = datetime.now()
                else:
                    status = FollowUpStatus(
                        telegram_id=user_id,
                        initial_message_sent=True,
                        initial_message_sent_at=datetime.now()
                    )
                    db.add(status)
                db.commit()
            except Exception as e:
                logger.error(f"Ошибка отметки отправки для {user_id}: {e}")
            finally:
                db.close()

        await asyncio.get_event_loop().run_in_executor(None, _mark_sent)

        logger.info(f"Начальное сообщение ТОЧКА 3 отправлено пользователю {user_id}")
        return True

    except Exception as e:
        logger.error(f"Ошибка отправки начального сообщения ТОЧКА 3 пользователю {user_id}: {e}")
        return False

async def broadcast_followup_to_all(bot: Bot) -> dict:
    """
    Массовая рассылка начального сообщения ТОЧКА 3 всем пользователям

    Args:
        bot: Экземпляр бота

    Returns:
        dict: Статистика рассылки {total, sent, failed}
    """
    def _get_users():
        db = get_db_sync()
        try:
            # Получаем всех зарегистрированных пользователей
            users = db.query(User).filter(
                User.registration_completed == True,
                User.telegram_id >= 100000,
                User.telegram_id <= 9999999999
            ).all()

            user_ids = [user.telegram_id for user in users]

            # Исключаем тех, кому уже отправили
            statuses = db.query(FollowUpStatus).filter(
                FollowUpStatus.initial_message_sent == True
            ).all()

            already_sent = {status.telegram_id for status in statuses}

            # Фильтруем
            target_ids = [uid for uid in user_ids if uid not in already_sent]

            return target_ids, len(user_ids)

        finally:
            db.close()

    # Получаем список пользователей
    target_ids, total_users = await asyncio.get_event_loop().run_in_executor(None, _get_users)

    logger.info(f"Начинаю рассылку ТОЧКА 3 для {len(target_ids)} пользователей из {total_users}")

    sent = 0
    failed = 0

    for user_id in target_ids:
        success = await broadcast_followup_initial_message(bot, user_id)
        if success:
            sent += 1
        else:
            failed += 1

        # Небольшая задержка между отправками
        await asyncio.sleep(0.05)

    logger.info(f"Рассылка ТОЧКА 3 завершена: отправлено {sent}/{len(target_ids)}, ошибок {failed}")

    return {
        'total': len(target_ids),
        'sent': sent,
        'failed': failed,
        'total_users': total_users
    }
