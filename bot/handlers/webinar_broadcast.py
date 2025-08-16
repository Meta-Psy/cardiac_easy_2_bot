"""
Модуль для обработки рассылки после вебинара (ТОЧКА 2)
Реализует логику согласно ТЗ: рассылка, обработка ответов, напоминания
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext

from .base import UserStates, safe_edit_message, safe_answer_callback, log_user_interaction
from database import get_db_sync, User, WebinarStatus, WebinarSurvey

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# ФУНКЦИИ РАССЫЛКИ
# ============================================================================

async def send_initial_webinar_message(bot: Bot, chat_id: int) -> bool:
    """
    Отправляет первое сообщение о просмотре вебинара (Сообщение 1 из ТЗ)
    ВАЖНО: используем chat_id для совместимости со старой системой
    """
    try:
        text = """Здравствуйте! На связи врачи-кардиологи Диана Сергеевна И Елена Васильевна.
🫀 Напоминаем, что запись вебинара «Умный кардиочекап» уже доступна.
Мы разобрали, как выявить реальные риски сердечно-сосудистых заболеваний до симптомов и катастроф, даже если «анализы в норме» и «ничего не беспокоит».
💬 Вопрос только один:
А вы уже успели посмотреть запись?

Если посмотрите в ближайшие 2 дня — получите ценный бонус в подарок."""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔘 Да, посмотрел(а)", callback_data="webinar_watched_yes")],
            [InlineKeyboardButton(text="🔘 Нет, ещё не успел(а)", callback_data="webinar_watched_no")]
        ])

        await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
        
        # Обновляем статус в БД
        await update_webinar_status(chat_id, {
            'initial_message_sent': True,
            'initial_message_sent_at': datetime.now()
        })
        
        logger.info(f"✅ Отправлено первое сообщение о вебинаре пользователю {chat_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки первого сообщения пользователю {chat_id}: {e}")
        return False

async def broadcast_initial_message_to_all(bot: Bot) -> Dict[str, int]:
    """
    Рассылка первого сообщения всем пользователям в БД
    Возвращает статистику отправки
    """
    stats = {"sent": 0, "failed": 0, "total": 0}
    
    def _get_users():
        db = get_db_sync()
        try:
            # Получаем всех пользователей, ИСПОЛЬЗУЯ CHAT_ID (telegram_id)
            users = db.query(User).filter(
                User.registration_completed == True
            ).all()
            return [(user.telegram_id, user.name) for user in users]
        finally:
            db.close()
    
    users = await asyncio.get_event_loop().run_in_executor(None, _get_users)
    stats["total"] = len(users)
    
    logger.info(f"🚀 Начинаем рассылку первого сообщения {stats['total']} пользователям")
    
    for chat_id, name in users:
        try:
            # Проверяем, не отправляли ли уже
            status = await get_webinar_status(chat_id)
            if status and status.get('initial_message_sent'):
                logger.info(f"⏭ Пользователю {chat_id} ({name}) уже отправлено, пропускаем")
                stats["sent"] += 1
                continue
            
            success = await send_initial_webinar_message(bot, chat_id)
            if success:
                stats["sent"] += 1
                await log_user_interaction(chat_id, "webinar_initial_message_sent")
            else:
                stats["failed"] += 1
                await log_user_interaction(chat_id, "webinar_initial_message_failed")
            
            # Пауза между отправками
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logger.error(f"❌ Ошибка рассылки пользователю {chat_id}: {e}")
            stats["failed"] += 1
    
    logger.info(f"✅ Рассылка завершена. Отправлено: {stats['sent']}, Ошибок: {stats['failed']}")
    return stats

# ============================================================================
# ОБРАБОТЧИКИ КНОПОК
# ============================================================================

@router.callback_query(F.data == "webinar_watched_yes")
async def handle_webinar_watched_yes(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия 'Да, посмотрел(а)'"""
    await safe_answer_callback(callback)
    chat_id = callback.message.chat.id
    
    await log_user_interaction(chat_id, "webinar_watched_yes")
    
    # Обновляем статус
    await update_webinar_status(chat_id, {
        'watched_webinar': True,
        'user_response_at': datetime.now()
    })
    
    # Отправляем сообщение перед опросом (Вариант 2.1 из ТЗ)
    text = """Вы сделали важный шаг: узнали, как оценить риски сердечно-сосудистых заболеваний, какие обследования действительно нужны, и как действовать, чтобы сохранить здоровье — своё и близких.
📋 Теперь просим вас пройти короткий финальный опрос. Для нас важно понять, что было для вас полезным, и узнать ваше отношение к профилактическому обследованию сердечно-сосудистой системы после вебинара.
🎁 В подарок за прохождение опроса — памятка «Тревожные звоночки: как проявляются инфаркт и инсульт у женщин и мужчин — типичные и неожиданные симптомы»."""
    
    await safe_edit_message(callback.message, text)
    
    # Задержка 5 секунд как указано в ТЗ
    await asyncio.sleep(5)
    
    # Запускаем опрос ТОЧКА 2
    await start_webinar_survey(callback.message, state)

@router.callback_query(F.data == "webinar_watched_no")
async def handle_webinar_watched_no(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия 'Нет, ещё не успел(а)'"""
    await safe_answer_callback(callback)
    chat_id = callback.message.chat.id
    
    await log_user_interaction(chat_id, "webinar_watched_no")
    
    # Обновляем статус
    await update_webinar_status(chat_id, {
        'watched_webinar': False,
        'user_response_at': datetime.now(),
        'reminder_scheduled': True
    })
    
    # Отправляем сообщение 2.1 из ТЗ
    text = """⏳ Ничего страшного! Запись вебинара «Умный кардиочекап» доступна — и вы ещё можете её посмотреть в удобное время.

💡 А чтобы получить бонусный материал — памятку «Тревожные звоночки: как проявляются инфаркт и инсульт у женщин и мужчин — типичные и неожиданные симптомы», 

просто посмотрите вебинар в любое удобное для вас время. По окончанию просмотра, у вас будет возможность забрать свой бонус!

🎁 Бонус будет доступен ещё 2 дня. Успейте воспользоваться!
📽 Посмотреть запись:
https://novikova-diana.ru/kardiochekup_record"""
    
    await safe_edit_message(callback.message, text)

# ============================================================================
# НАПОМИНАНИЯ
# ============================================================================

async def send_reminder_message(bot: Bot, chat_id: int) -> bool:
    """Отправляет напоминание через сутки (Сообщение 2.2 из ТЗ)"""
    try:
        text = """🔔 Вы уже успели посмотреть вебинар «Умный Кардиочекап»?
Если вы уже посмотрели — просто нажмите кнопку ниже, и мы пришлём вам бонус.
Если ещё не успели — напомним, что запись доступна, и бонус вы тоже сможете получить после просмотра.
👇 Выберите ваш вариант:"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я посмотрел(а)", callback_data="reminder_watched_yes")],
            [InlineKeyboardButton(text="⏳ Ещё не успел(а), пришлите ссылку ещё раз", callback_data="reminder_watched_no")]
        ])

        await bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)
        
        # Обновляем статус
        await update_webinar_status(chat_id, {
            'reminder_sent': True,
            'reminder_sent_at': datetime.now()
        })
        
        logger.info(f"✅ Отправлено напоминание пользователю {chat_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки напоминания пользователю {chat_id}: {e}")
        return False

@router.callback_query(F.data == "reminder_watched_yes")
async def handle_reminder_watched_yes(callback: CallbackQuery, state: FSMContext):
    """Обработка 'Я посмотрел(а)' в напоминании"""
    await safe_answer_callback(callback)
    chat_id = callback.message.chat.id
    
    await log_user_interaction(chat_id, "reminder_watched_yes")
    
    # Обновляем статус
    await update_webinar_status(chat_id, {
        'watched_webinar': True,
        'user_response_at': datetime.now()
    })
    
    # Запускаем опрос
    await start_webinar_survey(callback.message, state)

@router.callback_query(F.data == "reminder_watched_no")
async def handle_reminder_watched_no(callback: CallbackQuery, state: FSMContext):
    """Обработка 'Ещё не успел(а)' в напоминании"""
    await safe_answer_callback(callback)
    chat_id = callback.message.chat.id
    
    await log_user_interaction(chat_id, "reminder_watched_no_again")
    
    # Отправляем ссылку еще раз
    text = """📽 Вот ссылка на запись вебинара:
https://novikova-diana.ru/kardiochekup_record

🎁 Не забудьте про бонус после просмотра!"""
    
    await safe_edit_message(callback.message, text)

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БД
# ============================================================================

async def get_webinar_status(chat_id: int) -> Dict:
    """Получает статус пользователя по вебинару"""
    def _get():
        db = get_db_sync()
        try:
            status = db.query(WebinarStatus).filter(WebinarStatus.telegram_id == chat_id).first()
            if not status:
                return {}
            return {
                'initial_message_sent': status.initial_message_sent,
                'watched_webinar': status.watched_webinar,
                'reminder_scheduled': status.reminder_scheduled,
                'reminder_sent': status.reminder_sent,
            }
        finally:
            db.close()
    
    return await asyncio.get_event_loop().run_in_executor(None, _get)

async def update_webinar_status(chat_id: int, updates: Dict) -> bool:
    """Обновляет статус пользователя по вебинару"""
    def _update():
        db = get_db_sync()
        try:
            status = db.query(WebinarStatus).filter(WebinarStatus.telegram_id == chat_id).first()
            
            if not status:
                # Создаем новую запись
                status = WebinarStatus(telegram_id=chat_id, **updates)
                db.add(status)
            else:
                # Обновляем существующую
                for key, value in updates.items():
                    if hasattr(status, key):
                        setattr(status, key, value)
            
            db.commit()
            return True
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка обновления статуса вебинара: {e}")
            return False
        finally:
            db.close()
    
    return await asyncio.get_event_loop().run_in_executor(None, _update)

async def get_users_for_reminders() -> List[int]:
    """Получает список пользователей, которым нужно отправить напоминания"""
    def _get():
        db = get_db_sync()
        try:
            # Ищем пользователей, которые ответили "нет" сутки назад
            cutoff_time = datetime.now() - timedelta(days=1)
            
            users = db.query(WebinarStatus).filter(
                WebinarStatus.watched_webinar == False,
                WebinarStatus.user_response_at <= cutoff_time,
                WebinarStatus.reminder_sent == False,
                WebinarStatus.reminder_scheduled == True
            ).all()
            
            return [user.telegram_id for user in users]
        finally:
            db.close()
    
    return await asyncio.get_event_loop().run_in_executor(None, _get)

# ============================================================================
# ОПРОС ТОЧКА 2 (заглушка - реализуется в следующем модуле)
# ============================================================================

async def start_webinar_survey(message: Message, state: FSMContext):
    """Запускает опрос ТОЧКА 2"""
    # Импортируем функцию из webinar_survey модуля
    from .webinar_survey import start_webinar_survey as start_survey
    await start_survey(message, state)