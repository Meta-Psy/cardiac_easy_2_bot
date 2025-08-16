"""
Fallback handler для неизвестных сообщений
Этот модуль должен подключаться ПОСЛЕДНИМ в цепочке роутеров
"""
import logging
from aiogram import Router, F
from aiogram.types import Message
from aiogram.fsm.context import FSMContext

from .base import log_user_interaction

logger = logging.getLogger(__name__)

router = Router()

@router.message()
async def handle_unknown_message(message: Message, state: FSMContext):
    """Обработчик неизвестных сообщений (подключается ПОСЛЕДНИМ)"""
    await log_user_interaction(message.from_user.id, "unknown_message", message.text)
    
    current_state = await state.get_state()
    
    # Если пользователь в процессе диагностики, направляем его
    if current_state:
        if "waiting_name" in current_state:
            text = """❌ Пожалуйста, введите ваше имя для продолжения регистрации."""
        elif "waiting_email" in current_state:
            text = """❌ Пожалуйста, введите ваш email для продолжения регистрации."""
        elif "waiting_phone" in current_state:
            text = """❌ Пожалуйста, поделитесь номером телефона, используя кнопку ниже."""
        elif "survey" in current_state:
            text = """❌ Пожалуйста, ответьте на текущий вопрос опроса, используя кнопки."""
        elif "test" in current_state:
            text = """❌ Пожалуйста, ответьте на вопрос теста, используя кнопки."""
        else:
            text = """❌ Неизвестная команда. Используйте /help для помощи или /status для проверки прогресса."""
    else:
        # Пользователь не в процессе диагностики
        text = """❌ Неизвестная команда.

📋 <b>Доступные команды:</b>
/start - Начать диагностику
/help - Получить помощь
/status - Проверить статус
/restart - Начать заново
/score - SCORE2 калькулятор риска"""
    
    await message.answer(text, parse_mode="HTML")