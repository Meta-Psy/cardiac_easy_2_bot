import asyncio
import logging
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BotCommand, BotCommandScopeDefault
from database import log_user_activity

# Настройка логирования
logger = logging.getLogger(__name__)

class UserStates(StatesGroup):
    waiting_start = State()
    waiting_name = State()
    waiting_email = State()
    waiting_phone = State()
    
    # Опрос удален - переходим сразу к тестам
    
    # Тесты состояния
    test_selection = State()
    hads_test = State()
    burns_test = State()
    isi_test = State()
    stop_bang_test = State()
    ess_test = State()
    fagerstrom_test = State()
    audit_test = State()

COMMANDS = [
    BotCommand(command="start", description="🚀 Начать диагностику"),
    BotCommand(command="score", description="📊 Калькулятор SCORE2"),
]

async def setup_bot_commands(bot):
    """Установка команд бота в меню"""
    await bot.set_my_commands(COMMANDS, BotCommandScopeDefault())

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ЗАЩИТЫ СОСТОЯНИЙ
# ============================================================================

async def safe_edit_message(message, text, parse_mode="HTML", reply_markup=None, max_retries=3):
    """Безопасное редактирование сообщения с повторными попытками"""
    for attempt in range(max_retries):
        try:
            await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
            return True
        except Exception as e:
            if "message is not modified" in str(e):
                return True  # Сообщение уже такое
            if attempt == max_retries - 1:
                # Последняя попытка - отправляем новое сообщение
                try:
                    await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
                    return True
                except:
                    return False
            await asyncio.sleep(0.5)
    return False

async def safe_answer_callback(callback, text="", show_alert=False, max_retries=2):
    """Безопасный ответ на callback"""
    for attempt in range(max_retries):
        try:
            await callback.answer(text, show_alert=show_alert)
            return True
        except Exception as e:
            if "query is too old" in str(e) or "QUERY_ID_INVALID" in str(e):
                return True  # Игнорируем устаревшие callback'и
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(0.3)
    return False

async def log_user_interaction(user_id: int, action: str, details: str = None):
    """Логирование взаимодействий пользователя"""
    try:
        await log_user_activity(
            telegram_id=user_id,
            action=action,
            details={"interaction": details} if details else {}
        )
    except Exception as e:
        logger.warning(f"Не удалось залогировать активность пользователя {user_id}: {e}")

# ============================================================================
# MIDDLEWARE КЛАССЫ
# ============================================================================

class StateProtectionMiddleware:
    """Middleware для защиты состояний FSM от некорректных переходов"""
    
    def __init__(self):
        # Карта разрешенных переходов: текущее_состояние -> [список_разрешенных_состояний]
        self.allowed_transitions = {
            # Начальные состояния
            None: [UserStates.waiting_start],
            UserStates.waiting_start: [UserStates.waiting_name],
            
            # Регистрация
            UserStates.waiting_name: [UserStates.waiting_email],
            UserStates.waiting_email: [UserStates.waiting_phone],
            UserStates.waiting_phone: [UserStates.test_selection],
            
            # Тесты
            UserStates.test_selection: [
                UserStates.hads_test, UserStates.burns_test, UserStates.isi_test, 
                UserStates.stop_bang_test, UserStates.ess_test, UserStates.fagerstrom_test, UserStates.audit_test
            ],
            # Из любого теста можно вернуться к выбору тестов
            UserStates.hads_test: [UserStates.test_selection],
            UserStates.burns_test: [UserStates.test_selection],
            UserStates.isi_test: [UserStates.test_selection],
            UserStates.stop_bang_test: [UserStates.test_selection],
            UserStates.ess_test: [UserStates.test_selection],
            UserStates.fagerstrom_test: [UserStates.test_selection],
            UserStates.audit_test: [UserStates.test_selection],
        }
        
        # Состояния, которые могут переходить к любому (экстренные ситуации)
        self.emergency_states = [UserStates.waiting_start]
        
        # Специальные разрешения для команд
        self.command_allowed_states = {
            'start': [None],  # /start только из пустого состояния
            'score': 'all',   # /score из любого состояния
        }

    async def __call__(self, handler, event, data):
        """Middleware обработчик"""
        return await handler(event, data)

class DebugStateProtectionMiddleware(StateProtectionMiddleware):
    """Debug версия middleware с логированием переходов"""
    
    async def __call__(self, handler, event, data):
        if hasattr(event, 'from_user'):
            user_id = event.from_user.id
            state = data.get('state')
            current_state = await state.get_state() if state else None
            
            logger.info(f"User {user_id}: Current state: {current_state}, Event: {type(event).__name__}")
            
        return await handler(event, data)