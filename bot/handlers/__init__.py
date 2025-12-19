"""
Handlers module for cardio bot

Этот модуль содержит все обработчики бота, разделенные на логические группы:

- base: Базовые классы, состояния FSM и вспомогательные функции
- start: Стартовые команды и основная навигация
- registration: Регистрация пользователей (имя, email, телефон)
- tests: Психологические тесты (HADS, Burns, ISI и др.)
- score2: SCORE2 калькулятор сердечно-сосудистого риска (команда /score)
"""

from aiogram import Router
from . import start, registration, tests, score2, fallback
from .base import UserStates, setup_bot_commands, StateProtectionMiddleware, DebugStateProtectionMiddleware

def get_handlers_router() -> Router:
    """
    Создает и возвращает главный роутер со всеми обработчиками
    ВАЖНО: порядок подключения роутеров имеет значение!
    """
    main_router = Router()

    # Регистрируем middleware для защиты состояний
    main_router.message.middleware(StateProtectionMiddleware())
    main_router.callback_query.middleware(StateProtectionMiddleware())

    # Подключаем роутеры модулей в правильном порядке
    main_router.include_router(start.router)            # Команды и стартовая логика
    main_router.include_router(score2.score2_router)    # SCORE2 калькулятор сердечно-сосудистого риска
    main_router.include_router(registration.router)     # Регистрация (имя, email, телефон)
    main_router.include_router(tests.router)            # Психологические тесты
    main_router.include_router(fallback.router)         # ПОСЛЕДНИЙ - неизвестные сообщения

    return main_router

# Создаем главный роутер и middleware для обратной совместимости
main_router = get_handlers_router()
state_protection = DebugStateProtectionMiddleware()

# Экспортируем основные компоненты
__all__ = [
    'get_handlers_router',
    'main_router',
    'state_protection',
    'UserStates',
    'setup_bot_commands',
    'StateProtectionMiddleware',
    'DebugStateProtectionMiddleware',
]