import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
# Proxy imports удалены - добавить при необходимости

from core.config import BOT_TOKEN, ADMIN_PASSWORD, PROXY_URL, ADMIN_IDS, check_environment, print_startup_banner
from core.logging_setup import setup_logging
from core.middleware import AdminMiddleware, SimpleDiagnosticMiddleware

from database import init_db, ensure_database_exists, validate_data_integrity
from handlers import main_router, state_protection
from admin import admin_router
from broadcast import BroadcastScheduler

# Настройка логирования
setup_logging()
import logging
logger = logging.getLogger(__name__)

async def create_bot():
    """Создание бота с базовыми настройками"""
    try:
        bot = Bot(
            token=BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
        )
        
        # Если нужен proxy, можно добавить позже через environment переменные
        if PROXY_URL:
            logger.info(f"PROXY_URL настроен: {PROXY_URL}")
            logger.warning("Proxy поддержка требует дополнительной настройки")
        
        return bot
        
    except Exception as e:
        logger.error(f"Ошибка создания бота: {e}")
        raise

async def create_bot_with_retry():
    """Создание бота с повторными попытками"""
    
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            logger.info(f"Попытка создания бота ({attempt + 1}/{max_attempts})...")
            bot = await create_bot()
            logger.info("УСПЕХ: Бот создан")
            return bot
        
        except Exception as e:
            logger.error(f"Попытка {attempt + 1} неудачна: {e}")
            if attempt < max_attempts - 1:
                await asyncio.sleep(2)  # Пауза перед следующей попыткой
            else:
                logger.error("Все попытки создания бота исчерпаны!")
                raise

async def setup_commands(bot):
    """Настройка команд бота"""
    try:
        from aiogram.types import BotCommand, BotCommandScopeDefault
        
        commands = [
            BotCommand(command="start", description="🚀 Начать диагностику"),
            BotCommand(command="score", description="🚀 Пройти тест SCORE"),
            BotCommand(command="help", description="❓ Помощь и инструкции"),
            BotCommand(command="restart", description="🔄 Начать заново"),
        ]
        
        await bot.set_my_commands(commands, BotCommandScopeDefault())
        logger.info("УСПЕХ: Команды бота настроены")
    except Exception as e:
        logger.warning(f"Не удалось настроить команды: {e}")

async def test_bot_connection(bot: Bot, max_retries: int = 3):
    """Тестирование подключения к Telegram с retry"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Попытка подключения к Telegram ({attempt + 1}/{max_retries})...")
            me = await bot.get_me()
            logger.info(f"УСПЕХ: Подключение к Telegram. Бот: @{me.username}")
            return True
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1} неудачна: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                logger.info(f"Жду {wait_time} секунд перед следующей попыткой...")
                await asyncio.sleep(wait_time)
    
    logger.error("Все попытки подключения исчерпаны!")
    return False

async def startup_checks():
    """Проверки при запуске бота"""
    logger.info("Выполняю проверки при запуске...")
    
    # Проверка переменных окружения
    missing_vars = []
    if not BOT_TOKEN or BOT_TOKEN == "your_bot_token_here":
        missing_vars.append("BOT_TOKEN")
    if not ADMIN_PASSWORD or ADMIN_PASSWORD == "your_secure_admin_password_here":
        missing_vars.append("ADMIN_PASSWORD")
    if not ADMIN_IDS:
        missing_vars.append("ADMIN_IDS")
    
    if missing_vars:
        logger.error(f"ОШИБКА: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        logger.error("Создайте файл .env и укажите недостающие переменные")
        return False
    
    # Проверка базы данных
    try:
        if not ensure_database_exists():
            logger.error("ОШИБКА: Не удалось инициализировать базу данных!")
            return False
        
        init_db()
        logger.info("УСПЕХ: База данных инициализирована")
        
        # Проверяем целостность данных
        try:
            integrity_check = validate_data_integrity()
            if not integrity_check['healthy']:
                logger.warning(f"ВНИМАНИЕ: Обнаружены проблемы с данными: {'; '.join(integrity_check['issues'])}")
            else:
                logger.info("УСПЕХ: Целостность данных проверена")
        except Exception as e:
            logger.warning(f"Не удалось проверить целостность: {e}")
            
    except Exception as e:
        logger.error(f"ОШИБКА при работе с базой данных: {e}")
        return False
    
    # Создаем папку для материалов
    try:
        os.makedirs("materials", exist_ok=True)
        logger.info("УСПЕХ: Папка materials создана/проверена")
    except Exception as e:
        logger.warning(f"Не удалось создать папку materials: {e}")
    
    return True

async def main():
    """Основная функция запуска бота с интеграцией защиты состояний"""
    
    logger.info("Запуск бота кардиочекапа с защитой от зацикливания...")
    
    # Выполняем проверки при запуске
    if not await startup_checks():
        logger.error("КРИТИЧЕСКАЯ ОШИБКА: Проверки при запуске не пройдены. Завершение работы.")
        return
    
    # Создание бота
    bot = None
    try:
        bot = await create_bot_with_retry()
        logger.info("УСПЕХ: Бот создан")
        
        # Тестируем подключение
        if not await test_bot_connection(bot):
            logger.error("ОШИБКА: Не удалось подключиться к Telegram API")
            logger.error("Проверьте интернет-подключение или используйте VPN")
            return
        
        # Настройка команд
        await setup_commands(bot)
        
        # Создаем диспетчер
        storage = MemoryStorage()
        dp = Dispatcher(storage=storage)
        
        # ============================================================================
        # ИНТЕГРАЦИЯ MIDDLEWARE ДЛЯ ЗАЩИТЫ ОТ ЗАЦИКЛИВАНИЯ
        # ============================================================================
        
        # КРИТИЧЕСКИ ВАЖНО: Регистрируем middleware защиты состояний ПЕРВЫМ
        # Это обеспечивает обработку всех запросов через защиту от дублирования
        if os.getenv("DEBUG_MODE", "true").lower() == "true":
            logger.info("🔍 РЕЖИМ ДИАГНОСТИКИ: простой middleware")
            
            diagnostic_middleware = SimpleDiagnosticMiddleware()
            dp.message.middleware(diagnostic_middleware)
            dp.callback_query.middleware(diagnostic_middleware)
            logger.info("✅ Диагностический middleware зарегистрирован")
        else:
            logger.info("🛡️ ПРОДАКШН РЕЖИМ: защищенный middleware") 
            dp.message.middleware(state_protection)
            dp.callback_query.middleware(state_protection)
            logger.info("✅ Защищенный middleware зарегистрирован")
        
        # Регистрация административного middleware
        admin_middleware = AdminMiddleware(ADMIN_IDS)
        dp.message.middleware(admin_middleware)
        dp.callback_query.middleware(admin_middleware)
        logger.info("УСПЕХ: Административный middleware зарегистрирован")
        
        # Регистрация роутеров (ПОРЯДОК ВАЖЕН!)
        if ADMIN_IDS:
            dp.include_router(admin_router)  # ПЕРВЫМ - админский роутер
            logger.info("УСПЕХ: Административный роутер подключен")
            
        # Подключаем score2_router если он существует
        try:
            from handlers.score2 import score2_router
            dp.include_router(score2_router)
            logger.info("УСПЕХ: SCORE2 роутер подключен")
        except ImportError:
            logger.warning("SCORE2 роутер не найден, пропускаем")

        dp.include_router(main_router)  # ВТОРЫМ - основной роутер
        
        logger.info("УСПЕХ: Диспетчер настроен с защитой состояний")
        
    except Exception as e:
        logger.error(f"ОШИБКА создания бота: {e}")
        return
    
    # Запуск планировщика рассылок
    scheduler = None
    scheduler_task = None
    
    try:
        if ADMIN_IDS:
            scheduler = BroadcastScheduler(bot)
            logger.info("УСПЕХ: Планировщик рассылок создан")
    except Exception as e:
        logger.warning(f"Ошибка создания планировщика: {e}")
    
    # Запуск планировщика вебинара (ТОЧКА 2)
    webinar_scheduler = None
    try:
        from handlers.webinar_scheduler import start_scheduler
        await start_scheduler(bot)
        logger.info("УСПЕХ: Планировщик вебинара запущен")
    except Exception as e:
        logger.warning(f"Ошибка запуска планировщика вебинара: {e}")
    
    # Выводим информацию о конфигурации
    logger.info("Конфигурация бота:")
    logger.info(f"   Администраторы: {len(ADMIN_IDS)} пользователей")
    logger.info(f"   Пароль админки: {'установлен' if ADMIN_PASSWORD else 'НЕ УСТАНОВЛЕН'}")
    logger.info(f"   Планировщик рассылок: {'включен' if scheduler else 'отключен'}")
    logger.info(f"   Прокси: {'используется' if PROXY_URL else 'не используется'}")
    logger.info(f"   Защита состояний: ВКЛЮЧЕНА")
    logger.info(f"   Middleware: StateProtection -> AdminMiddleware -> Handlers")
    
    # Запуск бота
    logger.info("Запуск polling с защитой от зацикливания...")
    
    try:
        # Запускаем планировщик в фоне
        if scheduler:
            scheduler_task = asyncio.create_task(scheduler.start_scheduler())
            logger.info("ЗАПУЩЕН: Планировщик рассылок")
        
        # Статистика защиты состояний
        def log_protection_stats():
            processing_count = len(state_protection.processing_users)
            cache_size = len(state_protection.user_last_action)
            timeout_size = len(state_protection.action_timeouts)
            
            if processing_count > 0 or cache_size > 50:
                logger.info(f"Защита состояний: обрабатывается {processing_count} пользователей, "
                           f"кэш {cache_size} записей, тайм-ауты {timeout_size}")
        
        # Периодическое логирование статистики (каждые 5 минут)
        async def stats_logger():
            while True:
                await asyncio.sleep(300)  # 5 минут
                log_protection_stats()
        
        stats_task = asyncio.create_task(stats_logger())
        
        # Запускаем поллинг
        await dp.start_polling(
            bot,
            handle_signals=True,
            drop_pending_updates=True
        )
        
    except KeyboardInterrupt:
        logger.info("ОСТАНОВКА: Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"ОШИБКА при работе бота: {e}")
        # Логируем состояние защиты при ошибке
        logger.error(f"Состояние защиты: {len(state_protection.processing_users)} активных пользователей")
    finally:
        # Останавливаем планировщик
        if scheduler:
            scheduler.stop_scheduler()
            logger.info("ОСТАНОВЛЕН: Планировщик рассылок")
            
        # Останавливаем планировщик вебинара
        try:
            from handlers.webinar_scheduler import stop_scheduler
            await stop_scheduler()
            logger.info("ОСТАНОВЛЕН: Планировщик вебинара")
        except Exception as e:
            logger.warning(f"Ошибка остановки планировщика вебинара: {e}")
            
        if scheduler_task:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass
        
        # Останавливаем логирование статистики
        if 'stats_task' in locals():
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass
        
        # Финальная статистика защиты
        final_processing = len(state_protection.processing_users)
        final_cache = len(state_protection.user_last_action)
        logger.info(f"ФИНАЛЬНАЯ СТАТИСТИКА: {final_processing} активных пользователей, "
                   f"{final_cache} записей в кэше")
        
        # Очищаем состояние защиты
        state_protection.processing_users.clear()
        state_protection.user_last_action.clear()
        state_protection.action_timeouts.clear()
        logger.info("ОЧИЩЕНО: Состояние защиты сброшено")
        
        # Закрываем сессию бота
        if bot:
            try:
                await bot.session.close()
                logger.info("ЗАКРЫТО: Сессия бота")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии сессии бота: {e}")
        
        logger.info("ЗАВЕРШЕНО: Бот корректно завершен с защитой состояний")

if __name__ == "__main__":
    try:
        # Проверяем окружение
        if not check_environment():
            exit(1)
        
        # Показываем баннер
        print_startup_banner()
        
        # Запускаем бота
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("STOP: Bot stopped by user")
    except Exception as e:
        try:
            logger.error(f"Critical error during bot startup: {e}")
        except:
            # Если даже логгер не работает
            print(f"Critical error: {e}")
        exit(1)