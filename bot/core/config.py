"""
Конфигурация бота
"""
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Получаем переменные из .env
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
PROXY_URL = os.getenv("PROXY_URL") 

# ID администраторов
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = []
if ADMIN_IDS_STR:
    try:
        ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_STR.split(",") if x.strip()]
    except ValueError:
        logger.warning("Некорректный формат ADMIN_IDS в .env файле")

def check_environment():
    """Проверка окружения перед запуском"""
    if not os.path.exists('.env'):
        print("ОШИБКА: Файл .env не найден!")
        print("Создайте файл .env на основе .env.example")
        print("\nПример содержимого .env:")
        print("BOT_TOKEN=your_bot_token_here")
        print("ADMIN_IDS=123456789")
        print("ADMIN_PASSWORD=your_password_here")
        return False
    
    return True

def print_startup_banner():
    """Упрощенный баннер без Unicode символов для совместимости с Windows"""
    banner = """
====================================================================
                   CARDIO CHECKUP BOT v2.0                       
                  C ZASHCHITOY OT ZATSIKLIVANIYA                 
====================================================================
  * Middleware zashchity sostoyaniy                              
  * Deduplikatsiya deystviy polzovateley                         
  * Zashchita ot spama i perepolneniya                           
  * Bezopasnoe redaktirovanie soobshcheniy                       
  * Logirovanie vsekh vzaimodeystviy                             
====================================================================
"""
    try:
        print(banner)
    except UnicodeEncodeError:
        # Fallback для старых систем
        print("CARDIO CHECKUP BOT v2.0 - Starting...")
        print("System protection enabled")