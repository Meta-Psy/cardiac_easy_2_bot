"""
Планировщик для рассылок вебинара (ТОЧКА 2)
Управляет отложенными рассылками и напоминаниями
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List
from aiogram import Bot

from .webinar_broadcast import send_reminder_message, get_users_for_reminders
from database import get_db_sync, WebinarStatus

# Настройка логирования
logger = logging.getLogger(__name__)

class WebinarScheduler:
    """Планировщик рассылок для вебинара"""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.running = False
        
    async def start(self):
        """Запускает планировщик"""
        self.running = True
        logger.info("🚀 Планировщик вебинара запущен")
        
        # Запускаем мониторинг напоминаний
        asyncio.create_task(self._monitor_reminders())
        
    async def stop(self):
        """Останавливает планировщик"""
        self.running = False
        logger.info("🛑 Планировщик вебинара остановлен")
        
    async def _monitor_reminders(self):
        """Мониторинг и отправка напоминаний"""
        while self.running:
            try:
                # Проверяем каждые 10 минут
                await asyncio.sleep(600)  # 10 минут
                
                if not self.running:
                    break
                    
                # Получаем пользователей для напоминаний
                users_for_reminders = await get_users_for_reminders()
                
                if users_for_reminders:
                    logger.info(f"📨 Найдено {len(users_for_reminders)} пользователей для напоминаний")
                    
                    for chat_id in users_for_reminders:
                        try:
                            await send_reminder_message(self.bot, chat_id)
                            await asyncio.sleep(1)  # Пауза между отправками
                        except Exception as e:
                            logger.error(f"Ошибка отправки напоминания {chat_id}: {e}")
                
            except Exception as e:
                logger.error(f"Ошибка в мониторинге напоминаний: {e}")
                await asyncio.sleep(60)  # Пауза при ошибке

# Глобальный экземпляр планировщика
_scheduler: WebinarScheduler = None

def get_scheduler(bot: Bot = None) -> WebinarScheduler:
    """Получает глобальный экземпляр планировщика"""
    global _scheduler
    if _scheduler is None and bot is not None:
        _scheduler = WebinarScheduler(bot)
    return _scheduler

async def start_scheduler(bot: Bot):
    """Запускает планировщик"""
    scheduler = get_scheduler(bot)
    await scheduler.start()

async def stop_scheduler():
    """Останавливает планировщик"""
    scheduler = get_scheduler()
    if scheduler:
        await scheduler.stop()