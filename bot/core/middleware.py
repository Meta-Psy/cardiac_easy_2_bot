"""
Middleware для бота
"""
import logging
from typing import Any, Awaitable, Callable, Dict

logger = logging.getLogger(__name__)

class AdminMiddleware:
    """Middleware для проверки прав администратора"""
    
    def __init__(self, admin_ids):
        self.admin_ids = admin_ids
    
    async def __call__(self, handler, event, data):
        if hasattr(event, 'from_user') and event.from_user:
            data['is_admin'] = event.from_user.id in self.admin_ids
        else:
            data['is_admin'] = False
        return await handler(event, data)

class SimpleDiagnosticMiddleware:
    """Простой диагностический middleware"""
    async def __call__(self, handler, event, data):
        if hasattr(event, 'from_user') and event.from_user:
            user_id = event.from_user.id
            logger.info(f"🔍 ДИАГНОСТИКА: user_id={user_id}")
        return await handler(event, data)