"""
Утилиты для миграции и работы с ID пользователей
Решение проблемы chat_id vs telegram_id
"""
import sqlite3
import logging
from typing import Dict, List, Tuple, Optional
from database.connection import get_db_sync
from database.models import User, Survey, TestResult, ActivityLog

logger = logging.getLogger(__name__)

def analyze_user_ids() -> Dict[str, any]:
    """Анализирует ID пользователей в базе данных"""
    try:
        conn = sqlite3.connect('cardio_bot.db')
        cursor = conn.cursor()
        
        # Анализ telegram_id в таблице users
        cursor.execute('SELECT telegram_id FROM users')
        user_ids = [row[0] for row in cursor.fetchall()]
        
        analysis = {
            'total_users': len(user_ids),
            'id_ranges': {
                'very_small': len([id for id in user_ids if id < 1000]),      # < 1k (возможно message_id)
                'small': len([id for id in user_ids if 1000 <= id < 100000]), # 1k-100k (возможно message_id)
                'normal': len([id for id in user_ids if 100000 <= id < 10**10]), # 100k-10B (telegram_id)
                'large': len([id for id in user_ids if id >= 10**10])          # > 10B (большие ID)
            },
            'sample_ids': user_ids[:10] if user_ids else [],
            'min_id': min(user_ids) if user_ids else 0,
            'max_id': max(user_ids) if user_ids else 0
        }
        
        conn.close()
        return analysis
    except Exception as e:
        logger.error(f"Ошибка анализа ID: {e}")
        return {'error': str(e)}

def detect_problematic_ids() -> List[Tuple[int, str]]:
    """Находит проблемные ID (возможно chat_id вместо telegram_id)"""
    try:
        conn = sqlite3.connect('cardio_bot.db')
        cursor = conn.cursor()
        
        # Ищем подозрительно малые ID (возможно message_id или chat_id)
        cursor.execute("""
            SELECT telegram_id, 
                   CASE 
                       WHEN telegram_id < 1000 THEN 'очень_малый'
                       WHEN telegram_id < 100000 THEN 'малый'
                       WHEN telegram_id > 999999999999 THEN 'очень_большой'
                       ELSE 'нормальный'
                   END as id_type
            FROM users 
            WHERE telegram_id < 100000 OR telegram_id > 999999999999
            ORDER BY telegram_id
        """)
        
        problematic = cursor.fetchall()
        conn.close()
        return problematic
    except Exception as e:
        logger.error(f"Ошибка поиска проблемных ID: {e}")
        return []

def validate_telegram_ids_in_db() -> Dict[str, any]:
    """Валидирует telegram_id в базе данных"""
    analysis = analyze_user_ids()
    problematic = detect_problematic_ids()
    
    return {
        'analysis': analysis,
        'problematic_ids': problematic,
        'has_problems': len(problematic) > 0,
        'recommendations': generate_id_recommendations(analysis, problematic)
    }

def generate_id_recommendations(analysis: Dict, problematic: List) -> List[str]:
    """Генерирует рекомендации по исправлению ID"""
    recommendations = []
    
    if analysis.get('id_ranges', {}).get('very_small', 0) > 0:
        recommendations.append("Найдены очень малые ID (< 1000) - возможно это message_id")
    
    if analysis.get('id_ranges', {}).get('small', 0) > 0:
        recommendations.append("Найдены малые ID (< 100k) - проверьте, не chat_id ли это")
        
    if len(problematic) > 0:
        recommendations.append(f"Найдено {len(problematic)} проблемных ID")
        recommendations.append("Рекомендуется проверить логику сохранения telegram_id")
    
    if analysis.get('id_ranges', {}).get('normal', 0) == analysis.get('total_users', 0):
        recommendations.append("Все ID выглядят корректно")
        
    return recommendations

def fix_admin_functions_for_existing_ids():
    """Исправляет админские функции для работы с существующими ID"""
    logger.info("Проверка совместимости админских функций...")
    
    # Проверяем, что все функции рассылки используют правильные поля
    validation = validate_telegram_ids_in_db()
    
    if validation['has_problems']:
        logger.warning("Обнаружены проблемные ID в базе данных")
        for rec in validation['recommendations']:
            logger.warning(f"- {rec}")
    else:
        logger.info("ID в базе данных выглядят корректно")
    
    return validation

def get_user_ids_for_broadcast() -> List[int]:
    """Получает все корректные telegram_id для рассылки"""
    try:
        conn = sqlite3.connect('cardio_bot.db')
        cursor = conn.cursor()
        
        # Берем только ID которые выглядят как настоящие telegram_id
        cursor.execute("""
            SELECT DISTINCT telegram_id 
            FROM users 
            WHERE telegram_id >= 100000 AND telegram_id <= 9999999999
            ORDER BY telegram_id
        """)
        
        user_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        logger.info(f"Найдено {len(user_ids)} корректных telegram_id для рассылки")
        return user_ids
    except Exception as e:
        logger.error(f"Ошибка получения ID для рассылки: {e}")
        return []

def clean_duplicate_users():
    """Очищает дублированных пользователей"""
    try:
        conn = sqlite3.connect('cardio_bot.db')
        cursor = conn.cursor()
        
        # Находим дубликаты по email или phone
        cursor.execute("""
            SELECT email, COUNT(*) as cnt
            FROM users 
            WHERE email IS NOT NULL AND email != ''
            GROUP BY email 
            HAVING cnt > 1
        """)
        
        email_dups = cursor.fetchall()
        
        cursor.execute("""
            SELECT phone, COUNT(*) as cnt
            FROM users 
            WHERE phone IS NOT NULL AND phone != ''
            GROUP BY phone 
            HAVING cnt > 1
        """)
        
        phone_dups = cursor.fetchall()
        
        conn.close()
        
        return {
            'email_duplicates': len(email_dups),
            'phone_duplicates': len(phone_dups),
            'details': {
                'emails': email_dups,
                'phones': phone_dups
            }
        }
    except Exception as e:
        logger.error(f"Ошибка поиска дубликатов: {e}")
        return {'error': str(e)}

def create_id_mapping_report() -> str:
    """Создает отчет о mapping ID для админа"""
    validation = validate_telegram_ids_in_db()
    duplicates = clean_duplicate_users()
    
    report = f"""
ОТЧЕТ О ПОЛЬЗОВАТЕЛЬСКИХ ID

Общая статистика:
- Всего пользователей: {validation['analysis'].get('total_users', 0)}
- Минимальный ID: {validation['analysis'].get('min_id', 0)}
- Максимальный ID: {validation['analysis'].get('max_id', 0)}

Распределение по диапазонам:
- Очень малые (< 1k): {validation['analysis']['id_ranges']['very_small']}
- Малые (1k-100k): {validation['analysis']['id_ranges']['small']}
- Нормальные (100k-10B): {validation['analysis']['id_ranges']['normal']}
- Большие (> 10B): {validation['analysis']['id_ranges']['large']}

Проблемные ID: {len(validation['problematic_ids'])}

Дубликаты:
- По email: {duplicates.get('email_duplicates', 0)}
- По телефону: {duplicates.get('phone_duplicates', 0)}

Рекомендации:
"""
    
    for rec in validation['recommendations']:
        report += f"- {rec}\n"
    
    return report

async def admin_id_diagnostic():
    """Быстрая диагностика ID для админа"""
    validation = validate_telegram_ids_in_db()
    
    return {
        'total_users': validation['analysis'].get('total_users', 0),
        'valid_users': validation['analysis']['id_ranges']['normal'],
        'problematic_users': len(validation['problematic_ids']),
        'broadcast_ready_ids': len(get_user_ids_for_broadcast()),
        'has_problems': validation['has_problems'],
        'summary': f"Valid IDs: {validation['analysis']['id_ranges']['normal']} of {validation['analysis'].get('total_users', 0)} total"
    }