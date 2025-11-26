"""
Database module for cardio bot
Реорганизованный модуль базы данных
"""

# Импорты моделей
from .models import Base, User, Survey, TestResult, ActivityLog, BroadcastLog, SystemStats, WebinarStatus, WebinarSurvey, FollowUpStatus, FollowUpSurvey

# Импорты подключения к БД  
from .connection import (
    engine, SessionLocal, get_db, get_db_sync, 
    ensure_database_exists, init_db
)

# Импорты операций с пользователями
from .user_operations import (
    # Основные функции работы с пользователями
    find_existing_user,
    find_existing_user_safe, 
    merge_duplicate_users,
    safe_save_user_data,
    mark_user_completed,
    check_user_completed,
    
    # Логирование и статистика
    log_user_activity,
    
    # Валидация и утилиты
    clean_user_data,
    anonymize_user_data,
)


# Импорты аналитики и статистики
from .analytics import (
    # Функции статистики пользователей
    get_user_data,
    get_user_stats,
    get_detailed_stats,
    get_comprehensive_user_stats,
    
    # Функции экспорта данных
    export_to_excel,
    export_single_table_csv,
    export_users_for_external_system,
    
    # Административные функции
    admin_export_data,
    admin_get_stats,
    admin_get_detailed_stats,
    
    # Функции валидации данных
    validate_database_integrity,
    validate_data_integrity,
    validate_telegram_ids,
    
    # Функции системной статистики
    update_daily_stats,
    get_daily_stats_range,
    get_database_statistics,
    
    # Функции анализа производительности
    analyze_database_performance,
    calculate_performance_score,
)

# Экспорты для обратной совместимости
__all__ = [
    # Модели
    'Base', 'User', 'Survey', 'TestResult', 'ActivityLog', 'BroadcastLog', 'SystemStats', 'WebinarStatus', 'WebinarSurvey', 'FollowUpStatus', 'FollowUpSurvey',
    
    # Подключение к БД
    'engine', 'SessionLocal', 'get_db', 'get_db_sync', 
    'ensure_database_exists', 'init_db',
    
    # Операции с пользователями
    'find_existing_user', 'find_existing_user_safe', 'merge_duplicate_users',
    'safe_save_user_data', 'mark_user_completed', 'check_user_completed',
    'log_user_activity', 'clean_user_data', 'anonymize_user_data',
    
    # Аналитика и статистика
    'get_user_data', 'get_user_stats', 'get_detailed_stats', 
    'get_comprehensive_user_stats', 'export_to_excel', 'export_single_table_csv',
    'export_users_for_external_system', 'admin_export_data', 'admin_get_stats',
    'admin_get_detailed_stats', 'validate_database_integrity', 
    'validate_data_integrity', 'validate_telegram_ids', 'update_daily_stats',
    'get_daily_stats_range', 'get_database_statistics', 
    'analyze_database_performance', 'calculate_performance_score',
]