"""
Базовые константы и функции для психологических тестов
"""
from datetime import datetime
from typing import Dict, List, Any, Tuple

# ============================================================================
# КОНСТАНТЫ И НАСТРОЙКИ
# ============================================================================

# Нормативные значения для тестов
TEST_NORMS = {
    'hads_anxiety': {
        'норма': (0, 7),
        'субклиническая': (8, 10), 
        'клиническая': (11, 21)
    },
    'hads_depression': {
        'норма': (0, 7),
        'субклиническая': (8, 10),
        'клиническая': (11, 21)
    },
    'burns': {
        'минимальная': (0, 5),
        'легкая': (6, 10),
        'умеренная': (11, 25),
        'тяжелая': (26, 50),
        'крайне_тяжелая': (51, 100)
    },
    'isi': {
        'нет_бессонницы': (0, 7),
        'подпороговая': (8, 14),
        'умеренная': (15, 21),
        'тяжелая': (22, 28)
    },
    'stop_bang': {
        'низкий': (0, 2),
        'умеренный': (3, 4),
        'высокий': (5, 8)
    },
    'ess': {
        'норма': (0, 10),
        'легкая': (11, 12),
        'умеренная': (13, 15),
        'выраженная': (16, 24)
    },
    'fagerstrom': {
        'очень_слабая': (0, 2),
        'слабая': (3, 4),
        'средняя': (5, 6),
        'сильная': (7, 8),
        'очень_сильная': (9, 10)
    },
    'audit': {
        'низкий': (0, 7),
        'опасное': (8, 15),
        'вредное': (16, 19),
        'зависимость': (20, 40)
    }
}

def validate_test_scores(**scores) -> Dict[str, Any]:
    """Валидация результатов тестов"""
    validation_rules = {
        'hads_anxiety_score': (0, 21),
        'hads_depression_score': (0, 21),
        'burns_score': (0, 100),
        'isi_score': (0, 28),
        'stop_bang_score': (0, 8),
        'ess_score': (0, 24),
        'fagerstrom_score': (0, 10),
        'audit_score': (0, 40)
    }
    
    errors = []
    for test_name, score in scores.items():
        if test_name in validation_rules and score is not None:
            min_val, max_val = validation_rules[test_name]
            if not (min_val <= score <= max_val):
                errors.append(f"Некорректное значение для {test_name}: {score} (должно быть {min_val}-{max_val})")
    
    return {'valid': len(errors) == 0, 'errors': errors}

def get_test_norms() -> Dict[str, Dict[str, Tuple[int, int]]]:
    """Получить нормативные значения всех тестов"""
    return TEST_NORMS

def get_risk_category(test_name: str, score: int) -> str:
    """Определить категорию риска для конкретного теста"""
    if test_name not in TEST_NORMS:
        return "неизвестно"
    
    norms = TEST_NORMS[test_name]
    for category, (min_val, max_val) in norms.items():
        if min_val <= score <= max_val:
            return category
    
    return "вне нормы"

def calculate_test_percentile(test_name: str, score: int, population_scores: List[int]) -> int:
    """Рассчитать процентиль для результата теста"""
    if not population_scores:
        return 50
    
    sorted_scores = sorted(population_scores)
    position = sum(1 for s in sorted_scores if s <= score)
    percentile = (position / len(sorted_scores)) * 100
    
    return int(percentile)