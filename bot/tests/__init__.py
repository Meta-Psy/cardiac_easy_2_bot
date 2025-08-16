"""
Tests module for cardio bot
Психологические и медицинские тесты
"""

# Импорты базовых функций
from .base import (
    TEST_NORMS,
    validate_test_scores,
    get_test_norms,
    get_risk_category,
    calculate_test_percentile
)

# Импорты HADS теста
from .hads import (
    get_hads_questions,
    calculate_hads_scores,
    get_hads_interpretation
)

# Импорты теста Бернса
from .burns import (
    get_burns_questions,
    get_burns_interpretation
)

# Импорты ISI теста
from .isi import (
    get_isi_questions,
    get_isi_interpretation
)

# Импорты STOP-BANG теста
from .stop_bang import (
    get_stop_bang_questions,
    get_stop_bang_interpretation
)

# Экспорты для обратной совместимости
__all__ = [
    # Базовые функции
    'TEST_NORMS', 'validate_test_scores', 'get_test_norms', 
    'get_risk_category', 'calculate_test_percentile',
    
    # HADS тест
    'get_hads_questions', 'calculate_hads_scores', 'get_hads_interpretation',
    
    # Тест Бернса
    'get_burns_questions', 'get_burns_interpretation',
    
    # ISI тест
    'get_isi_questions', 'get_isi_interpretation',
    
    # STOP-BANG тест
    'get_stop_bang_questions', 'get_stop_bang_interpretation',
]