import asyncio
import json
import logging
from typing import Dict, Any
from datetime import datetime

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command

# Импорты из существующего кода
from database import log_user_activity, get_db_sync, User, ActivityLog

logger = logging.getLogger(__name__)

# ============================================================================
# СОСТОЯНИЯ ДЛЯ FSM
# ============================================================================

class Score2States(StatesGroup):
    """Состояния для прохождения SCORE2 калькулятора"""
    waiting_for_gender = State()
    waiting_for_smoking = State()
    waiting_for_age = State()
    waiting_for_blood_pressure = State()
    waiting_for_cholesterol_unit = State()
    waiting_for_cholesterol_mmol = State()
    waiting_for_cholesterol_mgdl = State()
    showing_result = State()

# ============================================================================
# SCORE2 ДАННЫЕ ДЛЯ РАСЧЕТА
# ============================================================================

# Таблица SCORE2 - структура: возраст -> пол -> курение -> САД -> холестерин -> риск
SCORE2_TABLE = {
    "40-44": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 5, "4.0-4.9": 6, "5.0-5.9": 7, "6.0-6.9": 8},
                "140-159": {"3.0-3.9": 4, "4.0-4.9": 4, "5.0-5.9": 5, "6.0-6.9": 6},
                "120-139": {"3.0-3.9": 3, "4.0-4.9": 3, "5.0-5.9": 3, "6.0-6.9": 4},
                "100-119": {"3.0-3.9": 2, "4.0-4.9": 2, "5.0-5.9": 2, "6.0-6.9": 3}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 13, "4.0-4.9": 15, "5.0-5.9": 17, "6.0-6.9": 19},
                "140-159": {"3.0-3.9": 9, "4.0-4.9": 11, "5.0-5.9": 12, "6.0-6.9": 14},
                "120-139": {"3.0-3.9": 7, "4.0-4.9": 8, "5.0-5.9": 9, "6.0-6.9": 10},
                "100-119": {"3.0-3.9": 5, "4.0-4.9": 6, "5.0-5.9": 6, "6.0-6.9": 7}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 7, "4.0-4.9": 9, "5.0-5.9": 11, "6.0-6.9": 13},
                "140-159": {"3.0-3.9": 5, "4.0-4.9": 6, "5.0-5.9": 8, "6.0-6.9": 10},
                "120-139": {"3.0-3.9": 4, "4.0-4.9": 5, "5.0-5.9": 6, "6.0-6.9": 7},
                "100-119": {"3.0-3.9": 3, "4.0-4.9": 4, "5.0-5.9": 4, "6.0-6.9": 5}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 14, "4.0-4.9": 17, "5.0-5.9": 20, "6.0-6.9": 24},
                "140-159": {"3.0-3.9": 11, "4.0-4.9": 13, "5.0-5.9": 16, "6.0-6.9": 19},
                "120-139": {"3.0-3.9": 8, "4.0-4.9": 10, "5.0-5.9": 12, "6.0-6.9": 14},
                "100-119": {"3.0-3.9": 6, "4.0-4.9": 7, "5.0-5.9": 9, "6.0-6.9": 11}
            }
        }
    },
    "45-49": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 7, "4.0-4.9": 8, "5.0-5.9": 9, "6.0-6.9": 10},
                "140-159": {"3.0-3.9": 5, "4.0-4.9": 6, "5.0-5.9": 7, "6.0-6.9": 8},
                "120-139": {"3.0-3.9": 4, "4.0-4.9": 4, "5.0-5.9": 5, "6.0-6.9": 6},
                "100-119": {"3.0-3.9": 3, "4.0-4.9": 3, "5.0-5.9": 4, "6.0-6.9": 4}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 16, "4.0-4.9": 18, "5.0-5.9": 21, "6.0-6.9": 23},
                "140-159": {"3.0-3.9": 12, "4.0-4.9": 14, "5.0-5.9": 15, "6.0-6.9": 17},
                "120-139": {"3.0-3.9": 9, "4.0-4.9": 10, "5.0-5.9": 12, "6.0-6.9": 13},
                "100-119": {"3.0-3.9": 7, "4.0-4.9": 8, "5.0-5.9": 9, "6.0-6.9": 10}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 9, "4.0-4.9": 11, "5.0-5.9": 13, "6.0-6.9": 16},
                "140-159": {"3.0-3.9": 7, "4.0-4.9": 8, "5.0-5.9": 10, "6.0-6.9": 12},
                "120-139": {"3.0-3.9": 5, "4.0-4.9": 6, "5.0-5.9": 8, "6.0-6.9": 9},
                "100-119": {"3.0-3.9": 4, "4.0-4.9": 5, "5.0-5.9": 6, "6.0-6.9": 7}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 17, "4.0-4.9": 20, "5.0-5.9": 24, "6.0-6.9": 28},
                "140-159": {"3.0-3.9": 13, "4.0-4.9": 16, "5.0-5.9": 18, "6.0-6.9": 22},
                "120-139": {"3.0-3.9": 10, "4.0-4.9": 12, "5.0-5.9": 14, "6.0-6.9": 17},
                "100-119": {"3.0-3.9": 8, "4.0-4.9": 9, "5.0-5.9": 11, "6.0-6.9": 13}
            }
        }
    },
    "50-54": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 10, "4.0-4.9": 11, "5.0-5.9": 12, "6.0-6.9": 14},
                "140-159": {"3.0-3.9": 8, "4.0-4.9": 9, "5.0-5.9": 9, "6.0-6.9": 11},
                "120-139": {"3.0-3.9": 6, "4.0-4.9": 6, "5.0-5.9": 7, "6.0-6.9": 8},
                "100-119": {"3.0-3.9": 4, "4.0-4.9": 5, "5.0-5.9": 5, "6.0-6.9": 6}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 21, "4.0-4.9": 23, "5.0-5.9": 25, "6.0-6.9": 28},
                "140-159": {"3.0-3.9": 16, "4.0-4.9": 18, "5.0-5.9": 19, "6.0-6.9": 22},
                "120-139": {"3.0-3.9": 12, "4.0-4.9": 13, "5.0-5.9": 15, "6.0-6.9": 17},
                "100-119": {"3.0-3.9": 9, "4.0-4.9": 10, "5.0-5.9": 11, "6.0-6.9": 13}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 12, "4.0-4.9": 14, "5.0-5.9": 16, "6.0-6.9": 19},
                "140-159": {"3.0-3.9": 10, "4.0-4.9": 11, "5.0-5.9": 13, "6.0-6.9": 15},
                "120-139": {"3.0-3.9": 7, "4.0-4.9": 9, "5.0-5.9": 10, "6.0-6.9": 12},
                "100-119": {"3.0-3.9": 4, "4.0-4.9": 5, "5.0-5.9": 6, "6.0-6.9": 7}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 21, "4.0-4.9": 24, "5.0-5.9": 28, "6.0-6.9": 31},
                "140-159": {"3.0-3.9": 17, "4.0-4.9": 19, "5.0-5.9": 22, "6.0-6.9": 25},
                "120-139": {"3.0-3.9": 13, "4.0-4.9": 15, "5.0-5.9": 17, "6.0-6.9": 20},
                "100-119": {"3.0-3.9": 10, "4.0-4.9": 12, "5.0-5.9": 14, "6.0-6.9": 17}
            }
        }
    },
    "55-59": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 14, "4.0-4.9": 15, "5.0-5.9": 17, "6.0-6.9": 18},
                "140-159": {"3.0-3.9": 11, "4.0-4.9": 12, "5.0-5.9": 13, "6.0-6.9": 14},
                "120-139": {"3.0-3.9": 8, "4.0-4.9": 9, "5.0-5.9": 10, "6.0-6.9": 11},
                "100-119": {"3.0-3.9": 7, "4.0-4.9": 7, "5.0-5.9": 8, "6.0-6.9": 9}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 26, "4.0-4.9": 28, "5.0-5.9": 31, "6.0-6.9": 33},
                "140-159": {"3.0-3.9": 21, "4.0-4.9": 23, "5.0-5.9": 24, "6.0-6.9": 26},
                "120-139": {"3.0-3.9": 16, "4.0-4.9": 18, "5.0-5.9": 19, "6.0-6.9": 21},
                "100-119": {"3.0-3.9": 13, "4.0-4.9": 14, "5.0-5.9": 15, "6.0-6.9": 16}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 16, "4.0-4.9": 18, "5.0-5.9": 20, "6.0-6.9": 23},
                "140-159": {"3.0-3.9": 13, "4.0-4.9": 14, "5.0-5.9": 16, "6.0-6.9": 18},
                "120-139": {"3.0-3.9": 10, "4.0-4.9": 11, "5.0-5.9": 13, "6.0-6.9": 15},
                "100-119": {"3.0-3.9": 8, "4.0-4.9": 9, "5.0-5.9": 10, "6.0-6.9": 12}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 25, "4.0-4.9": 28, "5.0-5.9": 32, "6.0-6.9": 35},
                "140-159": {"3.0-3.9": 21, "4.0-4.9": 23, "5.0-5.9": 26, "6.0-6.9": 29},
                "120-139": {"3.0-3.9": 17, "4.0-4.9": 19, "5.0-5.9": 21, "6.0-6.9": 24},
                "100-119": {"3.0-3.9": 13, "4.0-4.9": 15, "5.0-5.9": 17, "6.0-6.9": 19}
            }
        }
    },
    "60-64": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 20, "4.0-4.9": 21, "5.0-5.9": 22, "6.0-6.9": 24},
                "140-159": {"3.0-3.9": 16, "4.0-4.9": 17, "5.0-5.9": 18, "6.0-6.9": 19},
                "120-139": {"3.0-3.9": 12, "4.0-4.9": 13, "5.0-5.9": 14, "6.0-6.9": 15},
                "100-119": {"3.0-3.9": 10, "4.0-4.9": 11, "5.0-5.9": 11, "6.0-6.9": 12}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 33, "4.0-4.9": 35, "5.0-5.9": 37, "6.0-6.9": 39},
                "140-159": {"3.0-3.9": 27, "4.0-4.9": 29, "5.0-5.9": 30, "6.0-6.9": 32},
                "120-139": {"3.0-3.9": 22, "4.0-4.9": 23, "5.0-5.9": 25, "6.0-6.9": 26},
                "100-119": {"3.0-3.9": 17, "4.0-4.9": 18, "5.0-5.9": 20, "6.0-6.9": 21}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 20, "4.0-4.9": 23, "5.0-5.9": 25, "6.0-6.9": 27},
                "140-159": {"3.0-3.9": 17, "4.0-4.9": 19, "5.0-5.9": 20, "6.0-6.9": 22},
                "120-139": {"3.0-3.9": 14, "4.0-4.9": 15, "5.0-5.9": 17, "6.0-6.9": 18},
                "100-119": {"3.0-3.9": 11, "4.0-4.9": 12, "5.0-5.9": 14, "6.0-6.9": 15}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 31, "4.0-4.9": 33, "5.0-5.9": 36, "6.0-6.9": 40},
                "140-159": {"3.0-3.9": 25, "4.0-4.9": 28, "5.0-5.9": 31, "6.0-6.9": 33},
                "120-139": {"3.0-3.9": 19, "4.0-4.9": 23, "5.0-5.9": 25, "6.0-6.9": 28},
                "100-119": {"3.0-3.9": 17, "4.0-4.9": 19, "5.0-5.9": 21, "6.0-6.9": 23}
            }
        }
    },
    "65-69": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 27, "4.0-4.9": 28, "5.0-5.9": 30, "6.0-6.9": 31},
                "140-159": {"3.0-3.9": 22, "4.0-4.9": 23, "5.0-5.9": 24, "6.0-6.9": 26},
                "120-139": {"3.0-3.9": 18, "4.0-4.9": 19, "5.0-5.9": 20, "6.0-6.9": 21},
                "100-119": {"3.0-3.9": 15, "4.0-4.9": 16, "5.0-5.9": 16, "6.0-6.9": 17}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 41, "4.0-4.9": 42, "5.0-5.9": 44, "6.0-6.9": 46},
                "140-159": {"3.0-3.9": 34, "4.0-4.9": 36, "5.0-5.9": 37, "6.0-6.9": 39},
                "120-139": {"3.0-3.9": 28, "4.0-4.9": 30, "5.0-5.9": 31, "6.0-6.9": 33},
                "100-119": {"3.0-3.9": 23, "4.0-4.9": 24, "5.0-5.9": 26, "6.0-6.9": 27}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 26, "4.0-4.9": 28, "5.0-5.9": 30, "6.0-6.9": 32},
                "140-159": {"3.0-3.9": 22, "4.0-4.9": 24, "5.0-5.9": 26, "6.0-6.9": 27},
                "120-139": {"3.0-3.9": 18, "4.0-4.9": 20, "5.0-5.9": 21, "6.0-6.9": 23},
                "100-119": {"3.0-3.9": 15, "4.0-4.9": 17, "5.0-5.9": 18, "6.0-6.9": 19}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 36, "4.0-4.9": 39, "5.0-5.9": 42, "6.0-6.9": 44},
                "140-159": {"3.0-3.9": 31, "4.0-4.9": 33, "5.0-5.9": 36, "6.0-6.9": 38},
                "120-139": {"3.0-3.9": 26, "4.0-4.9": 28, "5.0-5.9": 30, "6.0-6.9": 33},
                "100-119": {"3.0-3.9": 22, "4.0-4.9": 24, "5.0-5.9": 26, "6.0-6.9": 28}
            }
        }
    },
    "70-74": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 37, "4.0-4.9": 38, "5.0-5.9": 39, "6.0-6.9": 41},
                "140-159": {"3.0-3.9": 33, "4.0-4.9": 34, "5.0-5.9": 35, "6.0-6.9": 36},
                "120-139": {"3.0-3.9": 29, "4.0-4.9": 30, "5.0-5.9": 31, "6.0-6.9": 32},
                "100-119": {"3.0-3.9": 26, "4.0-4.9": 27, "5.0-5.9": 28, "6.0-6.9": 29}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 48, "4.0-4.9": 49, "5.0-5.9": 51, "6.0-6.9": 52},
                "140-159": {"3.0-3.9": 43, "4.0-4.9": 44, "5.0-5.9": 46, "6.0-6.9": 47},
                "120-139": {"3.0-3.9": 39, "4.0-4.9": 40, "5.0-5.9": 41, "6.0-6.9": 43},
                "100-119": {"3.0-3.9": 34, "4.0-4.9": 36, "5.0-5.9": 37, "6.0-6.9": 38}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 35, "4.0-4.9": 37, "5.0-5.9": 39, "6.0-6.9": 40},
                "140-159": {"3.0-3.9": 32, "4.0-4.9": 33, "5.0-5.9": 35, "6.0-6.9": 36},
                "120-139": {"3.0-3.9": 28, "4.0-4.9": 30, "5.0-5.9": 31, "6.0-6.9": 33},
                "100-119": {"3.0-3.9": 25, "4.0-4.9": 26, "5.0-5.9": 28, "6.0-6.9": 29}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 43, "4.0-4.9": 45, "5.0-5.9": 47, "6.0-6.9": 49},
                "140-159": {"3.0-3.9": 39, "4.0-4.9": 41, "5.0-5.9": 42, "6.0-6.9": 44},
                "120-139": {"3.0-3.9": 35, "4.0-4.9": 36, "5.0-5.9": 38, "6.0-6.9": 40},
                "100-119": {"3.0-3.9": 31, "4.0-4.9": 33, "5.0-5.9": 34, "6.0-6.9": 36}
            }
        }
    },
    "75-79": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 44, "4.0-4.9": 46, "5.0-5.9": 47, "6.0-6.9": 48},
                "140-159": {"3.0-3.9": 41, "4.0-4.9": 42, "5.0-5.9": 43, "6.0-6.9": 45},
                "120-139": {"3.0-3.9": 37, "4.0-4.9": 39, "5.0-5.9": 40, "6.0-6.9": 41},
                "100-119": {"3.0-3.9": 34, "4.0-4.9": 35, "5.0-5.9": 36, "6.0-6.9": 37}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 53, "4.0-4.9": 55, "5.0-5.9": 56, "6.0-6.9": 58},
                "140-159": {"3.0-3.9": 49, "4.0-4.9": 51, "5.0-5.9": 52, "6.0-6.9": 53},
                "120-139": {"3.0-3.9": 46, "4.0-4.9": 47, "5.0-5.9": 48, "6.0-6.9": 49},
                "100-119": {"3.0-3.9": 42, "4.0-4.9": 43, "5.0-5.9": 44, "6.0-6.9": 46}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 40, "4.0-4.9": 42, "5.0-5.9": 45, "6.0-6.9": 48},
                "140-159": {"3.0-3.9": 37, "4.0-4.9": 39, "5.0-5.9": 42, "6.0-6.9": 44},
                "120-139": {"3.0-3.9": 34, "4.0-4.9": 36, "5.0-5.9": 39, "6.0-6.9": 41},
                "100-119": {"3.0-3.9": 31, "4.0-4.9": 33, "5.0-5.9": 36, "6.0-6.9": 38}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 45, "4.0-4.9": 48, "5.0-5.9": 51, "6.0-6.9": 54},
                "140-159": {"3.0-3.9": 42, "4.0-4.9": 44, "5.0-5.9": 47, "6.0-6.9": 50},
                "120-139": {"3.0-3.9": 39, "4.0-4.9": 41, "5.0-5.9": 44, "6.0-6.9": 47},
                "100-119": {"3.0-3.9": 36, "4.0-4.9": 38, "5.0-5.9": 41, "6.0-6.9": 43}
            }
        }
    },
    "80-84": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 53, "4.0-4.9": 54, "5.0-5.9": 55, "6.0-6.9": 57},
                "140-159": {"3.0-3.9": 50, "4.0-4.9": 51, "5.0-5.9": 52, "6.0-6.9": 54},
                "120-139": {"3.0-3.9": 47, "4.0-4.9": 48, "5.0-5.9": 49, "6.0-6.9": 51},
                "100-119": {"3.0-3.9": 44, "4.0-4.9": 45, "5.0-5.9": 47, "6.0-6.9": 48}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 59, "4.0-4.9": 60, "5.0-5.9": 62, "6.0-6.9": 63},
                "140-159": {"3.0-3.9": 56, "4.0-4.9": 57, "5.0-5.9": 59, "6.0-6.9": 60},
                "120-139": {"3.0-3.9": 53, "4.0-4.9": 54, "5.0-5.9": 56, "6.0-6.9": 57},
                "100-119": {"3.0-3.9": 50, "4.0-4.9": 51, "5.0-5.9": 53, "6.0-6.9": 54}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 44, "4.0-4.9": 48, "5.0-5.9": 52, "6.0-6.9": 56},
                "140-159": {"3.0-3.9": 42, "4.0-4.9": 46, "5.0-5.9": 49, "6.0-6.9": 53},
                "120-139": {"3.0-3.9": 40, "4.0-4.9": 43, "5.0-5.9": 47, "6.0-6.9": 51},
                "100-119": {"3.0-3.9": 38, "4.0-4.9": 41, "5.0-5.9": 45, "6.0-6.9": 48}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 47, "4.0-4.9": 51, "5.0-5.9": 55, "6.0-6.9": 59},
                "140-159": {"3.0-3.9": 45, "4.0-4.9": 49, "5.0-5.9": 52, "6.0-6.9": 56},
                "120-139": {"3.0-3.9": 43, "4.0-4.9": 46, "5.0-5.9": 50, "6.0-6.9": 54},
                "100-119": {"3.0-3.9": 40, "4.0-4.9": 44, "5.0-5.9": 48, "6.0-6.9": 51}
            }
        }
    },
    "85-89": {
        "female": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 62, "4.0-4.9": 63, "5.0-5.9": 64, "6.0-6.9": 65},
                "140-159": {"3.0-3.9": 60, "4.0-4.9": 61, "5.0-5.9": 62, "6.0-6.9": 63},
                "120-139": {"3.0-3.9": 58, "4.0-4.9": 59, "5.0-5.9": 60, "6.0-6.9": 61},
                "100-119": {"3.0-3.9": 56, "4.0-4.9": 57, "5.0-5.9": 58, "6.0-6.9": 60}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 65, "4.0-4.9": 66, "5.0-5.9": 67, "6.0-6.9": 68},
                "140-159": {"3.0-3.9": 63, "4.0-4.9": 64, "5.0-5.9": 65, "6.0-6.9": 66},
                "120-139": {"3.0-3.9": 61, "4.0-4.9": 62, "5.0-5.9": 63, "6.0-6.9": 65},
                "100-119": {"3.0-3.9": 59, "4.0-4.9": 60, "5.0-5.9": 61, "6.0-6.9": 63}
            }
        },
        "male": {
            "non_smoking": {
                "160-179": {"3.0-3.9": 49, "4.0-4.9": 54, "5.0-5.9": 59, "6.0-6.9": 64},
                "140-159": {"3.0-3.9": 48, "4.0-4.9": 53, "5.0-5.9": 58, "6.0-6.9": 63},
                "120-139": {"3.0-3.9": 47, "4.0-4.9": 52, "5.0-5.9": 56, "6.0-6.9": 61},
                "100-119": {"3.0-3.9": 46, "4.0-4.9": 50, "5.0-5.9": 55, "6.0-6.9": 60}
            },
            "smoking": {
                "160-179": {"3.0-3.9": 49, "4.0-4.9": 54, "5.0-5.9": 59, "6.0-6.9": 64},
                "140-159": {"3.0-3.9": 48, "4.0-4.9": 53, "5.0-5.9": 58, "6.0-6.9": 63},
                "120-139": {"3.0-3.9": 47, "4.0-4.9": 52, "5.0-5.9": 56, "6.0-6.9": 61},
                "100-119": {"3.0-3.9": 46, "4.0-4.9": 50, "5.0-5.9": 55, "6.0-6.9": 60}
            }
        }
    }
}

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def get_age_group(age_choice: str) -> str:
    """Определение возрастной группы для таблицы SCORE2"""
    age_mapping = {
        "менее_40": "40-44",
        "40-44": "40-44",
        "45-49": "45-49", 
        "50-54": "50-54",
        "55-59": "55-59",
        "60-64": "60-64",
        "65-69": "65-69",
        "70-74": "70-74",
        "75-79": "75-79",
        "80-84": "80-84",
        "85-89": "85-89",
        "более_90": "85-89"
    }
    return age_mapping.get(age_choice, "40-44")

def get_bp_group(bp_choice: str) -> str:
    """Определение группы АД для таблицы SCORE2"""
    bp_mapping = {
        "менее_100": "100-119",
        "100-119": "100-119",
        "120-139": "120-139",
        "140-159": "140-159",
        "160-179": "160-179",
        "более_180": "160-179"
    }
    return bp_mapping.get(bp_choice, "120-139")

def get_cholesterol_group_mmol(chol_choice: str) -> str:
    """Определение группы холестерина (ммоль/л) для таблицы SCORE2"""
    chol_mapping = {
        "менее_3": "3.0-3.9",
        "3.0-3.9": "3.0-3.9",
        "4.0-4.9": "4.0-4.9",
        "5.0-5.9": "5.0-5.9",
        "6.0-6.9": "6.0-6.9",
        "более_6.9": "6.0-6.9"
    }
    return chol_mapping.get(chol_choice, "4.0-4.9")

def get_cholesterol_group_mgdl(chol_choice: str) -> str:
    """Определение группы холестерина (мг/дл) для таблицы SCORE2"""
    chol_mapping = {
        "менее_150": "3.0-3.9",
        "150-200": "4.0-4.9", 
        "200-250": "5.0-5.9",
        "более_250": "6.0-6.9"
    }
    return chol_mapping.get(chol_choice, "4.0-4.9")

def calculate_score2_risk(gender: str, smoking: str, age_group: str, bp_group: str, chol_group: str) -> int:
    """Расчет риска по таблице SCORE2"""
    try:
        gender_key = "female" if gender == "женский" else "male"
        smoking_key = "smoking" if smoking == "курит" else "non_smoking"
        
        risk = SCORE2_TABLE[age_group][gender_key][smoking_key][bp_group][chol_group]
        return risk
    except KeyError:
        logger.error(f"Ошибка расчета SCORE2: {gender}, {smoking}, {age_group}, {bp_group}, {chol_group}")
        return 0

def get_risk_interpretation(risk_score: int) -> Dict[str, str]:
    """Интерпретация результата SCORE2"""
    if risk_score < 5:
        return {
            "level": "НИЗКИЙ",
            "color": "🟢",
            "description": "Низкий риск сердечно-сосудистых заболеваний",
            "recommendation": "Здоровый образ жизни"
        }
    elif risk_score < 10:
        return {
            "level": "УМЕРЕННЫЙ", 
            "color": "🟡",
            "description": "Умеренный риск сердечно-сосудистых заболеваний",
            "recommendation": "Здоровый образ жизни"
        }
    elif risk_score < 20:
        return {
            "level": "ВЫСОКИЙ",
            "color": "🟠", 
            "description": "Высокий риск сердечно-сосудистых заболеваний",
            "recommendation": "Требуется консультация кардиолога. Необходима коррекция факторов риска."
        }
    else:
        return {
            "level": "ОЧЕНЬ ВЫСОКИЙ",
            "color": "🔴",
            "description": "Очень высокий риск сердечно-сосудистых заболеваний", 
            "recommendation": "Требуется консультация кардиолога. Необходима коррекция факторов риска."
        }

# ============================================================================
# СОЗДАНИЕ КЛАВИАТУР
# ============================================================================

def create_gender_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора пола"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👩 Женский", callback_data="score2_gender_женский"),
            InlineKeyboardButton(text="👨 Мужской", callback_data="score2_gender_мужской")
        ]
    ])
    return keyboard

def create_smoking_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора курения"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚭 Не курю", callback_data="score2_smoking_не_курит"),
            InlineKeyboardButton(text="🚬 Курю", callback_data="score2_smoking_курит")
        ]
    ])
    return keyboard

def create_age_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора возраста (в 2 ряда)"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="< 40", callback_data="score2_age_менее_40"),
            InlineKeyboardButton(text="40-44", callback_data="score2_age_40-44"),
            InlineKeyboardButton(text="45-49", callback_data="score2_age_45-49"),
            InlineKeyboardButton(text="50-54", callback_data="score2_age_50-54")
        ],
        [
            InlineKeyboardButton(text="55-59", callback_data="score2_age_55-59"),
            InlineKeyboardButton(text="60-64", callback_data="score2_age_60-64"),
            InlineKeyboardButton(text="65-69", callback_data="score2_age_65-69"),
            InlineKeyboardButton(text="70-74", callback_data="score2_age_70-74")
        ],
        [
            InlineKeyboardButton(text="75-79", callback_data="score2_age_75-79"),
            InlineKeyboardButton(text="80-84", callback_data="score2_age_80-84"),
            InlineKeyboardButton(text="85-89", callback_data="score2_age_85-89"),
            InlineKeyboardButton(text="> 90", callback_data="score2_age_более_90")
        ]
    ])
    return keyboard

def create_bp_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора артериального давления"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="< 100", callback_data="score2_bp_менее_100"),
            InlineKeyboardButton(text="100-119", callback_data="score2_bp_100-119")
        ],
        [
            InlineKeyboardButton(text="120-139", callback_data="score2_bp_120-139"),
            InlineKeyboardButton(text="140-159", callback_data="score2_bp_140-159")
        ],
        [
            InlineKeyboardButton(text="160-179", callback_data="score2_bp_160-179"),
            InlineKeyboardButton(text="> 180", callback_data="score2_bp_более_180")
        ]
    ])
    return keyboard

def create_cholesterol_unit_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора единиц измерения холестерина"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="ммоль/л", callback_data="score2_chol_unit_mmol"),
            InlineKeyboardButton(text="мг/дл", callback_data="score2_chol_unit_mgdl")
        ]
    ])
    return keyboard

def create_cholesterol_mmol_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора холестерина в ммоль/л"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="< 3,0", callback_data="score2_chol_mmol_менее_3"),
            InlineKeyboardButton(text="3,0-3,9", callback_data="score2_chol_mmol_3.0-3.9")
        ],
        [
            InlineKeyboardButton(text="4,0-4,9", callback_data="score2_chol_mmol_4.0-4.9"),
            InlineKeyboardButton(text="5,0-5,9", callback_data="score2_chol_mmol_5.0-5.9")
        ],
        [
            InlineKeyboardButton(text="6,0-6,9", callback_data="score2_chol_mmol_6.0-6.9"),
            InlineKeyboardButton(text="> 6,9", callback_data="score2_chol_mmol_более_6.9")
        ]
    ])
    return keyboard

def create_cholesterol_mgdl_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора холестерина в мг/дл"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="< 150", callback_data="score2_chol_mgdl_менее_150"),
            InlineKeyboardButton(text="150-200", callback_data="score2_chol_mgdl_150-200")
        ],
        [
            InlineKeyboardButton(text="200-250", callback_data="score2_chol_mgdl_200-250"),
            InlineKeyboardButton(text="> 250", callback_data="score2_chol_mgdl_более_250")
        ]
    ])
    return keyboard

def create_restart_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для повторного прохождения"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Пройти заново", callback_data="score2_restart"),
            InlineKeyboardButton(text="📋 Главное меню", callback_data="score2_main_menu")
        ]
    ])
    return keyboard

# ============================================================================
# РОУТЕР И ХЕНДЛЕРЫ
# ============================================================================

score2_router = Router()

@score2_router.message(Command("score"))
async def cmd_score2_start(message: Message, state: FSMContext):
    """Начало прохождения SCORE2 калькулятора"""
    user_id = message.from_user.id
    
    try:
        # Логируем начало SCORE2
        await log_user_activity(
            telegram_id=user_id,
            action="score2_started",
            details={"method": "command"},
            state="score2_start"
        )
        
        welcome_text = """
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

Научно обоснованный инструмент Европейского кардиологического общества для оценки 10-летнего риска развития сердечно-сосудистых заболеваний.

📊 *Особенности SCORE2:*
• Основан на данных 13 млн человек из 45 стран
• Учитывает возраст, пол, курение, АД и холестерин
• Дает персональный прогноз риска инфаркта и инсульта
• Используется врачами по всему миру

📋 *Что вас ждет:*
• 5 простых вопросов (2-3 минуты)
• Точная оценка вашего риска
• Рекомендации по профилактике
• Возможность отслеживать изменения

⚕️ *Важно:* Результат носит информационный характер и не заменяет консультацию врача.

*Начнем с первого вопроса:*

👤 **Укажите ваш пол:**
        """
        
        await message.answer(
            text=welcome_text,
            reply_markup=create_gender_keyboard(),
            parse_mode="Markdown"
        )
        
        await state.set_state(Score2States.waiting_for_gender)
        
    except Exception as e:
        logger.error(f"Ошибка запуска SCORE2 для {user_id}: {e}")
        error_text = "❌ Произошла ошибка при запуске калькулятора. Попробуйте позже или обратитесь в поддержку."
        await message.answer(error_text)

@score2_router.callback_query(F.data.startswith("score2_gender_"))
async def process_gender(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора пола"""
    user_id = callback.from_user.id
    gender = callback.data.replace("score2_gender_", "")
    
    try:
        await state.update_data(gender=gender)
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_gender_selected", 
            details={"gender": gender},
            state="score2_gender"
        )
        
        text = f"""
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

✅ Пол: {"Женский" if gender == "женский" else "Мужской"}

🚬 **Вопрос 2 из 5:**

**Курите ли вы в настоящее время?**

_Включается любое курение: сигареты, трубка, сигары, электронные сигареты с никотином_
        """
        
        await callback.message.edit_text(
            text=text,
            reply_markup=create_smoking_keyboard(),
            parse_mode="Markdown"
        )
        
        await state.set_state(Score2States.waiting_for_smoking)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка обработки пола для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

@score2_router.callback_query(F.data.startswith("score2_smoking_"))
async def process_smoking(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора курения"""
    user_id = callback.from_user.id
    smoking = callback.data.replace("score2_smoking_", "")
    
    try:
        await state.update_data(smoking=smoking)
        data = await state.get_data()
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_smoking_selected",
            details={"smoking": smoking},
            state="score2_smoking"
        )
        
        smoking_text = "Не курю" if smoking == "не_курит" else "Курю"
        gender_text = "Женский" if data.get("gender") == "женский" else "Мужской"
        
        text = f"""
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

✅ Пол: {gender_text}
✅ Курение: {smoking_text}

🎂 **Вопрос 3 из 5:**

**Укажите ваш возраст:**

_Выберите подходящую возрастную группу_
        """
        
        await callback.message.edit_text(
            text=text,
            reply_markup=create_age_keyboard(),
            parse_mode="Markdown"
        )
        
        await state.set_state(Score2States.waiting_for_age)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка обработки курения для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

@score2_router.callback_query(F.data.startswith("score2_age_"))
async def process_age(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора возраста"""
    user_id = callback.from_user.id
    age = callback.data.replace("score2_age_", "")
    
    try:
        await state.update_data(age=age)
        data = await state.get_data()
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_age_selected",
            details={"age": age},
            state="score2_age"
        )
        
        # Преобразуем возраст для отображения
        age_display_map = {
            "менее_40": "Менее 40 лет",
            "40-44": "40-44 года",
            "45-49": "45-49 лет",
            "50-54": "50-54 года", 
            "55-59": "55-59 лет",
            "60-64": "60-64 года",
            "65-69": "65-69 лет",
            "70-74": "70-74 года",
            "75-79": "75-79 лет",
            "80-84": "80-84 года",
            "85-89": "85-89 лет",
            "более_90": "Старше 90 лет"
        }
        
        age_text = age_display_map.get(age, age)
        smoking_text = "Не курю" if data.get("smoking") == "не_курит" else "Курю" 
        gender_text = "Женский" if data.get("gender") == "женский" else "Мужской"
        
        text = f"""
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

✅ Пол: {gender_text}
✅ Курение: {smoking_text}
✅ Возраст: {age_text}

🩸 **Вопрос 4 из 5:**

**Систолическое артериальное давление**

_Верхнее значение АД в мм рт. ст. Если не знаете точно - укажите примерное_
        """
        
        await callback.message.edit_text(
            text=text,
            reply_markup=create_bp_keyboard(),
            parse_mode="Markdown"
        )
        
        await state.set_state(Score2States.waiting_for_blood_pressure)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка обработки возраста для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

@score2_router.callback_query(F.data.startswith("score2_bp_"))
async def process_blood_pressure(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора артериального давления"""
    user_id = callback.from_user.id
    bp = callback.data.replace("score2_bp_", "")
    
    try:
        await state.update_data(blood_pressure=bp)
        data = await state.get_data()
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_bp_selected",
            details={"blood_pressure": bp},
            state="score2_blood_pressure"
        )
        
        # Преобразуем АД для отображения
        bp_display_map = {
            "менее_100": "Менее 100",
            "100-119": "100-119",
            "120-139": "120-139", 
            "140-159": "140-159",
            "160-179": "160-179",
            "более_180": "Более 180"
        }
        
        bp_text = bp_display_map.get(bp, bp) + " мм рт. ст."
        
        # Собираем все данные для отображения
        age_display_map = {
            "менее_40": "Менее 40 лет",
            "40-44": "40-44 года",
            "45-49": "45-49 лет",
            "50-54": "50-54 года",
            "55-59": "55-59 лет", 
            "60-64": "60-64 года",
            "65-69": "65-69 лет",
            "70-74": "70-74 года",
            "75-79": "75-79 лет",
            "80-84": "80-84 года",
            "85-89": "85-89 лет",
            "более_90": "Старше 90 лет"
        }
        
        age_text = age_display_map.get(data.get("age"), data.get("age", ""))
        smoking_text = "Не курю" if data.get("smoking") == "не_курит" else "Курю"
        gender_text = "Женский" if data.get("gender") == "женский" else "Мужской"
        
        text = f"""
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

✅ Пол: {gender_text}
✅ Курение: {smoking_text}
✅ Возраст: {age_text}
✅ АД: {bp_text}

🧪 **Последний вопрос (5 из 5):**

**Уровень не-ЛПВП холестерина**

_Сначала выберите единицы измерения. Если не знаете результат анализа - укажите средние значения_

**В каких единицах указать холестерин?**
        """
        
        await callback.message.edit_text(
            text=text,
            reply_markup=create_cholesterol_unit_keyboard(),
            parse_mode="Markdown"
        )
        
        await state.set_state(Score2States.waiting_for_cholesterol_unit)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка обработки АД для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

@score2_router.callback_query(F.data.startswith("score2_chol_unit_"))
async def process_cholesterol_unit(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора единиц измерения холестерина"""
    user_id = callback.from_user.id
    unit = callback.data.replace("score2_chol_unit_", "")
    
    try:
        await state.update_data(cholesterol_unit=unit)
        data = await state.get_data()
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_chol_unit_selected",
            details={"cholesterol_unit": unit},
            state="score2_cholesterol_unit"
        )
        
        # Собираем все данные для отображения
        age_display_map = {
            "менее_40": "Менее 40 лет",
            "40-44": "40-44 года",
            "45-49": "45-49 лет",
            "50-54": "50-54 года",
            "55-59": "55-59 лет",
            "60-64": "60-64 года",
            "65-69": "65-69 лет",
            "70-74": "70-74 года",
            "75-79": "75-79 лет",
            "80-84": "80-84 года",
            "85-89": "85-89 лет",
            "более_90": "Старше 90 лет"
        }
        
        bp_display_map = {
            "менее_100": "Менее 100",
            "100-119": "100-119",
            "120-139": "120-139",
            "140-159": "140-159",
            "160-179": "160-179",
            "более_180": "Более 180"
        }
        
        age_text = age_display_map.get(data.get("age"), data.get("age", ""))
        bp_text = bp_display_map.get(data.get("blood_pressure"), data.get("blood_pressure", "")) + " мм рт. ст."
        smoking_text = "Не курю" if data.get("smoking") == "не_курит" else "Курю"
        gender_text = "Женский" if data.get("gender") == "женский" else "Мужской"
        unit_text = "ммоль/л" if unit == "mmol" else "мг/дл"
        
        if unit == "mmol":
            text = f"""
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

✅ Пол: {gender_text}
✅ Курение: {smoking_text}
✅ Возраст: {age_text}
✅ АД: {bp_text}

🧪 **Уровень не-ЛПВП холестерина ({unit_text}):**

_Выберите ваш уровень холестерина в ммоль/л_
            """
            keyboard = create_cholesterol_mmol_keyboard()
            next_state = Score2States.waiting_for_cholesterol_mmol
        else:
            text = f"""
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

✅ Пол: {gender_text}
✅ Курение: {smoking_text}
✅ Возраст: {age_text}
✅ АД: {bp_text}

🧪 **Уровень не-ЛПВП холестерина ({unit_text}):**

_Выберите ваш уровень холестерина в мг/дл_
            """
            keyboard = create_cholesterol_mgdl_keyboard()
            next_state = Score2States.waiting_for_cholesterol_mgdl
        
        await callback.message.edit_text(
            text=text,
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        
        await state.set_state(next_state)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка обработки единиц холестерина для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

@score2_router.callback_query(F.data.startswith("score2_chol_mmol_"))
async def process_cholesterol_mmol(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора холестерина в ммоль/л"""
    user_id = callback.from_user.id
    cholesterol = callback.data.replace("score2_chol_mmol_", "")
    
    try:
        await state.update_data(cholesterol=cholesterol, cholesterol_unit="mmol")
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_chol_mmol_selected",
            details={"cholesterol": cholesterol},
            state="score2_cholesterol_mmol"
        )
        
        await calculate_and_show_result(callback, state)
        
    except Exception as e:
        logger.error(f"Ошибка обработки холестерина (ммоль/л) для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

@score2_router.callback_query(F.data.startswith("score2_chol_mgdl_"))
async def process_cholesterol_mgdl(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора холестерина в мг/дл"""
    user_id = callback.from_user.id
    cholesterol = callback.data.replace("score2_chol_mgdl_", "")
    
    try:
        await state.update_data(cholesterol=cholesterol, cholesterol_unit="mgdl")
        
        await log_user_activity(
            telegram_id=user_id,
            action="score2_chol_mgdl_selected",
            details={"cholesterol": cholesterol},
            state="score2_cholesterol_mgdl"
        )
        
        await calculate_and_show_result(callback, state)
        
    except Exception as e:
        logger.error(f"Ошибка обработки холестерина (мг/дл) для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")

async def calculate_and_show_result(callback: CallbackQuery, state: FSMContext):
    """Расчет и отображение результата SCORE2"""
    user_id = callback.from_user.id
    
    data = await state.get_data()
    
    # Получаем все параметры
    gender = data.get("gender")
    smoking = data.get("smoking")
    age = data.get("age")
    bp = data.get("blood_pressure")
    cholesterol = data.get("cholesterol")
    cholesterol_unit = data.get("cholesterol_unit")
    
    # Преобразуем в группы для таблицы SCORE2
    age_group = get_age_group(age)
    bp_group = get_bp_group(bp)
    
    if cholesterol_unit == "mmol":
        chol_group = get_cholesterol_group_mmol(cholesterol)
    else:
        chol_group = get_cholesterol_group_mgdl(cholesterol)
    
    # Рассчитываем риск
    risk_score = calculate_score2_risk(gender, smoking, age_group, bp_group, chol_group)
    risk_info = get_risk_interpretation(risk_score)
    
    # Логируем результат
    await log_user_activity(
        telegram_id=user_id,
        action="score2_completed",
        details={
            "gender": gender,
            "smoking": smoking,
            "age": age,
            "blood_pressure": bp,
            "cholesterol": cholesterol,
            "cholesterol_unit": cholesterol_unit,
            "risk_score": risk_score,
            "risk_level": risk_info["level"]
        },
        state="score2_result"
    )
    
    # Формируем текст результата
    age_display_map = {
        "менее_40": "Менее 40 лет",
        "40-44": "40-44 года", 
        "45-49": "45-49 лет",
        "50-54": "50-54 года",
        "55-59": "55-59 лет",
        "60-64": "60-64 года",
        "65-69": "65-69 лет",
        "70-74": "70-74 года",
        "75-79": "75-79 лет",
        "80-84": "80-84 года",
        "85-89": "85-89 лет",
        "более_90": "Старше 90 лет"
    }
    
    bp_display_map = {
        "менее_100": "Менее 100",
        "100-119": "100-119",
        "120-139": "120-139",
        "140-159": "140-159",
        "160-179": "160-179",
        "более_180": "Более 180"
    }
    
    chol_display_map_mmol = {
        "менее_3": "Менее 3,0",
        "3.0-3.9": "3,0-3,9",
        "4.0-4.9": "4,0-4,9",
        "5.0-5.9": "5,0-5,9",
        "6.0-6.9": "6,0-6,9",
        "более_6.9": "Более 6,9"
    }
    
    chol_display_map_mgdl = {
        "менее_150": "Менее 150",
        "150-200": "150-200",
        "200-250": "200-250", 
        "более_250": "Более 250"
    }
    
    age_text = age_display_map.get(age, age)
    bp_text = bp_display_map.get(bp, bp) + " мм рт. ст."
    smoking_text = "Не курю" if smoking == "не_курит" else "Курю"
    gender_text = "Женский" if gender == "женский" else "Мужской"
    
    if cholesterol_unit == "mmol":
        chol_text = chol_display_map_mmol.get(cholesterol, cholesterol) + " ммоль/л"
    else:
        chol_text = chol_display_map_mgdl.get(cholesterol, cholesterol) + " мг/дл"
    
    result_text = f"""
🩺 *РЕЗУЛЬТАТ SCORE2*

📊 **Ваши данные:**
• Пол: {gender_text}
• Курение: {smoking_text}
• Возраст: {age_text}
• АД: {bp_text}
• Холестерин: {chol_text}

{risk_info["color"]} **Ваш 10-летний риск: {risk_score}%**

📈 **Уровень риска: {risk_info["level"]}**

{risk_info["description"]}

📅 Дата расчета: {datetime.now().strftime("%d.%m.%Y")}
    """
    
    await callback.message.edit_text(
        text=result_text,
        reply_markup=create_restart_keyboard(),
        parse_mode="Markdown"
    )
    
    await state.set_state(Score2States.showing_result)
    await callback.answer()
        
@score2_router.callback_query(F.data == "score2_main_menu")
async def back_to_main_menu(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    user_id = callback.from_user.id
    
    try:
        await log_user_activity(
            telegram_id=user_id,
            action="score2_exit_to_menu",
            details={"method": "main_menu_button"},
            state="score2_exit"
        )
        
        # Очищаем состояние
        await state.clear()
        
        # Отправляем в главное меню
        main_menu_text = """
🩺 *Добро пожаловать в главное меню!*

Выберите необходимое действие:

🚀 `/start` - Начать диагностику
❓ `/help` - Помощь и инструкции
🔄 `/restart` - Начать заново
📈 `/score` - SCORE2 калькулятор риска

Для продолжения выберите команду из меню или введите её вручную.
        """
        
        await callback.message.edit_text(
            text=main_menu_text,
            parse_mode="Markdown"
        )
        
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка возврата в меню для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка")


logger.info("✅ SCORE2 Handler загружен успешно")

@score2_router.callback_query(F.data == "score2_restart")
async def restart_score2(callback: CallbackQuery, state: FSMContext):
    """Перезапуск SCORE2 калькулятора"""
    user_id = callback.from_user.id
    
    try:
        await log_user_activity(
            telegram_id=user_id,
            action="score2_restarted",
            details={"method": "restart_button"},
            state="score2_restart"
        )
        
        # Очищаем состояние
        await state.clear()
        
        welcome_text = """
🩺 *SCORE2 - Калькулятор сердечно-сосудистого риска*

Этот научно обоснованный инструмент поможет оценить ваш 10-летний риск развития сердечно-сосудистых заболеваний.

📋 *Что вас ждет:*
• 5 простых вопросов
• Персональная оценка риска
• Рекомендации по профилактике

⚕️ *Важно:* Результат носит информационный характер и не заменяет консультацию врача.

*Начнем с первого вопроса:*

👤 **Укажите ваш пол:**
        """
        
        await callback.message.edit_text(
            text=welcome_text,
            reply_markup=create_gender_keyboard(),
            parse_mode="Markdown"
        )
        
        await state.set_state(Score2States.waiting_for_gender)
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Ошибка перезапуска SCORE2 для {user_id}: {e}")
        await callback.answer("❌ Произошла ошибка при перезапуске")