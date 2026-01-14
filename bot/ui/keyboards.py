from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import List

def get_start_keyboard():
    """Клавиатура для начального сообщения"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Старт", callback_data="start_bot")]
    ])
    return keyboard



def get_gender_keyboard():
    """Клавиатура для выбора пола"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Женский", callback_data="gender_female")],
        [InlineKeyboardButton(text="Мужской", callback_data="gender_male")]
    ])
    return keyboard

def get_location_keyboard():
    """Клавиатура для выбора места жительства"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Город с населением >1 млн", callback_data="location_big_city")],
        [InlineKeyboardButton(text="Город 500–999 тыс", callback_data="location_medium_city")],
        [InlineKeyboardButton(text="Город с населением 100–500 тыс", callback_data="location_small_city")],
        [InlineKeyboardButton(text="Город до 100 тыс", callback_data="location_town")],
        [InlineKeyboardButton(text="Поселок / сельская местность", callback_data="location_village")]
    ])
    return keyboard

def get_education_keyboard():
    """Клавиатура для выбора образования"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Среднее общее", callback_data="education_secondary")],
        [InlineKeyboardButton(text="Средне-специальное", callback_data="education_vocational")],
        [InlineKeyboardButton(text="Высшее (немедицинское)", callback_data="education_higher")],
        [InlineKeyboardButton(text="Высшее медицинское", callback_data="education_medical")]
    ])
    return keyboard

def get_family_keyboard():
    """Клавиатура для выбора семейного положения"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Холост / не замужем", callback_data="family_single")],
        [InlineKeyboardButton(text="В браке", callback_data="family_married")],
        [InlineKeyboardButton(text="Разведён(а) / вдов(ец/а)", callback_data="family_divorced")]
    ])
    return keyboard

def get_children_keyboard():
    """Клавиатура для наличия детей"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Нет", callback_data="children_none")],
        [InlineKeyboardButton(text="Да, один", callback_data="children_one")],
        [InlineKeyboardButton(text="Да, двое и более", callback_data="children_multiple")]
    ])
    return keyboard

def get_income_keyboard():
    """Клавиатура для выбора дохода"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="До 20 000 ₽", callback_data="income_low")],
        [InlineKeyboardButton(text="20–40 тыс ₽", callback_data="income_medium")],
        [InlineKeyboardButton(text="40–70 тыс ₽", callback_data="income_high")],
        [InlineKeyboardButton(text="Более 70 тыс ₽", callback_data="income_very_high")],
        [InlineKeyboardButton(text="Предпочитаю не указывать", callback_data="income_no_answer")]
    ])
    return keyboard

def get_death_cause_keyboard():
    """Клавиатура для причин смерти"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 Онкологические заболевания", callback_data="death_cause_cancer")],
        [InlineKeyboardButton(text="2 Сердечно-сосудистые заболевания", callback_data="death_cause_cardio")],
        [InlineKeyboardButton(text="3 Инфекции", callback_data="death_cause_infections")],
        [InlineKeyboardButton(text="4 Болезни дыхательных путей", callback_data="death_cause_respiratory")],
        [InlineKeyboardButton(text="5 Болезни желудочно-кишечного тракта", callback_data="death_cause_digestive")],
        [InlineKeyboardButton(text="6 Внешние причины", callback_data="death_cause_external")]
    ])
    return keyboard

def get_heart_disease_keyboard():
    """Клавиатура для заболеваний сердца"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="heart_disease_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="heart_disease_no")],
        [InlineKeyboardButton(text="Не знаю / не обследовался(ась)", callback_data="heart_disease_unknown")]
    ])
    return keyboard

def get_cv_risk_keyboard():
    """Клавиатура для сердечно-сосудистого риска"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="низкий/умеренный", callback_data="cv_risk_low")],
        [InlineKeyboardButton(text="высокий", callback_data="cv_risk_high")],
        [InlineKeyboardButton(text="очень высокий", callback_data="cv_risk_very_high")]
    ])
    return keyboard

def get_cv_knowledge_keyboard():
    """Клавиатура для знания о факторах риска"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, хорошо разбираюсь", callback_data="cv_knowledge_good")],
        [InlineKeyboardButton(text="Да, но не до конца понимаю", callback_data="cv_knowledge_some")],
        [InlineKeyboardButton(text="Нет / почти ничего не знаю", callback_data="cv_knowledge_none")]
    ])
    return keyboard

def get_heart_danger_keyboard(selected: List[str]):
    """Клавиатура для опасных факторов сердца (мультивыбор до 3)"""
    options = [
        ("Возраст", "heart_danger_age"),
        ("Мужской пол", "heart_danger_male"),
        ("Семейный анамнез ранних сердечно-сосудистых заболеваний", "heart_danger_family"),
        ("Повышенное артериальное давление", "heart_danger_pressure"),
        ("Повышенный холестерин", "heart_danger_cholesterol"),
        ("Повышение глюкозы в крови", "heart_danger_glucose"),
        ("Избыточный вес", "heart_danger_weight"),
        ("Курение", "heart_danger_smoking"),
        ("Алкоголь", "heart_danger_alcohol"),
        ("Несбалансированное питание", "heart_danger_nutrition"),
        ("Малоподвижный образ жизни", "heart_danger_sedentary"),
        ("Стрессы", "heart_danger_stress"),
        ("Нарушение сна, храп", "heart_danger_sleep")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text=f"Готово ({len(selected)}/3)", callback_data="heart_danger_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_health_importance_keyboard():
    """Клавиатура для важности наблюдения за здоровьем сердца"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Это для пожилых / хронически больных, не про меня", callback_data="health_importance_elderly")],
        [InlineKeyboardButton(text="Важно, но не на первом месте", callback_data="health_importance_secondary")],
        [InlineKeyboardButton(text="Понимаю, что нужно, но раньше об этом не думал(а)", callback_data="health_importance_understand")],
        [InlineKeyboardButton(text="Осознаю значимость — планирую действовать", callback_data="health_importance_plan")]
    ])
    return keyboard

def get_checkup_history_keyboard():
    """Клавиатура для истории кардиочекапов"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, проходил(а)", callback_data="checkup_history_yes")],
        [InlineKeyboardButton(text="Проходил(а) только отдельные обследования", callback_data="checkup_history_partial")],
        [InlineKeyboardButton(text="Нет, никогда", callback_data="checkup_history_no")]
    ])
    return keyboard

def get_checkup_content_keyboard(selected: List[str]):
    """Клавиатура для содержимого кардиочекапа (мультивыбор) - СООТВЕТСТВУЕТ ТЗ"""
    # ТОЧНЫЕ названия из ТЗ
    options = [
        ("Консультация и осмотр врача-кардиолога / терапевта", "checkup_content_consultation"),
        ("Оценка факторов риска сердечно-сосудистых заболеваний", "checkup_content_risk_assessment"),
        ("Определение уровня липидов крови", "checkup_content_lipids"),
        ("Определение уровня глюкозы крови", "checkup_content_glucose"),
        ("ЭКГ", "checkup_content_ecg"),
        ("УЗИ сосудов (дуплексное сканирование)", "checkup_content_ultrasound"),
        ("ЭхоКГ", "checkup_content_echo"),
        ("Суточное мониторирование давления", "checkup_content_monitoring"),
        ("МСКТ-коронарный кальций", "checkup_content_ct"),
        ("Расчет индивидуального СС-риска", "checkup_content_calc")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка "Не проходил(а)" всегда доступна
    not_passed_selected = "Не проходил(а) кардиочекап" in selected
    prefix = "✅ " if not_passed_selected else "☐ "
    buttons.append([InlineKeyboardButton(text=prefix + "Не проходил(а) кардиочекап", callback_data="checkup_content_skip")])
    
    # Кнопка "Готово" с отображением количества выбранных элементов
    # Считаем количество выбранных обычных пунктов (не включая "не проходил")
    normal_selected_count = len([item for item in selected if item != "Не проходил(а) кардиочекап"])
    max_selections = 5  # Максимум 5 вариантов для вопроса 16
    buttons.append([InlineKeyboardButton(text=f"✅ Готово ({normal_selected_count}/{max_selections})", callback_data="checkup_content_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_prevention_barriers_keyboard(selected: List[str]):
    """Клавиатура для препятствий профилактического обследования (мультивыбор)"""
    options = [
        ("Не вижу необходимости — нет симптомов", "prevention_barriers_no_symptoms"),
        ("Страх услышать диагноз", "prevention_barriers_fear"),
        ("Финансовые ограничения", "prevention_barriers_money"),
        ("Нет времени", "prevention_barriers_time"),
        ("Не знаю, с чего начать", "prevention_barriers_knowledge"),
        ("Уже наблюдаюсь у врача", "prevention_barriers_doctor"),
        ("Ничего не мешает", "prevention_barriers_nothing")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка "Готово" с отображением количества выбранных элементов
    selected_count = len(selected)
    max_selections = 3  # Максимум 3 варианта для вопроса 17
    buttons.append([InlineKeyboardButton(text=f"✅ Готово ({selected_count}/{max_selections})", callback_data="prevention_barriers_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_health_advice_keyboard(selected: List[str]):
    """Клавиатура для источников советов по здоровью (мультивыбор до 2)"""
    options = [
        ("С врачом", "health_advice_doctor"),
        ("С родственниками", "health_advice_relatives"),  # Проверьте этот callback_data
        ("С коллегами", "health_advice_colleagues"),
        ("Через интернет (статьи, форумы)", "health_advice_internet"),
        ("С врачом-блогером в соцсетях", "health_advice_blogger"),  # Проверьте этот callback_data
        ("Ни с кем", "health_advice_nobody")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text=f"Готово ({len(selected)}/2)", callback_data="health_advice_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_test_selection_keyboard(completed_data=None):
    """ОБНОВЛЕННАЯ клавиатура для выбора тестов - более либеральная логика завершения"""
    if completed_data is None:
        completed_data = {}
    
    buttons = []
    
    # Обязательные тесты
    tests = [
        ("🟣 Тест HADS (тревога и депрессия)", "test_hads", ["hads_anxiety_score", "completed_hads"]),
        ("🔵 Тест Бернса (эмоциональное выгорание)", "test_burns", ["burns_score", "completed_burns"]),
        ("🌙 Тест ISI (качество сна)", "test_isi", ["isi_score", "completed_isi"]),
        ("😴 Тест STOP-BANG (апноэ сна)", "test_stop_bang", ["stop_bang_score", "completed_stop_bang"]),
        ("😴 Тест ESS (дневная сонливость)", "test_ess", ["ess_score", "completed_ess"])
    ]
    
    completed_count = 0
    for text, callback_data, check_keys in tests:
        # Проверяем, завершен ли тест
        completed = any(key in completed_data for key in check_keys)
        if completed:
            completed_count += 1
        
        prefix = "✅ " if completed else "⭕ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Фагерстрем (необязательный) - кнопка "Не курю" теперь внутри теста на первом вопросе
    fagerstrom_keys = ["fagerstrom_score", "fagerstrom_skipped", "completed_fagerstrom"]
    fagerstrom_completed = any(key in completed_data for key in fagerstrom_keys)

    if fagerstrom_completed:
        if completed_data.get("fagerstrom_skipped"):
            buttons.append([InlineKeyboardButton(text="⏭ 🚬 Тест Фагерстрема (не курю)", callback_data="test_fagerstrom")])
        else:
            buttons.append([InlineKeyboardButton(text="✅ 🚬 Тест Фагерстрема (никотиновая зависимость)", callback_data="test_fagerstrom")])
    else:
        buttons.append([InlineKeyboardButton(text="⭕ 🚬 Тест Фагерстрема (никотиновая зависимость)", callback_data="test_fagerstrom")])

    # AUDIT (необязательный) - кнопка "Не пью" теперь внутри теста на первом вопросе
    audit_keys = ["audit_score", "audit_skipped", "completed_audit"]
    audit_completed = any(key in completed_data for key in audit_keys)

    if audit_completed:
        if completed_data.get("audit_skipped"):
            buttons.append([InlineKeyboardButton(text="⏭ 🍷 Тест AUDIT (не употребляю)", callback_data="test_audit")])
        else:
            buttons.append([InlineKeyboardButton(text="✅ 🍷 Тест AUDIT (употребление алкоголя)", callback_data="test_audit")])
    else:
        buttons.append([InlineKeyboardButton(text="⭕ 🍷 Тест AUDIT (употребление алкоголя)", callback_data="test_audit")])
    
    # ИСПРАВЛЕННАЯ логика кнопок завершения - минимум 5 тестов + учет пропущенных
    # Считаем общее количество завершенных тестов (включая пропущенные)
    total_completed = completed_count
    if fagerstrom_completed:
        total_completed += 1
    if audit_completed:
        total_completed += 1
    
    if completed_count >= 5:  # Все 5 обязательных тестов пройдены
        buttons.append([InlineKeyboardButton(text="🎯 Завершить и получить материалы", callback_data="test_complete")])
    elif completed_count >= 3:  # 3-4 теста пройдено - еще нужно
        buttons.append([InlineKeyboardButton(text="📝 Пройти еще тесты (минимум 5)", callback_data="continue_tests")])
        buttons.append([InlineKeyboardButton(text="🔄 Проверить готовность", callback_data="test_check_completion")])
    elif completed_count >= 1:  # 1-2 теста пройдено
        buttons.append([InlineKeyboardButton(text="🔄 Проверить готовность", callback_data="test_check_completion")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_keyboard(question, test_type):
    """Клавиатура для вопроса теста"""
    buttons = []
    
    for i, option in enumerate(question['options']):
        text = option['text']
        score = option['score']
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"answer_{score}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_continue_keyboard():
    """Клавиатура для продолжения после завершения теста"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Продолжить", callback_data="continue_tests")]
    ])
    return keyboard

def get_yes_no_keyboard(yes_callback, no_callback):
    """Универсальная клавиатура Да/Нет"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data=yes_callback)],
        [InlineKeyboardButton(text="Нет", callback_data=no_callback)]
    ])
    return keyboard