"""
Модуль для опроса ТОЧКА 3 (13 вопросов через 3+ месяца после вебинара)
Реализует опрос согласно ТЗ в файле punchline.txt
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from .base import safe_edit_message, safe_answer_callback, log_user_interaction
from database import get_db_sync, User, FollowUpSurvey, FollowUpStatus

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# СОСТОЯНИЯ ДЛЯ ОПРОСА ТОЧКА 2 и 3 (20 вопросов согласно new_addition.txt)
# ============================================================================

class FollowUpSurveyStates(StatesGroup):
    # ТОЧКА 2 - Вопросы сразу после вебинара (1-7)
    q1_understanding = State()    # Понятность кардиочекапа
    q2_attitude = State()         # Изменение отношения к профилактике
    q3_problems = State()         # Проблемы при скрининге (мультивыбор)
    q4_risk = State()             # Оценка СС риска
    q5_plans = State()            # Планируемые действия (мультивыбор)
    q6_doctor_plan = State()      # Планы обращения к врачу
    q7_webinar_influence = State()  # Влияние вебинара
    # ТОЧКА 3 - Вопросы через 3+ месяца (8-20)
    q8_doctor_visit = State()     # Обращение к врачу
    q9_doctor_type = State()      # К какому врачу (мультивыбор)
    q10_doctor_attitude = State() # Отношение врача
    q11_visit_actions = State()   # Что было сделано на приеме (мультивыбор)
    q12_seriousness = State()     # Серьезность врача (шкала 1-10)
    q13_recommendations = State() # Следование рекомендациям
    q13_barriers = State()        # Что мешает (если не полностью соблюдают)
    q14_risk_steps = State()      # Шаги по снижению рисков (мультивыбор)
    q15_stability = State()       # Стабильность изменений (шкала 0-10)
    q16_difficulties = State()    # Трудности (мультивыбор)
    q17_attitude_change = State() # Изменение отношения к профилактике
    q18_confidence = State()      # Уверенность в понимании (шкала 0-10)
    q19_info_need = State()       # Потребность в дополнительной информации (мультивыбор)
    q20_main_change = State()     # Главное изменение (текст)
    # Обратная совместимость со старыми состояниями
    question_1 = State()
    question_2 = State()
    question_3 = State()
    question_4 = State()
    question_5 = State()
    question_6 = State()
    question_6_barriers = State()
    question_7 = State()
    question_8 = State()
    question_9 = State()
    question_10 = State()
    question_11 = State()
    question_12 = State()
    question_13 = State()

# ============================================================================
# КЛАВИАТУРЫ ДЛЯ ОПРОСА ТОЧКА 2 (Вопросы 1-7)
# ============================================================================

def get_q1_understanding_keyboard():
    """Вопрос 1: Понятность кардиочекапа"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Я полностью понял(а), что это и зачем", callback_data="nq1_fully")],
        [InlineKeyboardButton(text="Стало немного понятнее, чем раньше", callback_data="nq1_clearer")],
        [InlineKeyboardButton(text="Всё ещё не понимаю", callback_data="nq1_not_clear")]
    ])
    return keyboard

def get_q2_attitude_keyboard():
    """Вопрос 2: Изменение отношения к профилактическому обследованию"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Не изменилось, я и до вебинара планировал(а) пройти кардиочекап", callback_data="nq2_no_change_planned")],
        [InlineKeyboardButton(text="Не изменилось, по-прежнему не вижу смысла", callback_data="nq2_no_change_no_sense")],
        [InlineKeyboardButton(text="Стал(а) относиться серьёзнее, но пока не готов(а) действовать", callback_data="nq2_serious_not_ready")],
        [InlineKeyboardButton(text="Появилось желание пройти обследование", callback_data="nq2_want_exam")],
        [InlineKeyboardButton(text="Уже принял(а) решение действовать и начать обследование", callback_data="nq2_decided_action")]
    ])
    return keyboard

def get_q3_problems_keyboard(selected: List[str]):
    """Вопрос 3: Проблемы, выявленные при скрининге (множественный выбор)"""
    options = [
        ("Семейный анамнез ранних ССЗ", "nq3_family_history"),
        ("Несбалансированное питание", "nq3_nutrition"),
        ("Курение", "nq3_smoking"),
        ("Алкоголь", "nq3_alcohol"),
        ("Низкий уровень физ. активности", "nq3_low_activity"),
        ("Высокий уровень стресса", "nq3_stress"),
        ("Нарушение сна", "nq3_sleep"),
        ("Избыточный вес / ожирение", "nq3_weight"),
        ("Повышение артериального давления", "nq3_bp"),
        ("Повышение липидов", "nq3_lipids"),
        ("Повышение глюкозы", "nq3_glucose"),
        ("Ничего не выявил(а)", "nq3_nothing")
    ]
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="nq3_done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_q4_risk_keyboard():
    """Вопрос 4: Оценка сердечно-сосудистого риска"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Низкий / умеренный", callback_data="nq4_low_moderate")],
        [InlineKeyboardButton(text="Высокий", callback_data="nq4_high")],
        [InlineKeyboardButton(text="Очень высокий", callback_data="nq4_very_high")]
    ])
    return keyboard

def get_q5_plans_keyboard(selected: List[str]):
    """Вопрос 5: Планируемые действия по модификации образа жизни (множественный выбор)"""
    options = [
        ("Отказаться от курения", "nq5_quit_smoking"),
        ("Ограничить / исключить алкоголь", "nq5_limit_alcohol"),
        ("Скорректировать питание", "nq5_fix_nutrition"),
        ("Увеличить уровень физ. активности", "nq5_increase_activity"),
        ("Решить проблемы со сном", "nq5_fix_sleep"),
        ("Снизить уровень стресса", "nq5_reduce_stress"),
        ("Пока не планирую ничего делать", "nq5_nothing")
    ]
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="nq5_done")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_q6_doctor_plan_keyboard():
    """Вопрос 6: Планы обращения к врачу"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="nq6_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="nq6_no")],
        [InlineKeyboardButton(text="Пока не решил(а)", callback_data="nq6_undecided")]
    ])
    return keyboard

def get_q7_webinar_influence_keyboard():
    """Вопрос 7: Влияние вебинара на мотивацию"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Решающее влияние — до него даже не задумывался(ась)", callback_data="nq7_decisive")],
        [InlineKeyboardButton(text="Существенное влияние — помог систематизировать знания", callback_data="nq7_significant")],
        [InlineKeyboardButton(text="Незначительное влияние — многое уже было известно", callback_data="nq7_minor")],
        [InlineKeyboardButton(text="Без влияния — не изменил моего отношения", callback_data="nq7_none")]
    ])
    return keyboard

# ============================================================================
# КЛАВИАТУРЫ ДЛЯ ОПРОСА ТОЧКА 3 (Вопросы 8-20, бывшие 1-13)
# ============================================================================

def get_question_1_keyboard():
    """Вопрос 8 (бывший 1): Обращение к врачу"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да, специально пошёл(ла) к врачу после вебинара", callback_data="fq1_yes_after_webinar")],
        [InlineKeyboardButton(text="Да, я и раньше наблюдалась, просто обсудил(а) эту тему на приёме", callback_data="fq1_yes_discussed")],
        [InlineKeyboardButton(text="Нет, не обращался(ась), считаю, что у меня нет серьёзных рисков", callback_data="fq1_no_no_risks")],
        [InlineKeyboardButton(text="Нет, пока не дошёл(ла), но планирую", callback_data="fq1_no_planning")]
    ])
    return keyboard

def get_question_2_keyboard(selected: List[str]):
    """Вопрос 2: К какому врачу обращались (до 2 вариантов)"""
    options = [
        ("Кардиолог", "fq2_cardiologist"),
        ("Терапевт / врач общей практики", "fq2_therapist"),
        ("Другой специалист", "fq2_other")
    ]

    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])

    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="fq2_done")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_3_keyboard():
    """Вопрос 3: Отношение врача"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Врач поддержал мой интерес к профилактике, всё подробно объяснил(а)", callback_data="fq3_supportive")],
        [InlineKeyboardButton(text="Врач отнесся нейтрально, без особого интереса", callback_data="fq3_neutral")],
        [InlineKeyboardButton(text="Врач отнесся скептически / пренебрежительно", callback_data="fq3_skeptical")],
        [InlineKeyboardButton(text="Не обращался(ась) к врачу после вебинара", callback_data="fq3_no_visit")]
    ])
    return keyboard

def get_question_4_keyboard(selected: List[str]):
    """Вопрос 4: Что было сделано на приеме (мультивыбор)"""
    options = [
        ("Расспросил о жалобах и факторах риска", "fq4_detailed_questioning"),
        ("Пересмотрел анализы и обследования", "fq4_reviewed_tests"),
        ("Назначил доп. анализы/обследования", "fq4_additional_tests"),
        ("Рассчитал сердечно-сосудистый риск", "fq4_risk_calculation"),
        ("Изменил лечение/добавил препараты", "fq4_treatment_change"),
        ("Сказал «всё в норме» без объяснений", "fq4_no_worries"),
        ("Другое", "fq4_other")
    ]

    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])

    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="fq4_done")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_5_keyboard():
    """Вопрос 5: Серьезность врача (шкала 1-10)"""
    buttons = []
    row = []
    for i in range(1, 11):
        row.append(InlineKeyboardButton(text=str(i), callback_data=f"fq5_rating_{i}"))
        if i % 5 == 0:
            buttons.append(row)
            row = []

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_6_keyboard():
    """Вопрос 6: Следование рекомендациям"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Полностью соблюдаю", callback_data="fq6_fully_compliant")],
        [InlineKeyboardButton(text="В основном соблюдаю, но иногда отклоняюсь", callback_data="fq6_mostly_compliant")],
        [InlineKeyboardButton(text="Следую частично", callback_data="fq6_partially")],
        [InlineKeyboardButton(text="Практически не соблюдаю", callback_data="fq6_non_compliant")],
        [InlineKeyboardButton(text="Рекомендаций по профилактике почти не было", callback_data="fq6_no_recommendations")]
    ])
    return keyboard

def get_question_7_keyboard(selected: List[str]):
    """Вопрос 7: Шаги по снижению рисков (мультивыбор)"""
    options = [
        ("Бросил/сократил курение", "fq7_quit_smoking"),
        ("Сократил употребление алкоголя", "fq7_reduce_alcohol"),
        ("Изменил питание", "fq7_diet_change"),
        ("Увеличил физическую активность", "fq7_increase_activity"),
        ("Нормализовал холестерин/липиды", "fq7_lipids"),
        ("Нормализовалось давление", "fq7_bp"),
        ("Начал приём препаратов", "fq7_medications"),
        ("Управляю стрессом/наладил сон", "fq7_stress_sleep"),
        ("Работаю над снижением веса", "fq7_weight_loss"),
        ("Пока ничего не делал(а)", "fq7_nothing"),
        ("Другое", "fq7_other")
    ]

    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])

    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="fq7_done")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_8_keyboard():
    """Вопрос 8: Стабильность изменений (шкала 0-10)"""
    buttons = []
    row = []
    for i in range(0, 11):
        row.append(InlineKeyboardButton(text=str(i), callback_data=f"fq8_rating_{i}"))
        if (i + 1) % 6 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_9_keyboard(selected: List[str]):
    """Вопрос 9: Трудности (мультивыбор)"""
    options = [
        ("Недостаток времени", "fq9_time"),
        ("Сложно отказаться от привычек", "fq9_habits"),
        ("Трудно соблюдать режим (работа/семья)", "fq9_schedule"),
        ("Финансовые ограничения", "fq9_finances"),
        ("Недостаток поддержки окружения", "fq9_support"),
        ("Врач не придал значения рискам", "fq9_doctor"),
        ("Не понимаю, что конкретно делать", "fq9_understanding"),
        ("Усталость/выгорание", "fq9_burnout"),
        ("Не вижу эффекта от изменений", "fq9_no_effect"),
        ("Другое", "fq9_other")
    ]

    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])

    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="fq9_done")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_10_keyboard():
    """Вопрос 10: Изменение отношения к профилактике"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Стал(а) относиться серьезнее и уже действую", callback_data="fq10_serious_acting")],
        [InlineKeyboardButton(text="Стал(а) относиться серьёзнее, но действовать пока трудно", callback_data="fq10_serious_difficult")],
        [InlineKeyboardButton(text="В целом отношение не изменилось", callback_data="fq10_no_change")],
        [InlineKeyboardButton(text="Стал(а) спокойнее, потому что лучше понимаю свою ситуацию", callback_data="fq10_calmer")]
    ])
    return keyboard

def get_question_11_keyboard():
    """Вопрос 11: Уверенность в понимании (шкала 0-10)"""
    buttons = []
    row = []
    for i in range(0, 11):
        row.append(InlineKeyboardButton(text=str(i), callback_data=f"fq11_rating_{i}"))
        if (i + 1) % 6 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_12_keyboard(selected: List[str]):
    """Вопрос 12: Потребность в дополнительной информации (мультивыбор)"""
    options = [
        ("Да, про анализы и обследования", "fq12_tests"),
        ("Да, про давление и гипертензию", "fq12_bp"),
        ("Да, про холестерин, липиды, статины", "fq12_lipids"),
        ("Да, про питание и вес", "fq12_nutrition"),
        ("Да, про сон и режим дня", "fq12_sleep"),
        ("Да, про дисциплину и мотивацию", "fq12_motivation"),
        ("Да, про физ. активность и нагрузки", "fq12_activity"),
        ("Да, про женское здоровье/менопаузу", "fq12_women_health"),
        ("Да, про общение с врачами", "fq12_doctor_interaction"),
        ("Нет, сейчас информации достаточно", "fq12_no"),
        ("Другое", "fq12_other")
    ]

    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])

    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="fq12_done")])

    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

# ============================================================================
# НАЧАЛО ОПРОСА (вызывается при нажатии кнопки "Начать опрос")
# ============================================================================

@router.callback_query(F.data == "start_followup_survey")
async def start_followup_survey(callback: CallbackQuery, state: FSMContext):
    """Запуск опроса ТОЧКА 3"""
    chat_id = callback.from_user.id
    await safe_answer_callback(callback)
    await log_user_interaction(chat_id, "followup_survey_started")

    # Инициализируем данные опроса
    await state.update_data(followup_survey_data={})

    # Отмечаем начало опроса в базе
    def _mark_started():
        db = get_db_sync()
        try:
            status = db.query(FollowUpStatus).filter(FollowUpStatus.telegram_id == chat_id).first()
            if status:
                status.survey_started = True
                status.survey_started_at = datetime.now()
            else:
                status = FollowUpStatus(
                    telegram_id=chat_id,
                    survey_started=True,
                    survey_started_at=datetime.now()
                )
                db.add(status)
            db.commit()
        finally:
            db.close()

    await asyncio.get_event_loop().run_in_executor(None, _mark_started)

    # Начинаем с вопроса 1 (ТОЧКА 2) - понятность кардиочекапа
    text = """🟣 <b>Вопрос 1 из 20</b>

После прохождения скрининга и вебинара стало ли вам понятнее, что такое «кардиочекап» и зачем он нужен?
(выберите 1 вариант)"""

    keyboard = get_q1_understanding_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.q1_understanding)

# ============================================================================
# ОБРАБОТЧИКИ ВОПРОСОВ ТОЧКА 2 (Вопросы 1-7)
# ============================================================================

@router.callback_query(F.data.startswith("nq1_"), StateFilter(FollowUpSurveyStates.q1_understanding))
async def handle_q1_understanding(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 1 (понятность кардиочекапа)"""
    await safe_answer_callback(callback)

    answer_map = {
        "nq1_fully": "Я полностью понял(а), что это и зачем",
        "nq1_clearer": "Стало немного понятнее, чем раньше",
        "nq1_not_clear": "Всё ещё не понимаю"
    }

    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['understanding_cardio'] = answer_map.get(callback.data, callback.data)
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 2
    text = """🟣 <b>Вопрос 2 из 20</b>

Как изменилось ваше отношение к профилактическому обследованию после вебинара?
(выберите 1 вариант)"""

    keyboard = get_q2_attitude_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.q2_attitude)

@router.callback_query(F.data.startswith("nq2_"), StateFilter(FollowUpSurveyStates.q2_attitude))
async def handle_q2_attitude(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 2 (изменение отношения)"""
    await safe_answer_callback(callback)

    answer_map = {
        "nq2_no_change_planned": "Не изменилось, я и до вебинара планировал(а) пройти кардиочекап",
        "nq2_no_change_no_sense": "Не изменилось, по-прежнему не вижу смысла",
        "nq2_serious_not_ready": "Стал(а) относиться серьёзнее, но пока не готов(а) действовать",
        "nq2_want_exam": "Появилось желание пройти обследование",
        "nq2_decided_action": "Уже принял(а) решение действовать и начать обследование"
    }

    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['attitude_change'] = answer_map.get(callback.data, callback.data)
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 3
    text = """🟣 <b>Вопрос 3 из 20</b>

Какие проблемы вы обнаружили у себя при прохождении скрининга и тестов?
(выберите все подходящие варианты)"""

    await state.update_data(nq3_selected=[])
    keyboard = get_q3_problems_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.q3_problems)

@router.callback_query(F.data.startswith("nq3_"), StateFilter(FollowUpSurveyStates.q3_problems))
async def handle_q3_problems(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 3 (проблемы при скрининге)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('nq3_selected', [])

    if callback.data == "nq3_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        survey_data = data.get('followup_survey_data', {})
        survey_data['screening_problems'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Переход к вопросу 4
        text = """🟣 <b>Вопрос 4 из 20</b>

Как вы оцениваете свой сердечно-сосудистый риск после получения результатов?
(выберите 1 вариант)"""

        keyboard = get_q4_risk_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(FollowUpSurveyStates.q4_risk)
        return

    option_map = {
        "nq3_family_history": "Семейный анамнез ранних ССЗ",
        "nq3_nutrition": "Несбалансированное питание",
        "nq3_smoking": "Курение",
        "nq3_alcohol": "Алкоголь",
        "nq3_low_activity": "Низкий уровень физ. активности",
        "nq3_stress": "Высокий уровень стресса",
        "nq3_sleep": "Нарушение сна",
        "nq3_weight": "Избыточный вес / ожирение",
        "nq3_bp": "Повышение артериального давления",
        "nq3_lipids": "Повышение липидов",
        "nq3_glucose": "Повышение глюкозы",
        "nq3_nothing": "Ничего не выявил(а)"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)

        await state.update_data(nq3_selected=selected)
        keyboard = get_q3_problems_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("nq4_"), StateFilter(FollowUpSurveyStates.q4_risk))
async def handle_q4_risk(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 4 (оценка СС риска)"""
    await safe_answer_callback(callback)

    answer_map = {
        "nq4_low_moderate": "Низкий / умеренный",
        "nq4_high": "Высокий",
        "nq4_very_high": "Очень высокий"
    }

    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['cv_risk_assessment'] = answer_map.get(callback.data, callback.data)
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 5
    text = """🟣 <b>Вопрос 5 из 20</b>

Какие действия вы планируете для модификации образа жизни?
(выберите все подходящие варианты)"""

    await state.update_data(nq5_selected=[])
    keyboard = get_q5_plans_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.q5_plans)

@router.callback_query(F.data.startswith("nq5_"), StateFilter(FollowUpSurveyStates.q5_plans))
async def handle_q5_plans(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 5 (планируемые действия)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('nq5_selected', [])

    if callback.data == "nq5_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        survey_data = data.get('followup_survey_data', {})
        survey_data['lifestyle_plans'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Переход к вопросу 6
        text = """🟣 <b>Вопрос 6 из 20</b>

Планируете ли вы обратиться к врачу?
(выберите 1 вариант)"""

        keyboard = get_q6_doctor_plan_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(FollowUpSurveyStates.q6_doctor_plan)
        return

    option_map = {
        "nq5_quit_smoking": "Отказаться от курения",
        "nq5_limit_alcohol": "Ограничить / исключить алкоголь",
        "nq5_fix_nutrition": "Скорректировать питание",
        "nq5_increase_activity": "Увеличить уровень физ. активности",
        "nq5_fix_sleep": "Решить проблемы со сном",
        "nq5_reduce_stress": "Снизить уровень стресса",
        "nq5_nothing": "Пока не планирую ничего делать"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)

        await state.update_data(nq5_selected=selected)
        keyboard = get_q5_plans_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("nq6_"), StateFilter(FollowUpSurveyStates.q6_doctor_plan))
async def handle_q6_doctor_plan(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 6 (планы обращения к врачу)"""
    await safe_answer_callback(callback)

    answer_map = {
        "nq6_yes": "Да",
        "nq6_no": "Нет",
        "nq6_undecided": "Пока не решил(а)"
    }

    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['doctor_plan'] = answer_map.get(callback.data, callback.data)
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 7
    text = """🟣 <b>Вопрос 7 из 20</b>

Какое влияние вебинар оказал на вашу мотивацию заниматься профилактикой ССЗ?
(выберите 1 вариант)"""

    keyboard = get_q7_webinar_influence_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.q7_webinar_influence)

@router.callback_query(F.data.startswith("nq7_"), StateFilter(FollowUpSurveyStates.q7_webinar_influence))
async def handle_q7_webinar_influence(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 7 (влияние вебинара) и переход к ТОЧКА 3"""
    await safe_answer_callback(callback)

    answer_map = {
        "nq7_decisive": "Решающее влияние — до него даже не задумывался(ась)",
        "nq7_significant": "Существенное влияние — помог систематизировать знания",
        "nq7_minor": "Незначительное влияние — многое уже было известно",
        "nq7_none": "Без влияния — не изменил моего отношения"
    }

    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['webinar_influence'] = answer_map.get(callback.data, callback.data)
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 8 (начало ТОЧКА 3) - обращение к врачу
    text = """🟣 <b>Вопрос 8 из 20</b>

После вебинара обращались ли вы к врачу для оценки сердечно-сосудистых рисков?
(выберите 1 вариант)"""

    keyboard = get_question_1_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_1)

# ============================================================================
# ОБРАБОТЧИКИ ВОПРОСОВ ТОЧКА 3 (Вопросы 8-20, бывшие 1-13)
# ============================================================================

@router.callback_query(F.data.startswith("fq1_"), StateFilter(FollowUpSurveyStates.question_1))
async def handle_question_1(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 1"""
    await safe_answer_callback(callback)

    answer_map = {
        "fq1_yes_after_webinar": "Да, специально пошёл(ла) к врачу после вебинара",
        "fq1_yes_discussed": "Да, я и раньше наблюдалась, просто обсудил(а) эту тему на приёме",
        "fq1_no_no_risks": "Нет, не обращался(ась), считаю, что у меня нет серьёзных рисков",
        "fq1_no_planning": "Нет, пока не дошёл(ла), но планирую"
    }

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['doctor_visit'] = answer_map[callback.data]
    await state.update_data(followup_survey_data=survey_data)

    # Если обращались к врачу, переходим к вопросу 2, иначе пропускаем к вопросу 7
    if callback.data in ["fq1_yes_after_webinar", "fq1_yes_discussed"]:
        await state.update_data(visited_doctor=True)
        text = """🟣 <b>Вопрос 2</b>
К какому врачу вы обращались в первую очередь?
(можно выбрать до 2 вариантов)"""

        await state.update_data(fq2_selected=[])
        keyboard = get_question_2_keyboard([])
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(FollowUpSurveyStates.question_2)
    else:
        await state.update_data(visited_doctor=False)
        # Переходим сразу к вопросу 7
        await jump_to_question_7(callback.message, state)

async def jump_to_question_7(message: Message, state: FSMContext):
    """Переход к вопросу 7 (если не обращались к врачу)"""
    text = """🟣 <b>Вопрос 7</b>
После вебинара вы предпринимали какие-то шаги для снижения сердечно-сосудистых рисков?
(выберите все подходящие варианты)"""

    await state.update_data(fq7_selected=[])
    keyboard = get_question_7_keyboard([])
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_7)

@router.callback_query(F.data.startswith("fq2_"), StateFilter(FollowUpSurveyStates.question_2))
async def handle_question_2(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 2 (мультивыбор, до 2 вариантов)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('fq2_selected', [])

    if callback.data == "fq2_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        # Сохраняем ответ
        survey_data = data.get('followup_survey_data', {})
        survey_data['doctor_type'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Переход к вопросу 3
        text = """🟣 <b>Вопрос 3</b>
Если после вебинара вы обращались к врачу, как вы оцениваете его отношение к вашим знаниям и вопросам (с учётом того, что вы уже были информированы)?
(выберите 1 вариант)"""

        keyboard = get_question_3_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(FollowUpSurveyStates.question_3)
        return

    # Обработка выбора вариантов (максимум 2)
    option_map = {
        "fq2_cardiologist": "Кардиолог",
        "fq2_therapist": "Терапевт / врач общей практики",
        "fq2_other": "Другой специалист"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            if len(selected) >= 2:
                await safe_answer_callback(callback, "Можно выбрать максимум 2 варианта", show_alert=True)
                return
            selected.append(option)

        await state.update_data(fq2_selected=selected)
        keyboard = get_question_2_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("fq3_"), StateFilter(FollowUpSurveyStates.question_3))
async def handle_question_3(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 3"""
    await safe_answer_callback(callback)

    answer_map = {
        "fq3_supportive": "Врач поддержал мой интерес к профилактике, всё подробно объяснил(а)",
        "fq3_neutral": "Врач отнесся нейтрально, без особого интереса",
        "fq3_skeptical": "Врач отнесся скептически / пренебрежительно",
        "fq3_no_visit": "Не обращался(ась) к врачу после вебинара"
    }

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['doctor_attitude'] = answer_map[callback.data]
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 4
    text = """🟣 <b>Вопрос 4</b>
Что было сделано на приёме?
(выберите все подходящие варианты)"""

    await state.update_data(fq4_selected=[])
    keyboard = get_question_4_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_4)

@router.callback_query(F.data.startswith("fq4_"), StateFilter(FollowUpSurveyStates.question_4))
async def handle_question_4(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 4 (мультивыбор)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('fq4_selected', [])

    if callback.data == "fq4_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        # Проверяем, выбрали ли "Другое"
        has_other = "Другое" in selected

        # Сохраняем ответ
        survey_data = data.get('followup_survey_data', {})
        survey_data['visit_actions'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Если выбрали "Другое", запрашиваем текст
        if has_other:
            text = """✍️ Вы выбрали "Другое". Пожалуйста, напишите что именно было сделано на приёме:"""
            await safe_edit_message(callback.message, text)
            await state.set_state(FollowUpSurveyStates.question_4)  # Остаемся в том же состоянии
            await state.update_data(waiting_for_fq4_other=True)
            return

        # Переход к вопросу 5
        await ask_question_5(callback.message, state)
        return

    # Обработка выбора вариантов
    option_map = {
        "fq4_detailed_questioning": "Расспросил о жалобах и факторах риска",
        "fq4_reviewed_tests": "Пересмотрел анализы и обследования",
        "fq4_additional_tests": "Назначил доп. анализы/обследования",
        "fq4_risk_calculation": "Рассчитал сердечно-сосудистый риск",
        "fq4_treatment_change": "Изменил лечение/добавил препараты",
        "fq4_no_worries": "Сказал «всё в норме» без объяснений",
        "fq4_other": "Другое"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)

        await state.update_data(fq4_selected=selected)
        keyboard = get_question_4_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.message(StateFilter(FollowUpSurveyStates.question_4))
async def handle_question_4_other_text(message: Message, state: FSMContext):
    """Обработка текстового ответа для 'Другое' в вопросе 4"""
    data = await state.get_data()
    if not data.get('waiting_for_fq4_other'):
        return

    user_text = message.text.strip()
    if len(user_text) < 3:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 3 символа).")
        return

    # Сохраняем текст
    survey_data = data.get('followup_survey_data', {})
    survey_data['visit_actions_other'] = user_text
    await state.update_data(followup_survey_data=survey_data, waiting_for_fq4_other=False)

    # Удаляем сообщение пользователя
    await message.delete()

    # Переход к вопросу 5
    await ask_question_5(message, state)

async def ask_question_5(message: Message, state: FSMContext):
    """Переход к вопросу 5"""
    text = """🟣 <b>Вопрос 5</b>
Как вы оцениваете, насколько серьёзно врач отнесся к теме профилактики и ваших рисков?
(шкала от 1 до 10, где 1 — совсем несерьёзно, 10 — максимально серьёзно и внимательно)"""

    keyboard = get_question_5_keyboard()
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_5)

@router.callback_query(F.data.startswith("fq5_"), StateFilter(FollowUpSurveyStates.question_5))
async def handle_question_5(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 5 (шкала 1-10)"""
    await safe_answer_callback(callback)

    # Извлекаем оценку из callback_data
    rating = int(callback.data.split("_")[-1])

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['doctor_seriousness'] = rating
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 6
    text = """🟣 <b>Вопрос 6</b>
Насколько вы в целом следуете рекомендациям врача по профилактике и лечению?
(выберите 1 вариант)"""

    keyboard = get_question_6_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_6)

@router.callback_query(F.data.startswith("fq6_"), StateFilter(FollowUpSurveyStates.question_6))
async def handle_question_6(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 6"""
    await safe_answer_callback(callback)

    answer_map = {
        "fq6_fully_compliant": "Полностью соблюдаю",
        "fq6_mostly_compliant": "В основном соблюдаю, но иногда отклоняюсь",
        "fq6_partially": "Следую частично",
        "fq6_non_compliant": "Практически не соблюдаю",
        "fq6_no_recommendations": "Рекомендаций по профилактике почти не было"
    }

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['following_recommendations'] = answer_map[callback.data]
    await state.update_data(followup_survey_data=survey_data)

    # Если не полностью соблюдают, задаем дополнительный вопрос
    if callback.data in ["fq6_mostly_compliant", "fq6_partially", "fq6_non_compliant"]:
        text = """➡️ Если вы не полностью соблюдаете рекомендации, что мешает больше всего?

✍️ Напишите свой ответ текстом:"""

        await safe_edit_message(callback.message, text)
        await state.set_state(FollowUpSurveyStates.question_6_barriers)
    else:
        # Переходим сразу к вопросу 7
        await jump_to_question_7(callback.message, state)

@router.message(StateFilter(FollowUpSurveyStates.question_6_barriers))
async def handle_question_6_barriers(message: Message, state: FSMContext):
    """Обработка текстового ответа о барьерах для вопроса 6"""
    user_text = message.text.strip()

    if len(user_text) < 3:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 3 символа).")
        return

    # Сохраняем текст
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['following_barriers'] = user_text
    await state.update_data(followup_survey_data=survey_data)

    # Удаляем сообщение пользователя
    await message.delete()

    # Переход к вопросу 7
    await jump_to_question_7(message, state)

@router.callback_query(F.data.startswith("fq7_"), StateFilter(FollowUpSurveyStates.question_7))
async def handle_question_7(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 7 (мультивыбор)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('fq7_selected', [])

    if callback.data == "fq7_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        # Проверяем, выбрали ли "Другое"
        has_other = "Другое" in selected

        # Сохраняем ответ
        survey_data = data.get('followup_survey_data', {})
        survey_data['risk_reduction_steps'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Если выбрали "Другое", запрашиваем текст
        if has_other:
            text = """✍️ Вы выбрали "Другое". Пожалуйста, напишите какие именно шаги вы предприняли:"""
            await safe_edit_message(callback.message, text)
            await state.set_state(FollowUpSurveyStates.question_7)  # Остаемся в том же состоянии
            await state.update_data(waiting_for_fq7_other=True)
            return

        # Переход к вопросу 8
        await ask_question_8(callback.message, state)
        return

    # Обработка выбора вариантов
    option_map = {
        "fq7_quit_smoking": "Бросил/сократил курение",
        "fq7_reduce_alcohol": "Сократил употребление алкоголя",
        "fq7_diet_change": "Изменил питание",
        "fq7_increase_activity": "Увеличил физическую активность",
        "fq7_lipids": "Нормализовал холестерин/липиды",
        "fq7_bp": "Нормализовалось давление",
        "fq7_medications": "Начал приём препаратов",
        "fq7_stress_sleep": "Управляю стрессом/наладил сон",
        "fq7_weight_loss": "Работаю над снижением веса",
        "fq7_nothing": "Пока ничего не делал(а)",
        "fq7_other": "Другое"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)

        await state.update_data(fq7_selected=selected)
        keyboard = get_question_7_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.message(StateFilter(FollowUpSurveyStates.question_7))
async def handle_question_7_other_text(message: Message, state: FSMContext):
    """Обработка текстового ответа для 'Другое' в вопросе 7"""
    data = await state.get_data()
    if not data.get('waiting_for_fq7_other'):
        return

    user_text = message.text.strip()
    if len(user_text) < 3:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 3 символа).")
        return

    # Сохраняем текст
    survey_data = data.get('followup_survey_data', {})
    survey_data['risk_reduction_other'] = user_text
    await state.update_data(followup_survey_data=survey_data, waiting_for_fq7_other=False)

    # Удаляем сообщение пользователя
    await message.delete()

    # Переход к вопросу 8
    await ask_question_8(message, state)

async def ask_question_8(message: Message, state: FSMContext):
    """Переход к вопросу 8"""
    text = """🟣 <b>Вопрос 8</b>
Насколько стабильно вам удается удерживать эти изменения в течение последних месяцев?
(шкала от 0 до 10, где 0 — совсем не удаётся, 10 — полностью удаётся придерживаться выбранной стратегии)"""

    keyboard = get_question_8_keyboard()
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_8)

@router.callback_query(F.data.startswith("fq8_"), StateFilter(FollowUpSurveyStates.question_8))
async def handle_question_8(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 8 (шкала 0-10)"""
    await safe_answer_callback(callback)

    # Извлекаем оценку из callback_data
    rating = int(callback.data.split("_")[-1])

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['changes_stability'] = rating
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 9
    text = """🟣 <b>Вопрос 9</b>
С какими трудностями вы столкнулись, когда пытались следовать рекомендациям и менять образ жизни?
(выберите все подходящие варианты)"""

    await state.update_data(fq9_selected=[])
    keyboard = get_question_9_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_9)

@router.callback_query(F.data.startswith("fq9_"), StateFilter(FollowUpSurveyStates.question_9))
async def handle_question_9(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 9 (мультивыбор)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('fq9_selected', [])

    if callback.data == "fq9_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        # Проверяем, выбрали ли "Другое"
        has_other = "Другое" in selected

        # Сохраняем ответ
        survey_data = data.get('followup_survey_data', {})
        survey_data['difficulties'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Если выбрали "Другое", запрашиваем текст
        if has_other:
            text = """✍️ Вы выбрали "Другое". Пожалуйста, напишите с какими трудностями вы столкнулись:"""
            await safe_edit_message(callback.message, text)
            await state.set_state(FollowUpSurveyStates.question_9)  # Остаемся в том же состоянии
            await state.update_data(waiting_for_fq9_other=True)
            return

        # Переход к вопросу 10
        await ask_question_10(callback.message, state)
        return

    # Обработка выбора вариантов
    option_map = {
        "fq9_time": "Недостаток времени",
        "fq9_habits": "Сложно отказаться от привычек",
        "fq9_schedule": "Трудно соблюдать режим (работа/семья)",
        "fq9_finances": "Финансовые ограничения",
        "fq9_support": "Недостаток поддержки окружения",
        "fq9_doctor": "Врач не придал значения рискам",
        "fq9_understanding": "Не понимаю, что конкретно делать",
        "fq9_burnout": "Усталость/выгорание",
        "fq9_no_effect": "Не вижу эффекта от изменений",
        "fq9_other": "Другое"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)

        await state.update_data(fq9_selected=selected)
        keyboard = get_question_9_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.message(StateFilter(FollowUpSurveyStates.question_9))
async def handle_question_9_other_text(message: Message, state: FSMContext):
    """Обработка текстового ответа для 'Другое' в вопросе 9"""
    data = await state.get_data()
    if not data.get('waiting_for_fq9_other'):
        return

    user_text = message.text.strip()
    if len(user_text) < 3:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 3 символа).")
        return

    # Сохраняем текст
    survey_data = data.get('followup_survey_data', {})
    survey_data['difficulties_other'] = user_text
    await state.update_data(followup_survey_data=survey_data, waiting_for_fq9_other=False)

    # Удаляем сообщение пользователя
    await message.delete()

    # Переход к вопросу 10
    await ask_question_10(message, state)

async def ask_question_10(message: Message, state: FSMContext):
    """Переход к вопросу 10"""
    text = """🟣 <b>Вопрос 10</b>
Изменилось ли ваше отношение к профилактике сердечно-сосудистых заболеваний за прошедшие месяцы после вебинара?
(выберите 1 вариант)"""

    keyboard = get_question_10_keyboard()
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_10)

@router.callback_query(F.data.startswith("fq10_"), StateFilter(FollowUpSurveyStates.question_10))
async def handle_question_10(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 10"""
    await safe_answer_callback(callback)

    answer_map = {
        "fq10_serious_acting": "Стал(а) относиться серьезнее и уже действую",
        "fq10_serious_difficult": "Стал(а) относиться серьёзнее, но действовать пока трудно",
        "fq10_no_change": "В целом отношение не изменилось",
        "fq10_calmer": "Стал(а) спокойнее, потому что лучше понимаю свою ситуацию"
    }

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['prevention_attitude_change'] = answer_map[callback.data]
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 11
    text = """🟣 <b>Вопрос 11</b>
Насколько уверенно вы сейчас чувствуете себя в понимании того, что именно нужно делать, чтобы снижать свой сердечно-сосудистый риск?
(шкала от 0 до 10, где 0 — совсем не понимаю, 10 — всё ясно и понятно)"""

    keyboard = get_question_11_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_11)

@router.callback_query(F.data.startswith("fq11_"), StateFilter(FollowUpSurveyStates.question_11))
async def handle_question_11(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 11 (шкала 0-10)"""
    await safe_answer_callback(callback)

    # Извлекаем оценку из callback_data
    rating = int(callback.data.split("_")[-1])

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['understanding_confidence'] = rating
    await state.update_data(followup_survey_data=survey_data)

    # Переход к вопросу 12
    text = """🟣 <b>Вопрос 12</b>
Чувствуете ли вы потребность в дополнительной информации или поддержке по теме сердца и сосудов?
(выберите все подходящие варианты)"""

    await state.update_data(fq12_selected=[])
    keyboard = get_question_12_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_12)

@router.callback_query(F.data.startswith("fq12_"), StateFilter(FollowUpSurveyStates.question_12))
async def handle_question_12(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 12 (мультивыбор)"""
    await safe_answer_callback(callback)

    data = await state.get_data()
    selected = data.get('fq12_selected', [])

    if callback.data == "fq12_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return

        # Проверяем, выбрали ли "Другое"
        has_other = "Другое" in selected

        # Сохраняем ответ
        survey_data = data.get('followup_survey_data', {})
        survey_data['additional_info_need'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(followup_survey_data=survey_data)

        # Если выбрали "Другое", запрашиваем текст
        if has_other:
            text = """✍️ Вы выбрали "Другое". Пожалуйста, напишите какая информация вам нужна:"""
            await safe_edit_message(callback.message, text)
            await state.set_state(FollowUpSurveyStates.question_12)  # Остаемся в том же состоянии
            await state.update_data(waiting_for_fq12_other=True)
            return

        # Переход к вопросу 13 (последний)
        await ask_question_13(callback.message, state)
        return

    # Обработка выбора вариантов
    option_map = {
        "fq12_tests": "Да, про анализы и обследования",
        "fq12_bp": "Да, про давление и гипертензию",
        "fq12_lipids": "Да, про холестерин, липиды, статины",
        "fq12_nutrition": "Да, про питание и вес",
        "fq12_sleep": "Да, про сон и режим дня",
        "fq12_motivation": "Да, про дисциплину и мотивацию",
        "fq12_activity": "Да, про физ. активность и нагрузки",
        "fq12_women_health": "Да, про женское здоровье/менопаузу",
        "fq12_doctor_interaction": "Да, про общение с врачами",
        "fq12_no": "Нет, сейчас информации достаточно",
        "fq12_other": "Другое"
    }

    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)

        await state.update_data(fq12_selected=selected)
        keyboard = get_question_12_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.message(StateFilter(FollowUpSurveyStates.question_12))
async def handle_question_12_other_text(message: Message, state: FSMContext):
    """Обработка текстового ответа для 'Другое' в вопросе 12"""
    data = await state.get_data()
    if not data.get('waiting_for_fq12_other'):
        return

    user_text = message.text.strip()
    if len(user_text) < 3:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 3 символа).")
        return

    # Сохраняем текст
    survey_data = data.get('followup_survey_data', {})
    survey_data['additional_info_other'] = user_text
    await state.update_data(followup_survey_data=survey_data, waiting_for_fq12_other=False)

    # Удаляем сообщение пользователя
    await message.delete()

    # Переход к вопросу 13
    await ask_question_13(message, state)

async def ask_question_13(message: Message, state: FSMContext):
    """Переход к вопросу 13 (последний)"""
    text = """🟣 <b>Вопрос 13 (последний)</b>
Что для вас оказалось самым главным изменением после вебинара?

✍️ Напишите свой ответ текстом:"""

    await safe_edit_message(message, text)
    await state.set_state(FollowUpSurveyStates.question_13)

@router.message(StateFilter(FollowUpSurveyStates.question_13))
async def handle_question_13(message: Message, state: FSMContext):
    """Обработка ответа на вопрос 13 (текстовый) и завершение опроса"""
    chat_id = message.chat.id
    user_text = message.text.strip()

    if len(user_text) < 5:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 5 символов).")
        return

    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('followup_survey_data', {})
    survey_data['main_change'] = user_text

    # Сохраняем весь опрос в БД
    success = await save_followup_survey(chat_id, survey_data)

    if not success:
        await message.answer("❌ Произошла ошибка при сохранении опроса. Пожалуйста, обратитесь к администратору.")
        return

    # Удаляем сообщение пользователя
    await message.delete()

    # Отправляем благодарность и бонус
    await send_followup_bonus(message, state)

# ============================================================================
# СОХРАНЕНИЕ ОПРОСА И ОТПРАВКА БОНУСА
# ============================================================================

async def save_followup_survey(chat_id: int, survey_data: Dict) -> bool:
    """Сохранение опроса ТОЧКА 3 в БД"""
    def _save():
        db = get_db_sync()
        try:
            # Проверяем существующий опрос
            existing_survey = db.query(FollowUpSurvey).filter(FollowUpSurvey.telegram_id == chat_id).first()

            if existing_survey:
                # Обновляем существующий
                for key, value in survey_data.items():
                    if hasattr(existing_survey, key):
                        setattr(existing_survey, key, value)
                existing_survey.completed_at = datetime.now()
                logger.info(f"Обновлен существующий опрос ТОЧКА 3 для пользователя {chat_id}")
            else:
                # Создаем новый
                new_survey = FollowUpSurvey(
                    telegram_id=chat_id,
                    **survey_data,
                    completed_at=datetime.now()
                )
                db.add(new_survey)
                logger.info(f"Создан новый опрос ТОЧКА 3 для пользователя {chat_id}")

            # Обновляем статус в FollowUpStatus
            status = db.query(FollowUpStatus).filter(FollowUpStatus.telegram_id == chat_id).first()
            if status:
                status.survey_completed = True
                status.survey_completed_at = datetime.now()
            else:
                status = FollowUpStatus(
                    telegram_id=chat_id,
                    survey_completed=True,
                    survey_completed_at=datetime.now()
                )
                db.add(status)

            db.commit()
            return True

        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка сохранения опроса ТОЧКА 3: {e}")
            return False
        finally:
            db.close()

    # Выполняем в отдельном потоке
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)

async def send_followup_bonus(message: Message, state: FSMContext):
    """Отправка бонусной методички после завершения опроса ТОЧКА 2 и 3"""
    chat_id = message.chat.id

    # Отправляем благодарность
    text = """Спасибо, что нашли время ответить на вопросы 💚

Ваши ответы — это не формальность. Они помогают нам понимать, что реально меняется в вашей жизни после вебинара, и делать наши программы ещё полезнее и точнее для вас.

🎁 Спасибо за участие!
В знак благодарности — обещанный бонус:

<b>✅ «Руководство для женщин 40+: как защитить сердце в перименопаузе и менопаузе»</b>

Берегите сердце, а за доказательной профилактикой — приходите к нам. Мы рядом 🫀"""

    await message.answer(text, parse_mode="HTML")

    # Отправляем файл методички
    try:
        import os

        # Приоритет - новый файл, fallback на старый
        bonus_files = [
            ("materials/Руководство_для_женщин_40+.pdf", "Руководство_для_женщин_40+.pdf", "📄 «Руководство для женщин 40+: как защитить сердце в перименопаузе и менопаузе»"),
            ("materials/Методичка ЗОЖ.pdf", "Методичка_ЗОЖ.pdf", "📄 Методичка «Правила ЗОЖ как основа профилактики и лечения сердечно-сосудистых заболеваний»")
        ]

        sent = False
        for file_path, filename, caption in bonus_files:
            if os.path.exists(file_path):
                document = FSInputFile(file_path, filename=filename)
                await message.answer_document(document, caption=caption)
                logger.info(f"Бонус '{filename}' отправлен пользователю {chat_id}")
                sent = True
                break

        if not sent:
            logger.error("Файлы бонуса не найдены")
            await message.answer("❌ Извините, произошла ошибка при отправке файла. Обратитесь к администратору.")
    except Exception as e:
        logger.error(f"Ошибка отправки бонуса: {e}")
        await message.answer("❌ Извините, произошла ошибка при отправке файла. Обратитесь к администратору.")

    # Отмечаем отправку бонуса в БД
    def _mark_bonus():
        db = get_db_sync()
        try:
            status = db.query(FollowUpStatus).filter(FollowUpStatus.telegram_id == chat_id).first()
            if status:
                status.bonus_sent = True
                status.bonus_sent_at = datetime.now()
            db.commit()
        except Exception as e:
            logger.error(f"Ошибка отметки отправки бонуса: {e}")
        finally:
            db.close()

    await asyncio.get_event_loop().run_in_executor(None, _mark_bonus)

    await log_user_interaction(chat_id, "followup_survey_completed_bonus_sent")

    # Очищаем состояние
    await state.clear()
