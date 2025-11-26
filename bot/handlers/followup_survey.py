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
# СОСТОЯНИЯ ДЛЯ ОПРОСА ТОЧКА 3
# ============================================================================

class FollowUpSurveyStates(StatesGroup):
    question_1 = State()   # Обращение к врачу
    question_2 = State()   # К какому врачу (мультивыбор)
    question_3 = State()   # Отношение врача
    question_4 = State()   # Что было сделано на приеме (мультивыбор)
    question_5 = State()   # Серьезность врача (шкала 1-10)
    question_6 = State()   # Следование рекомендациям
    question_6_barriers = State()  # Что мешает (если не полностью соблюдают)
    question_7 = State()   # Шаги по снижению рисков (мультивыбор)
    question_8 = State()   # Стабильность изменений (шкала 0-10)
    question_9 = State()   # Трудности (мультивыбор)
    question_10 = State()  # Изменение отношения к профилактике
    question_11 = State()  # Уверенность в понимании (шкала 0-10)
    question_12 = State()  # Потребность в дополнительной информации (мультивыбор)
    question_13 = State()  # Главное изменение (текст)

# ============================================================================
# КЛАВИАТУРЫ ДЛЯ ОПРОСА
# ============================================================================

def get_question_1_keyboard():
    """Вопрос 1: Обращение к врачу"""
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
        ("Врач подробно расспросил(а) о жалобах и факторах риска", "fq4_detailed_questioning"),
        ("Пересмотрел(а) имеющиеся анализы и обследования", "fq4_reviewed_tests"),
        ("Назначил(а) дополнительные анализы / обследования", "fq4_additional_tests"),
        ("Рассчитал(а) сердечно-сосудистый риск", "fq4_risk_calculation"),
        ("Изменил(а) лечение / добавил(а) препараты", "fq4_treatment_change"),
        ("Сказал(а), что «всё в норме / не переживайте» без детальных объяснений", "fq4_no_worries"),
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
        ("Бросил(а) курить или сократил(а) количество сигарет", "fq7_quit_smoking"),
        ("Сократил(а) употребление алкоголя", "fq7_reduce_alcohol"),
        ("Изменил(а) питание", "fq7_diet_change"),
        ("Увеличил(а) уровень физической активности", "fq7_increase_activity"),
        ("Нормализовал(а) холестерин/липидный профиль", "fq7_lipids"),
        ("Нормализовалось давление", "fq7_bp"),
        ("Начал(а) приём рекомендованных препаратов", "fq7_medications"),
        ("Старал(ась) лучше управлять стрессом / наладить сон", "fq7_stress_sleep"),
        ("Работаю над снижением массы тела", "fq7_weight_loss"),
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
        ("Трудно соблюдать режим при работе / семье / нагрузке", "fq9_schedule"),
        ("Финансовые ограничения", "fq9_finances"),
        ("Недостаток поддержки со стороны семьи / окружения", "fq9_support"),
        ("Врач не придал значения моим рискам", "fq9_doctor"),
        ("Не до конца понимаю, что именно и в каком объёме нужно делать", "fq9_understanding"),
        ("Психологическая усталость / выгорание / руки опускаются", "fq9_burnout"),
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
        ("Да, хотелось бы более подробнее про анализы и обследования", "fq12_tests"),
        ("Да, про давление и лечение артериальной гипертензии", "fq12_bp"),
        ("Да, про холестерин, липиды и статины", "fq12_lipids"),
        ("Да, про питание и вес", "fq12_nutrition"),
        ("Да, про сон и режим дня", "fq12_sleep"),
        ("Да, про поддержание дисциплины и мотивации в модификации образа жизни", "fq12_motivation"),
        ("Да, про физическую активность и безопасные нагрузки", "fq12_activity"),
        ("Да, про женское здоровье и сердце в пременопаузе/менопаузе", "fq12_women_health"),
        ("Да, про взаимодействие с врачами (как говорить, что спрашивать)", "fq12_doctor_interaction"),
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

    text = """🟣 <b>Вопрос 1</b>
После вебинара обращались ли вы к врачу для оценки сердечно-сосудистых рисков?
(выберите 1 вариант)"""

    keyboard = get_question_1_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(FollowUpSurveyStates.question_1)

# ============================================================================
# ОБРАБОТЧИКИ ВОПРОСОВ
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
        "fq4_detailed_questioning": "Врач подробно расспросил(а) о жалобах и факторах риска",
        "fq4_reviewed_tests": "Пересмотрел(а) имеющиеся анализы и обследования",
        "fq4_additional_tests": "Назначил(а) дополнительные анализы / обследования",
        "fq4_risk_calculation": "Рассчитал(а) сердечно-сосудистый риск",
        "fq4_treatment_change": "Изменил(а) лечение / добавил(а) препараты",
        "fq4_no_worries": "Сказал(а), что «всё в норме / не переживайте» без детальных объяснений",
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
        "fq7_quit_smoking": "Бросил(а) курить или сократил(а) количество сигарет",
        "fq7_reduce_alcohol": "Сократил(а) употребление алкоголя",
        "fq7_diet_change": "Изменил(а) питание",
        "fq7_increase_activity": "Увеличил(а) уровень физической активности",
        "fq7_lipids": "Нормализовал(а) холестерин/липидный профиль",
        "fq7_bp": "Нормализовалось давление",
        "fq7_medications": "Начал(а) приём рекомендованных препаратов",
        "fq7_stress_sleep": "Старал(ась) лучше управлять стрессом / наладить сон",
        "fq7_weight_loss": "Работаю над снижением массы тела",
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
        "fq9_schedule": "Трудно соблюдать режим при работе / семье / нагрузке",
        "fq9_finances": "Финансовые ограничения",
        "fq9_support": "Недостаток поддержки со стороны семьи / окружения",
        "fq9_doctor": "Врач не придал значения моим рискам",
        "fq9_understanding": "Не до конца понимаю, что именно и в каком объёме нужно делать",
        "fq9_burnout": "Психологическая усталость / выгорание / руки опускаются",
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
        "fq12_tests": "Да, хотелось бы более подробнее про анализы и обследования",
        "fq12_bp": "Да, про давление и лечение артериальной гипертензии",
        "fq12_lipids": "Да, про холестерин, липиды и статины",
        "fq12_nutrition": "Да, про питание и вес",
        "fq12_sleep": "Да, про сон и режим дня",
        "fq12_motivation": "Да, про поддержание дисциплины и мотивации в модификации образа жизни",
        "fq12_activity": "Да, про физическую активность и безопасные нагрузки",
        "fq12_women_health": "Да, про женское здоровье и сердце в пременопаузе/менопаузе",
        "fq12_doctor_interaction": "Да, про взаимодействие с врачами (как говорить, что спрашивать)",
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
    """Отправка бонусной методички после завершения опроса ТОЧКА 3"""
    chat_id = message.chat.id

    # Отправляем благодарность
    text = """Спасибо, что нашли время ответить на вопросы 💚

Ваши ответы — это не формальность. Они помогают нам понимать, что реально меняется в вашей жизни после вебинара, и делать наши программы ещё полезнее и точнее для вас.

🎁 Как и обещали, делимся бонусом:

<b>Методичка «Правила ЗОЖ как основа профилактики и лечения сердечно-сосудистых заболеваний (режим • питание • тренировки • гигиена)»</b>

Берегите сердце, а за доказательной профилактикой — приходите к нам. Мы рядом 🫀"""

    await message.answer(text, parse_mode="HTML")

    # Отправляем файл методички
    try:
        import os

        file_path = os.path.join("materials", "Методичка ЗОЖ.pdf")
        if os.path.exists(file_path):
            document = FSInputFile(file_path, filename="Методичка_ЗОЖ.pdf")
            await message.answer_document(
                document,
                caption="📄 Методичка «Правила ЗОЖ как основа профилактики и лечения сердечно-сосудистых заболеваний»"
            )
            logger.info(f"Методичка ЗОЖ отправлена пользователю {chat_id}")
        else:
            logger.error(f"Файл {file_path} не найден")
            await message.answer("❌ Извините, произошла ошибка при отправке файла. Обратитесь к администратору.")
    except Exception as e:
        logger.error(f"Ошибка отправки методички: {e}")
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
