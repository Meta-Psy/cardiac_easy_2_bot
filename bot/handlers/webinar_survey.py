"""
Модуль для опроса ТОЧКА 2 (9 вопросов после просмотра вебинара)
Реализует опрос согласно ТЗ в файле new.txt
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext

from .base import UserStates, safe_edit_message, safe_answer_callback, log_user_interaction
from database import get_db_sync, User, WebinarSurvey

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# СОСТОЯНИЯ ДЛЯ ОПРОСА ТОЧКА 2
# ============================================================================

# Добавим новые состояния в base.py через импорт
from aiogram.fsm.state import State, StatesGroup

class WebinarSurveyStates(StatesGroup):
    question_1 = State()  # Понятность кардиочекапа
    question_2 = State()  # Изменение отношения к профилактике  
    question_3 = State()  # Проблемы при скрининге (мультивыбор)
    question_4 = State()  # Оценка сердечно-сосудистого риска
    question_5 = State()  # Планируемые действия (мультивыбор)
    question_6 = State()  # Планы обращения к врачу
    question_7 = State()  # Мотивация для кардиочекапа (мультивыбор)
    question_8 = State()  # Влияние вебинара на мотивацию
    question_9 = State()  # Самое полезное из вебинара (текст)

# ============================================================================
# КЛАВИАТУРЫ ДЛЯ ОПРОСА
# ============================================================================

def get_question_1_keyboard():
    """Вопрос 1: Понятность кардиочекапа"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Я полностью понял(а), что это и зачем", callback_data="q1_fully_understood")],
        [InlineKeyboardButton(text="Стало немного понятнее, чем раньше", callback_data="q1_somewhat_clearer")],
        [InlineKeyboardButton(text="Всё ещё не понимаю", callback_data="q1_still_confused")]
    ])
    return keyboard

def get_question_2_keyboard():
    """Вопрос 2: Изменение отношения к профилактике"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Не изменилось, я и до вебинара планировал(а) пройти кардиочекап", callback_data="q2_already_planned")],
        [InlineKeyboardButton(text="Не изменилось, по-прежнему не вижу смысла", callback_data="q2_no_change_skeptical")],
        [InlineKeyboardButton(text="Стал(а) относиться серьёзнее, но пока не готов(а) действовать", callback_data="q2_more_serious_not_ready")],
        [InlineKeyboardButton(text="Появилось желание пройти обследование", callback_data="q2_want_examination")],
        [InlineKeyboardButton(text="Уже принял(а) решение действовать и начать обследование", callback_data="q2_decided_to_act")]
    ])
    return keyboard

def get_question_3_keyboard(selected: List[str]):
    """Вопрос 3: Проблемы при скрининге (мультивыбор)"""
    options = [
        ("Семейный анамнез ранних ССЗ", "q3_family_history"),
        ("Несбалансированное питание", "q3_poor_nutrition"),
        ("Курение", "q3_smoking"),
        ("Алкоголь", "q3_alcohol"),
        ("Низкий уровень физ. активности", "q3_low_activity"),
        ("Высокий уровень стресса", "q3_high_stress"),
        ("Нарушение сна", "q3_sleep_problems"),
        ("Избыточный вес / ожирение / абдоминальное ожирение", "q3_weight_problems"),
        ("Повышение артериального давления", "q3_high_bp"),
        ("Повышение липидов", "q3_high_lipids"),
        ("Повышение глюкозы", "q3_high_glucose"),
        ("Ничего не выявил(а)", "q3_nothing_found")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="q3_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_4_keyboard():
    """Вопрос 4: Оценка сердечно-сосудистого риска"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Низкий / умеренный", callback_data="q4_low_moderate")],
        [InlineKeyboardButton(text="Высокий", callback_data="q4_high")],
        [InlineKeyboardButton(text="Очень высокий", callback_data="q4_very_high")]
    ])
    return keyboard

def get_question_5_keyboard(selected: List[str]):
    """Вопрос 5: Планируемые действия (мультивыбор)"""
    options = [
        ("Отказаться от курения", "q5_quit_smoking"),
        ("Ограничить / исключить алкоголь", "q5_limit_alcohol"),
        ("Скорректировать питание", "q5_correct_nutrition"),
        ("Увеличить уровень физ. активности", "q5_increase_activity"),
        ("Решить проблемы со сном", "q5_fix_sleep"),
        ("Снизить уровень стресса", "q5_reduce_stress"),
        ("Пока не планирую ничего делать", "q5_no_plans")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="q5_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_6_keyboard():
    """Вопрос 6: Планы обращения к врачу"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Да", callback_data="q6_yes")],
        [InlineKeyboardButton(text="Нет", callback_data="q6_no")],
        [InlineKeyboardButton(text="Пока не решил(а)", callback_data="q6_undecided")]
    ])
    return keyboard

def get_question_7_keyboard(selected: List[str]):
    """Вопрос 7: Мотивация для кардиочекапа (мультивыбор)"""
    options = [
        ("Понимание реального риска", "q7_understanding_risk"),
        ("Эмоциональный отклик на информацию", "q7_emotional_response"),
        ("Узнал(а) о простых и доступных шагах", "q7_simple_steps"),
        ("Убедили примеры и статистика", "q7_examples_stats"),
        ("Ничего — просто послушал(а), без отклика", "q7_no_response")
    ]
    
    buttons = []
    for text, callback_data in options:
        prefix = "✅ " if text in selected else "☐ "
        buttons.append([InlineKeyboardButton(text=prefix + text, callback_data=callback_data)])
    
    # Кнопка завершения
    buttons.append([InlineKeyboardButton(text="Готово", callback_data="q7_done")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

def get_question_8_keyboard():
    """Вопрос 8: Влияние вебинара на мотивацию"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Решающее влияние — до него даже не задумывался(ась), а теперь точно знаю, что нужно действовать", callback_data="q8_decisive")],
        [InlineKeyboardButton(text="Существенное влияние — помог систематизировать знания и подтолкнул к конкретным шагам", callback_data="q8_significant")],
        [InlineKeyboardButton(text="Незначительное влияние — многое уже было известно, но некоторые моменты стали понятнее", callback_data="q8_minor")],
        [InlineKeyboardButton(text="Без влияния — не изменил моего отношения или планов", callback_data="q8_no_influence")]
    ])
    return keyboard

# ============================================================================
# НАЧАЛО ОПРОСА
# ============================================================================

async def start_webinar_survey(message: Message, state: FSMContext):
    """Запуск опроса ТОЧКА 2"""
    chat_id = message.chat.id
    await log_user_interaction(chat_id, "webinar_survey_started")
    
    # Инициализируем данные опроса
    await state.update_data(webinar_survey_data={})
    
    text = """🟣 <b>Вопрос 1</b>
Насколько для вас стало понятнее, что включает в себя кардиочекап?
(выберите 1 вариант)"""
    
    keyboard = get_question_1_keyboard()
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(WebinarSurveyStates.question_1)

# ============================================================================
# ОБРАБОТЧИКИ ВОПРОСОВ
# ============================================================================

@router.callback_query(F.data.startswith("q1_"), StateFilter(WebinarSurveyStates.question_1))
async def handle_question_1(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 1"""
    await safe_answer_callback(callback)
    
    answer_map = {
        "q1_fully_understood": "Я полностью понял(а), что это и зачем",
        "q1_somewhat_clearer": "Стало немного понятнее, чем раньше",
        "q1_still_confused": "Всё ещё не понимаю"
    }
    
    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('webinar_survey_data', {})
    survey_data['understanding_cardio_checkup'] = answer_map[callback.data]
    await state.update_data(webinar_survey_data=survey_data)
    
    # Переход к вопросу 2
    text = """🟣 <b>Вопрос 2</b>
Как изменилось ваше отношение к профилактическому обследованию сердца?"""
    
    keyboard = get_question_2_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(WebinarSurveyStates.question_2)

@router.callback_query(F.data.startswith("q2_"), StateFilter(WebinarSurveyStates.question_2))
async def handle_question_2(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 2"""
    await safe_answer_callback(callback)
    
    answer_map = {
        "q2_already_planned": "Не изменилось, я и до вебинара планировал(а) пройти кардиочекап",
        "q2_no_change_skeptical": "Не изменилось, по-прежнему не вижу смысла",
        "q2_more_serious_not_ready": "Стал(а) относиться серьёзнее, но пока не готов(а) действовать",
        "q2_want_examination": "Появилось желание пройти обследование",
        "q2_decided_to_act": "Уже принял(а) решение действовать и начать обследование"
    }
    
    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('webinar_survey_data', {})
    survey_data['attitude_change'] = answer_map[callback.data]
    await state.update_data(webinar_survey_data=survey_data)
    
    # Переход к вопросу 3
    text = """🟣 <b>Вопрос 3</b>
Какие проблемы выявлены вами при скрининге?
(выберите все подходящие варианты)"""
    
    await state.update_data(q3_selected=[])
    keyboard = get_question_3_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(WebinarSurveyStates.question_3)

@router.callback_query(F.data.startswith("q3_"), StateFilter(WebinarSurveyStates.question_3))
async def handle_question_3(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 3 (мультивыбор)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('q3_selected', [])
    
    if callback.data == "q3_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        # Сохраняем ответ
        survey_data = data.get('webinar_survey_data', {})
        survey_data['screening_problems'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(webinar_survey_data=survey_data)
        
        # Переход к вопросу 4
        text = """🟣 <b>Вопрос 4</b>
Как вы сейчас оцениваете свой сердечно-сосудистый риск?"""
        
        keyboard = get_question_4_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(WebinarSurveyStates.question_4)
        return
    
    # Обработка выбора вариантов
    option_map = {
        "q3_family_history": "Семейный анамнез ранних ССЗ",
        "q3_poor_nutrition": "Несбалансированное питание",
        "q3_smoking": "Курение",
        "q3_alcohol": "Алкоголь",
        "q3_low_activity": "Низкий уровень физ. активности",
        "q3_high_stress": "Высокий уровень стресса",
        "q3_sleep_problems": "Нарушение сна",
        "q3_weight_problems": "Избыточный вес / ожирение / абдоминальное ожирение",
        "q3_high_bp": "Повышение артериального давления",
        "q3_high_lipids": "Повышение липидов",
        "q3_high_glucose": "Повышение глюкозы",
        "q3_nothing_found": "Ничего не выявил(а)"
    }
    
    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)
        
        await state.update_data(q3_selected=selected)
        keyboard = get_question_3_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("q4_"), StateFilter(WebinarSurveyStates.question_4))
async def handle_question_4(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 4"""
    await safe_answer_callback(callback)
    
    answer_map = {
        "q4_low_moderate": "Низкий / умеренный",
        "q4_high": "Высокий",
        "q4_very_high": "Очень высокий"
    }
    
    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('webinar_survey_data', {})
    survey_data['cv_risk_assessment'] = answer_map[callback.data]
    await state.update_data(webinar_survey_data=survey_data)
    
    # Переход к вопросу 5
    text = """🟣 <b>Вопрос 5</b>
Какие действия по модификации образа жизни вы планируете предпринять в первую очередь? 
(выберите все подходящие варианты)"""
    
    await state.update_data(q5_selected=[])
    keyboard = get_question_5_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(WebinarSurveyStates.question_5)

@router.callback_query(F.data.startswith("q5_"), StateFilter(WebinarSurveyStates.question_5))
async def handle_question_5(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 5 (мультивыбор)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('q5_selected', [])
    
    if callback.data == "q5_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        # Сохраняем ответ
        survey_data = data.get('webinar_survey_data', {})
        survey_data['lifestyle_actions'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(webinar_survey_data=survey_data)
        
        # Переход к вопросу 6
        text = """🟣 <b>Вопрос 6</b>
Планируете ли вы обратиться к врачу для дообследования или коррекции рисков?"""
        
        keyboard = get_question_6_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(WebinarSurveyStates.question_6)
        return
    
    # Обработка выбора вариантов
    option_map = {
        "q5_quit_smoking": "Отказаться от курения",
        "q5_limit_alcohol": "Ограничить / исключить алкоголь",
        "q5_correct_nutrition": "Скорректировать питание",
        "q5_increase_activity": "Увеличить уровень физ. активности",
        "q5_fix_sleep": "Решить проблемы со сном",
        "q5_reduce_stress": "Снизить уровень стресса",
        "q5_no_plans": "Пока не планирую ничего делать"
    }
    
    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)
        
        await state.update_data(q5_selected=selected)
        keyboard = get_question_5_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("q6_"), StateFilter(WebinarSurveyStates.question_6))
async def handle_question_6(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 6"""
    await safe_answer_callback(callback)
    
    answer_map = {
        "q6_yes": "Да",
        "q6_no": "Нет",
        "q6_undecided": "Пока не решил(а)"
    }
    
    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('webinar_survey_data', {})
    survey_data['doctor_consultation_plans'] = answer_map[callback.data]
    await state.update_data(webinar_survey_data=survey_data)
    
    # Переход к вопросу 7
    text = """🟣 <b>Вопрос 7</b>
Что стало для вас главным мотивом для прохождения кардиочекапа после вебинара? 
(выберите все подходящие варианты)"""
    
    await state.update_data(q7_selected=[])
    keyboard = get_question_7_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(WebinarSurveyStates.question_7)

@router.callback_query(F.data.startswith("q7_"), StateFilter(WebinarSurveyStates.question_7))
async def handle_question_7(callback: CallbackQuery, state: FSMContext):
    """Обработка вопроса 7 (мультивыбор)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('q7_selected', [])
    
    if callback.data == "q7_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        # Сохраняем ответ
        survey_data = data.get('webinar_survey_data', {})
        survey_data['checkup_motivation'] = json.dumps(selected, ensure_ascii=False)
        await state.update_data(webinar_survey_data=survey_data)
        
        # Переход к вопросу 8
        text = """🟣 <b>Вопрос 8</b>
Какое влияние вебинар оказал на ваше решение или мотивацию заняться профилактическим обследованием сердечно-сосудистых заболеваний?"""
        
        keyboard = get_question_8_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(WebinarSurveyStates.question_8)
        return
    
    # Обработка выбора вариантов
    option_map = {
        "q7_understanding_risk": "Понимание реального риска",
        "q7_emotional_response": "Эмоциональный отклик на информацию",
        "q7_simple_steps": "Узнал(а) о простых и доступных шагах",
        "q7_examples_stats": "Убедили примеры и статистика",
        "q7_no_response": "Ничего — просто послушал(а), без отклика"
    }
    
    option = option_map.get(callback.data)
    if option:
        if option in selected:
            selected.remove(option)
        else:
            selected.append(option)
        
        await state.update_data(q7_selected=selected)
        keyboard = get_question_7_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("q8_"), StateFilter(WebinarSurveyStates.question_8))
async def handle_question_8(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос 8"""
    await safe_answer_callback(callback)
    
    answer_map = {
        "q8_decisive": "Решающее влияние — до него даже не задумывался(ась), а теперь точно знаю, что нужно действовать",
        "q8_significant": "Существенное влияние — помог систематизировать знания и подтолкнул к конкретным шагам",
        "q8_minor": "Незначительное влияние — многое уже было известно, но некоторые моменты стали понятнее",
        "q8_no_influence": "Без влияния — не изменил моего отношения или планов"
    }
    
    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('webinar_survey_data', {})
    survey_data['webinar_influence'] = answer_map[callback.data]
    await state.update_data(webinar_survey_data=survey_data)
    
    # Переход к вопросу 9 (текстовый)
    text = """🟣 <b>Вопрос 9 (последний)</b>
Что из вебинара запомнилось больше всего? Что было самым полезным и ценным лично для вас?

✍️ Напишите свой ответ текстом:"""
    
    await safe_edit_message(callback.message, text)
    await state.set_state(WebinarSurveyStates.question_9)

@router.message(StateFilter(WebinarSurveyStates.question_9))
async def handle_question_9(message: Message, state: FSMContext):
    """Обработка ответа на вопрос 9 (текстовый)"""
    chat_id = message.chat.id
    user_text = message.text.strip()
    
    if len(user_text) < 5:
        await message.answer("Пожалуйста, напишите более развернутый ответ (минимум 5 символов).")
        return
    
    # Сохраняем ответ
    data = await state.get_data()
    survey_data = data.get('webinar_survey_data', {})
    survey_data['most_useful_from_webinar'] = user_text
    
    # Сохраняем весь опрос в БД
    await save_webinar_survey(chat_id, survey_data)
    
    # Отправляем бонус
    await send_bonus_material(message, state)

# ============================================================================
# СОХРАНЕНИЕ ОПРОСА И ОТПРАВКА БОНУСА
# ============================================================================

async def save_webinar_survey(chat_id: int, survey_data: Dict) -> bool:
    """Сохранение опроса ТОЧКА 2 в БД"""
    def _save():
        db = get_db_sync()
        try:
            # Проверяем существующий опрос
            existing_survey = db.query(WebinarSurvey).filter(WebinarSurvey.telegram_id == chat_id).first()
            
            if existing_survey:
                # Обновляем существующий
                for key, value in survey_data.items():
                    if hasattr(existing_survey, key):
                        setattr(existing_survey, key, value)
                existing_survey.completed_at = datetime.now()
                logger.info(f"Обновлен существующий опрос ТОЧКА 2 для пользователя {chat_id}")
            else:
                # Создаем новый
                new_survey = WebinarSurvey(
                    telegram_id=chat_id,
                    **survey_data,
                    completed_at=datetime.now()
                )
                db.add(new_survey)
                logger.info(f"Создан новый опрос ТОЧКА 2 для пользователя {chat_id}")
            
            # Отмечаем пользователя как завершившего опрос вебинара
            user = db.query(User).filter(User.telegram_id == chat_id).first()
            if user:
                user.webinar_survey_completed = True
            
            db.commit()
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка сохранения опроса ТОЧКА 2: {e}")
            return False
        finally:
            db.close()
    
    # Выполняем в отдельном потоке
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)

async def send_bonus_material(message: Message, state: FSMContext):
    """Отправка бонусного материала после завершения опроса"""
    chat_id = message.chat.id
    
    # Удаляем сообщение пользователя
    await message.delete()
    
    # Отправляем бонус
    text = """🎁 <b>Спасибо за участие!</b>

В знак благодарности — обещанный бонус:

✅ Памятка «Тревожные звоночки: как проявляются инфаркт и инсульт у женщин и мужчин — типичные и неожиданные симптомы».

📩 Скачайте по ссылке: https://novikova-diana.ru/bonus-pamyatka"""
    
    await message.answer(text, parse_mode="HTML")
    
    # Отмечаем получение бонуса
    def _mark_bonus():
        db = get_db_sync()
        try:
            user = db.query(User).filter(User.telegram_id == chat_id).first()
            if user:
                user.bonus_received = True
            db.commit()
        except Exception as e:
            logger.error(f"Ошибка отметки получения бонуса: {e}")
        finally:
            db.close()
    
    await asyncio.get_event_loop().run_in_executor(None, _mark_bonus)
    
    await log_user_interaction(chat_id, "webinar_survey_completed_bonus_sent")
    
    # Очищаем состояние
    await state.clear()