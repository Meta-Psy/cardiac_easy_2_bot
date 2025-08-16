import asyncio
import json
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext

from .base import UserStates, safe_edit_message, safe_answer_callback, log_user_interaction
from ui.keyboards import *
from database import safe_save_user_data, Survey, get_db_sync, User
from datetime import datetime

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

async def save_survey_data(telegram_id: int, survey_data: dict):
    """Сохранение данных опроса в базу данных"""
    def _save():
        db = get_db_sync()
        try:
            # Проверяем существующий опрос
            existing_survey = db.query(Survey).filter(Survey.telegram_id == telegram_id).first()
            
            if existing_survey:
                # Обновляем существующий
                for key, value in survey_data.items():
                    if hasattr(existing_survey, key):
                        # Сериализуем списки в JSON
                        if isinstance(value, list):
                            value = json.dumps(value, ensure_ascii=False)
                        setattr(existing_survey, key, value)
                existing_survey.completed_at = datetime.now()
                logger.info(f"Обновлен существующий опрос для пользователя {telegram_id}")
            else:
                # Создаем новый - фильтруем данные для Survey
                survey_fields = {}
                for key, value in survey_data.items():
                    if hasattr(Survey, key):
                        # Сериализуем списки в JSON
                        if isinstance(value, list):
                            value = json.dumps(value, ensure_ascii=False)
                        survey_fields[key] = value
                
                new_survey = Survey(
                    telegram_id=telegram_id,
                    **survey_fields,
                    completed_at=datetime.now()
                )
                db.add(new_survey)
                logger.info(f"Создан новый опрос для пользователя {telegram_id}")
            
            # Отмечаем пользователя как завершившего опрос
            user = db.query(User).filter(User.telegram_id == telegram_id).first()
            if user:
                user.survey_completed = True
            
            db.commit()
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Ошибка сохранения опроса: {e}")
            return False
        finally:
            db.close()
    
    # Выполняем в отдельном потоке чтобы не блокировать async
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _save)

def emergency_create_user(telegram_id: int):
    """Экстренное создание пользователя если он не существует"""
    db = get_db_sync()
    try:
        # Проверяем существование пользователя
        existing_user = db.query(User).filter(User.telegram_id == telegram_id).first()
        
        if not existing_user:
            # Создаем нового пользователя с минимальными данными
            new_user = User(
                telegram_id=telegram_id,
                name=f"Пользователь_{telegram_id}",
                email=f"user_{telegram_id}@bot.com",
                phone="",
                registration_completed=True,
                created_at=datetime.now()
            )
            db.add(new_user)
            db.commit()
            logger.info(f"Экстренно создан пользователь {telegram_id}")
            return True
        
        return False
        
    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка экстренного создания пользователя: {e}")
        return False
    finally:
        db.close()

# ============================================================================
# ОБРАБОТЧИКИ ОПРОСА
# ============================================================================

@router.message(StateFilter(UserStates.survey_age))
async def handle_age(message: Message, state: FSMContext):
    """Обработка возраста с удалением сообщений"""
    await log_user_interaction(message.from_user.id, "age_entered", message.text)
    
    try:
        age = int(message.text.strip())
        if age < 1 or age > 120:
            await message.answer("Пожалуйста, введите корректный возраст.")
            return
        
        await state.update_data(age=age)
        
        # Удаляем сообщение пользователя и вопрос
        await message.delete()
        # Пытаемся найти и удалить сообщение с вопросом (обычно предыдущее)
        if message.message_id > 1:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id - 1)
            except:
                pass
       
        text = """<b>❓ Вопрос 2</b>

Ваш пол"""
        
        keyboard = get_gender_keyboard()
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(UserStates.survey_gender)
        
    except ValueError:
        await message.answer("Пожалуйста, введите число.")

@router.callback_query(F.data.in_(["gender_female", "gender_male"]), StateFilter(UserStates.survey_gender))
async def handle_gender(callback: CallbackQuery, state: FSMContext):
    """Обработка пола"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "gender_selected", callback.data)
    
    gender = "Женский" if callback.data == "gender_female" else "Мужской"
    await state.update_data(gender=gender)
    
    text = """<b>❓ Вопрос 3</b>
Где вы живёте?
(выберите 1 вариант ответа)

Выберите тип населённого пункта:"""
    
    keyboard = get_location_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_location)

@router.callback_query(F.data.startswith("location_"), StateFilter(UserStates.survey_location))
async def handle_location(callback: CallbackQuery, state: FSMContext):
    """Обработка места жительства"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "location_selected", callback.data)
    
    location_map = {
        "location_big_city": "Город с населением >1 млн",
        "location_medium_city": "Город 500–999 тыс",
        "location_small_city": "Город с населением 100–500 тыс",
        "location_town": "Город до 100 тыс",
        "location_village": "Поселок / сельская местность"
    }
    
    location = location_map[callback.data]
    await state.update_data(location=location)
    
    text = """<b>❓ Вопрос 4</b>
Ваше образование
(выберите 1 вариант ответа)"""
    
    keyboard = get_education_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_education)

@router.callback_query(F.data.startswith("education_"), StateFilter(UserStates.survey_education))
async def handle_education(callback: CallbackQuery, state: FSMContext):
    """Обработка образования"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "education_selected", callback.data)
    
    education_map = {
        "education_secondary": "Среднее общее",
        "education_vocational": "Средне-специальное",
        "education_higher": "Высшее (немедицинское)",
        "education_medical": "Высшее медицинское"
    }
    
    education = education_map[callback.data]
    await state.update_data(education=education)
    
    text = """<b>❓ Вопрос 5</b>
Ваше семейное положение
(выберите 1 вариант ответа)"""
    
    keyboard = get_family_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_family)

@router.callback_query(F.data.startswith("family_"), StateFilter(UserStates.survey_family))
async def handle_family(callback: CallbackQuery, state: FSMContext):
    """Обработка семейного положения"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "family_selected", callback.data)
    
    family_map = {
        "family_single": "Холост / не замужем",
        "family_married": "В браке",
        "family_divorced": "Разведён(а) / вдов(ец/а)"
    }
    
    family = family_map[callback.data]
    await state.update_data(family_status=family)
    
    text = """<b>❓ Вопрос 6</b>
Есть ли у вас дети?
(выберите 1 вариант ответа)"""
    
    keyboard = get_children_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_children)

@router.callback_query(F.data.startswith("children_"), StateFilter(UserStates.survey_children))
async def handle_children(callback: CallbackQuery, state: FSMContext):
    """Обработка наличия детей"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "children_selected", callback.data)
    
    children_map = {
        "children_none": "Нет",
        "children_one": "Да, один",
        "children_multiple": "Да, двое и более"
    }
    
    children = children_map[callback.data]
    await state.update_data(children=children)
    
    text = """<b>❓ Вопрос 7</b>
Среднемесячный доход на 1 работающего человека в семье 
(выберите 1 вариант ответа)"""
    
    keyboard = get_income_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_income)

@router.callback_query(F.data.startswith("income_"), StateFilter(UserStates.survey_income))
async def handle_income(callback: CallbackQuery, state: FSMContext):
    """Обработка дохода"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "income_selected", callback.data)
    
    income_map = {
        "income_low": "До 20 000 ₽",
        "income_medium": "20–40 тыс ₽",
        "income_high": "40–70 тыс ₽",
        "income_very_high": "Более 70 тыс ₽",
        "income_no_answer": "Предпочитаю не указывать"
    }
    
    income = income_map[callback.data]
    await state.update_data(income=income)
    
    text = """<b>❓ Вопрос 8</b>
Как вы оцениваете своё здоровье по шкале от 0 до 10?
(0 — очень плохо, 10 — отлично)

Введите число"""
    
    await safe_edit_message(callback.message, text)
    await state.set_state(UserStates.survey_health)

@router.message(StateFilter(UserStates.survey_health))
async def handle_health_rating(message: Message, state: FSMContext):
    """Обработка оценки здоровья"""
    await log_user_interaction(message.from_user.id, "health_rating_entered", message.text)
    
    try:
        health_rating = int(message.text.strip())
        if health_rating < 0 or health_rating > 10:
            await message.answer("Пожалуйста, введите число от 0 до 10.")
            return
        
        await state.update_data(health_rating=health_rating)
        
        # Удаляем сообщение пользователя и вопрос
        await message.delete()
        # Пытаемся найти и удалить сообщение с вопросом (обычно предыдущее)
        if message.message_id > 1:
            try:
                await message.bot.delete_message(chat_id=message.chat.id, message_id=message.message_id - 1)
            except:
                pass

        text = """<b>❓ Вопрос 9</b>
На ваш взгляд, какая из перечисленных причин чаще всего приводит к смерти людей в мире? 
(выберите 1 вариант ответа)"""
        
        keyboard = get_death_cause_keyboard()
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        await state.set_state(UserStates.survey_death_cause)
        
    except ValueError:
        await message.answer("Пожалуйста, введите число от 0 до 10.")

@router.callback_query(F.data.startswith("death_cause_"), StateFilter(UserStates.survey_death_cause))
async def handle_death_cause(callback: CallbackQuery, state: FSMContext):
    """Обработка причины смерти"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "death_cause_selected", callback.data)
    
    cause_map = {
        "death_cause_cancer": "Онкологические заболевания",
        "death_cause_cardio": "Сердечно-сосудистые заболевания",
        "death_cause_infections": "Инфекции",
        "death_cause_respiratory": "Болезни дыхательных путей",
        "death_cause_digestive": "Болезни желудочно-кишечного тракта",
        "death_cause_external": "Внешние причины"
    }
    
    death_cause = cause_map[callback.data]
    await state.update_data(death_cause=death_cause)
    
    text = """<b>❓ Вопрос 10</b>
Есть ли у вас хронические заболевания сердца или сосудов?
(выберите 1 вариант ответа)"""
    
    keyboard = get_heart_disease_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_heart_disease)

@router.callback_query(F.data.startswith("heart_disease_"), StateFilter(UserStates.survey_heart_disease))
async def handle_heart_disease(callback: CallbackQuery, state: FSMContext):
    """Обработка наличия заболеваний сердца"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "heart_disease_selected", callback.data)
    
    disease_map = {
        "heart_disease_yes": "Да",
        "heart_disease_no": "Нет",
        "heart_disease_unknown": "Не знаю / не обследовался(ась)"
    }
    
    heart_disease = disease_map[callback.data]
    await state.update_data(heart_disease=heart_disease)
    
    text = """<b>❓ Вопрос 11</b>
Как вы оцениваете свой сердечно-сосудистый риск? 
(выберите 1 вариант ответа)"""
    
    keyboard = get_cv_risk_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_cv_risk)

@router.callback_query(F.data.startswith("cv_risk_"), StateFilter(UserStates.survey_cv_risk))
async def handle_cv_risk(callback: CallbackQuery, state: FSMContext):
    """Обработка оценки сердечно-сосудистого риска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "cv_risk_selected", callback.data)
    
    risk_map = {
        "cv_risk_low": "низкий/умеренный",
        "cv_risk_high": "высокий",
        "cv_risk_very_high": "очень высокий"
    }
    
    cv_risk = risk_map[callback.data]
    await state.update_data(cv_risk=cv_risk)
    
    text = """<b>❓ Вопрос 12</b>
Слышали ли вы раньше о факторах риска сердечно-сосудистых заболеваний?
(выберите 1 вариант ответа)"""
    
    keyboard = get_cv_knowledge_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_cv_knowledge)

@router.callback_query(F.data.startswith("cv_knowledge_"), StateFilter(UserStates.survey_cv_knowledge))
async def handle_cv_knowledge(callback: CallbackQuery, state: FSMContext):
    """Обработка знания о факторах риска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "cv_knowledge_selected", callback.data)
    
    knowledge_map = {
        "cv_knowledge_good": "Да, хорошо разбираюсь",
        "cv_knowledge_some": "Да, но не до конца понимаю",
        "cv_knowledge_none": "Нет / почти ничего не знаю"
    }
    
    cv_knowledge = knowledge_map[callback.data]
    await state.update_data(cv_knowledge=cv_knowledge)
    
    text = """<b>❓ Вопрос 13</b>
Что из перечисленного вы считаете наиболее опасным для сердца?
(выберите до 3 вариантов)"""
    
    await state.update_data(heart_danger_selected=[])  # Инициализируем список выбранных вариантов
    keyboard = get_heart_danger_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_heart_danger)

@router.callback_query(F.data.startswith("heart_danger_"), StateFilter(UserStates.survey_heart_danger))
async def handle_heart_danger(callback: CallbackQuery, state: FSMContext):
    """Обработка опасных факторов для сердца (мультивыбор до 3)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('heart_danger_selected', [])
    
    if callback.data == "heart_danger_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        await state.update_data(heart_danger=selected)
        await log_user_interaction(callback.from_user.id, "heart_danger_completed", f"Selected: {len(selected)} items")
        
        text = """<b>❓ Вопрос 14</b>
Насколько важно для вас наблюдение за здоровьем сердца?
(выберите 1 вариант ответа)"""
        
        keyboard = get_health_importance_keyboard()
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(UserStates.survey_health_importance)
        return
    
    # Обработка выбора вариантов (СООТВЕТСТВУЮТ keyboards.py)
    danger_map = {
        "heart_danger_age": "Возраст",
        "heart_danger_male": "Мужской пол",
        "heart_danger_family": "Семейный анамнез ранних сердечно-сосудистых заболеваний",
        "heart_danger_pressure": "Повышенное артериальное давление",
        "heart_danger_cholesterol": "Повышенный холестерин",
        "heart_danger_glucose": "Повышение глюкозы в крови",
        "heart_danger_weight": "Избыточный вес",
        "heart_danger_smoking": "Курение",
        "heart_danger_alcohol": "Алкоголь",
        "heart_danger_nutrition": "Несбалансированное питание",
        "heart_danger_sedentary": "Малоподвижный образ жизни",
        "heart_danger_stress": "Стрессы",
        "heart_danger_sleep": "Нарушение сна, храп"
    }
    
    danger_option = danger_map.get(callback.data)
    if danger_option:
        if danger_option in selected:
            selected.remove(danger_option)
        else:
            if len(selected) < 3:
                selected.append(danger_option)
            else:
                await safe_answer_callback(callback, "Можно выбрать максимум 3 варианта", show_alert=True)
                return
        
        await state.update_data(heart_danger_selected=selected)
        keyboard = get_heart_danger_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("health_importance_"), StateFilter(UserStates.survey_health_importance))
async def handle_health_importance(callback: CallbackQuery, state: FSMContext):
    """Обработка важности наблюдения за здоровьем сердца"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "health_importance_selected", callback.data)
    
    importance_map = {
        "health_importance_elderly": "Это для пожилых / хронически больных, не про меня",
        "health_importance_secondary": "Важно, но не на первом месте",
        "health_importance_understand": "Понимаю, что нужно, но раньше об этом не думал(а)",
        "health_importance_plan": "Осознаю значимость — планирую действовать"
    }
    
    health_importance = importance_map[callback.data]
    await state.update_data(health_importance=health_importance)
    
    text = """<b>❓ Вопрос 15</b>
Проходили ли вы когда-нибудь комплексное кардиологическое обследование?
(выберите 1 вариант ответа)"""
    
    keyboard = get_checkup_history_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_checkup_history)

@router.callback_query(F.data.startswith("checkup_history_"), StateFilter(UserStates.survey_checkup_history))
async def handle_checkup_history(callback: CallbackQuery, state: FSMContext):
    """Обработка истории кардиочекапов"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "checkup_history_selected", callback.data)
    
    history_map = {
        "checkup_history_yes": "Да, проходил(а)",
        "checkup_history_partial": "Проходил(а) только отдельные обследования",
        "checkup_history_no": "Нет, никогда"
    }
    
    checkup_history = history_map[callback.data]
    await state.update_data(checkup_history=checkup_history)
    
    text = """<b>❓ Вопрос 16</b>
Что бы вы хотели включить в идеальный кардиочекап?
(выберите до 5 вариантов)"""
    
    await state.update_data(checkup_content_selected=[])
    keyboard = get_checkup_content_keyboard([])
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.survey_checkup_content)

@router.callback_query(F.data.startswith("checkup_content_"), StateFilter(UserStates.survey_checkup_content))
async def handle_checkup_content(callback: CallbackQuery, state: FSMContext):
    """ПОЛНОСТЬЮ ИСПРАВЛЕННАЯ обработка содержимого кардиочекапа"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('checkup_content_selected', [])
    
    if callback.data == "checkup_content_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        await state.update_data(checkup_content=selected)
        await log_user_interaction(callback.from_user.id, "checkup_content_completed", f"Selected: {len(selected)} items")
        
        text = """<b>❓ Вопрос 17</b>
Что мешает вам регулярно проходить профилактические обследования?
(выберите до 3 вариантов)"""
        
        await state.update_data(prevention_barriers_selected=[])
        keyboard = get_prevention_barriers_keyboard([])
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(UserStates.survey_prevention_barriers)
        return
    
    # Специальная обработка для "Не проходил(а) кардиочекап"
    if callback.data == "checkup_content_skip":
        skip_option = "Не проходил(а) кардиочекап"
        
        if skip_option in selected:
            # Если уже выбрано "Не проходил", убираем это
            selected.remove(skip_option)
        else:
            # Если выбираем "Не проходил", очищаем все остальные варианты
            selected.clear()
            selected.append(skip_option)
        
        await state.update_data(checkup_content_selected=selected)
        keyboard = get_checkup_content_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)
        return

    # Обработка выбора обычных вариантов (СООТВЕТСТВУЮТ keyboards.py)
    content_map = {
        "checkup_content_consultation": "Консультация и осмотр врача-кардиолога / терапевта",
        "checkup_content_risk_assessment": "Оценка факторов риска сердечно-сосудистых заболеваний",
        "checkup_content_lipids": "Определение уровня липидов крови",
        "checkup_content_glucose": "Определение уровня глюкозы крови",
        "checkup_content_ecg": "ЭКГ",
        "checkup_content_ultrasound": "УЗИ сосудов (дуплексное сканирование)",
        "checkup_content_echo": "ЭхоКГ",
        "checkup_content_monitoring": "Суточное мониторирование давления",
        "checkup_content_ct": "МСКТ-коронарный кальций",
        "checkup_content_calc": "Расчет индивидуального СС-риска"
    }
    
    content_option = content_map.get(callback.data)
    if content_option:
        # Если выбираем обычный вариант, убираем "Не проходил(а)" если он был выбран
        skip_option = "Не проходил(а) кардиочекап"
        if skip_option in selected:
            selected.remove(skip_option)
        
        if content_option in selected:
            selected.remove(content_option)
        else:
            if len(selected) < 5:
                selected.append(content_option)
            else:
                await safe_answer_callback(callback, "Можно выбрать максимум 5 вариантов", show_alert=True)
                return
        
        await state.update_data(checkup_content_selected=selected)
        keyboard = get_checkup_content_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("prevention_barriers_"), StateFilter(UserStates.survey_prevention_barriers))
async def handle_prevention_barriers(callback: CallbackQuery, state: FSMContext):
    """Обработка препятствий для профилактического обследования (мультивыбор)"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('prevention_barriers_selected', [])
    
    if callback.data == "prevention_barriers_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        await state.update_data(prevention_barriers=selected)
        await log_user_interaction(callback.from_user.id, "prevention_barriers_completed", f"Selected: {len(selected)} items")
        
        text = """<b>❓ Вопрос 18 (последний)</b>
С кем вы обычно обсуждаете вопросы здоровья?
(выберите до 2 вариантов)"""
        
        await state.update_data(health_advice_selected=[])
        keyboard = get_health_advice_keyboard([])
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        await state.set_state(UserStates.survey_health_advice)
        return
    
    # Обработка выбора вариантов (СООТВЕТСТВУЮТ keyboards.py)
    barriers_map = {
        "prevention_barriers_no_symptoms": "Не вижу необходимости — нет симптомов",
        "prevention_barriers_fear": "Страх услышать диагноз",
        "prevention_barriers_money": "Финансовые ограничения",
        "prevention_barriers_time": "Нет времени",
        "prevention_barriers_knowledge": "Не знаю, с чего начать",
        "prevention_barriers_doctor": "Уже наблюдаюсь у врача",
        "prevention_barriers_nothing": "Ничего не мешает"
    }
    
    barriers_option = barriers_map.get(callback.data)
    if barriers_option:
        if barriers_option in selected:
            selected.remove(barriers_option)
        else:
            if len(selected) < 3:
                selected.append(barriers_option)
            else:
                await safe_answer_callback(callback, "Можно выбрать максимум 3 варианта", show_alert=True)
                return
        
        await state.update_data(prevention_barriers_selected=selected)
        keyboard = get_prevention_barriers_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("health_advice_"), StateFilter(UserStates.survey_health_advice))
async def handle_health_advice(callback: CallbackQuery, state: FSMContext):
    """НАДЕЖНАЯ обработка последнего вопроса опроса с гарантированным сохранением"""
    await safe_answer_callback(callback)
    
    data = await state.get_data()
    selected = data.get('health_advice_selected', [])
    
    if callback.data == "health_advice_done":
        if not selected:
            await safe_answer_callback(callback, "Выберите хотя бы один вариант", show_alert=True)
            return
        
        await state.update_data(health_advice=selected)
        await log_user_interaction(callback.from_user.id, "health_advice_completed", f"Selected: {len(selected)} items")
        
        # НАДЕЖНОЕ сохранение данных опроса
        survey_success = False
        error_details = ""
        
        try:
            # ПОПЫТКА 1: Обычное сохранение опроса
            logger.info(f"ПОПЫТКА 1: Сохранение опроса для {callback.from_user.id}")
            survey_data = await state.get_data()
            save_result = await save_survey_data(callback.from_user.id, survey_data)
            logger.info(f"✅ ОПРОС СОХРАНЕН: {save_result}")
            survey_success = True
            
        except Exception as e1:
            logger.error(f"❌ ОШИБКА сохранения опроса: {e1}")
            error_details += f"Survey save error: {str(e1)[:100]}; "
            
            # ПОПЫТКА 2: Убеждаемся что пользователь существует
            try:
                logger.info("ПОПЫТКА 2: Проверка/создание пользователя")
                
                user_created = emergency_create_user(callback.from_user.id)
                if user_created:
                    # Повторяем сохранение опроса
                    save_result = await save_survey_data(callback.from_user.id, await state.get_data())
                    survey_success = True
                    logger.info("✅ ОПРОС СОХРАНЕН после создания пользователя")
                
            except Exception as e2:
                logger.error(f"❌ ПОПЫТКА 2 провалилась: {e2}")
                error_details += f"User creation error: {str(e2)[:100]}; "
        
        # ФОРМИРУЕМ ПОДРОБНЫЕ РЕЗУЛЬТАТЫ ОПРОСА
        survey_data = await state.get_data()
        
        # Собираем ключевые результаты
        age = survey_data.get('age', 'не указан')
        gender = survey_data.get('gender', 'не указан')
        location = survey_data.get('location', 'не указано')
        education = survey_data.get('education', 'не указано')
        family_status = survey_data.get('family_status', 'не указано')
        children = survey_data.get('children', 'не указано')
        income = survey_data.get('income', 'не указан')
        health_rating = survey_data.get('health_rating', 'не указана')
        death_cause = survey_data.get('death_cause', 'не указана')
        heart_disease = survey_data.get('heart_disease', 'не указано')
        cv_risk = survey_data.get('cv_risk', 'не указан')
        cv_knowledge = survey_data.get('cv_knowledge', 'не указано')
        heart_danger = survey_data.get('heart_danger', [])
        health_importance = survey_data.get('health_importance', 'не указана')
        checkup_history = survey_data.get('checkup_history', 'не указана')
        checkup_content = survey_data.get('checkup_content', [])
        prevention_barriers = survey_data.get('prevention_barriers', [])
        health_advice = survey_data.get('health_advice', [])
        
        # Форматируем списки
        heart_danger_text = ', '.join(heart_danger) if isinstance(heart_danger, list) and heart_danger else 'не выбрано'
        checkup_content_text = ', '.join(checkup_content) if isinstance(checkup_content, list) and checkup_content else 'не выбрано'
        prevention_barriers_text = ', '.join(prevention_barriers) if isinstance(prevention_barriers, list) and prevention_barriers else 'не выбрано'
        health_advice_text = ', '.join(health_advice) if isinstance(health_advice, list) and health_advice else 'не выбрано'
        
        # ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ ОПРОСА
        results_text = f"""📊 <b>РЕЗУЛЬТАТЫ ВАШЕГО ОПРОСА</b>

<b>👤 Личные данные:</b>
• Возраст: {age}
• Пол: {gender}
• Место жительства: {location}
• Образование: {education}
• Семейное положение: {family_status}
• Дети: {children}
• Доход: {income}

<b>🫀 Здоровье и риски:</b>
• Самооценка здоровья: {health_rating}/10
• Главная причина смерти: {death_cause}
• Заболевания сердца: {heart_disease}
• Сердечно-сосудистый риск: {cv_risk}
• Знания о факторах риска: {cv_knowledge}

<b>⚠️ Опасные факторы для сердца:</b>
{heart_danger_text}

<b>💡 Отношение к здоровью:</b>
• Важность наблюдения: {health_importance}
• Опыт кардиочекапа: {checkup_history}

<b>🔬 Желаемый кардиочекап:</b>
{checkup_content_text}

<b>🚧 Препятствия для обследований:</b>
{prevention_barriers_text}

<b>💬 Обсуждение здоровья:</b>
{health_advice_text}

✅ <b>Спасибо за помощь!</b> 

Мы подходим к следующему этапу — диагностике скрытых факторов риска, которые часто остаются вне фокуса, но напрямую влияют на здоровье сердца и сосудов.

👉 После тестов я пришлю вам список базовых анализов и чек-лист подготовки к вебинару."""
        
        await safe_edit_message(callback.message, results_text)
        
        if survey_success:
            logger.info(f"✅ ОПРОС {callback.from_user.id} СОХРАНЕН УСПЕШНО")
        else:
            logger.error(f"❌ ОПРОС {callback.from_user.id} НЕ СОХРАНЕН: {error_details}")
            # НО НЕ показываем ошибку пользователю
        
        # ВСЕГДА переходим к тестам
        await asyncio.sleep(5)
        await start_tests(callback.message, state)
        return
    
    # Обработка выбора вариантов (без изменений)
    advice_map = {
        "health_advice_doctor": "С врачом",
        "health_advice_relatives": "С родственниками",
        "health_advice_colleagues": "С коллегами",
        "health_advice_internet": "Через интернет (статьи, форумы)",
        "health_advice_blogger": "С врачом-блогером в соцсетях",
        "health_advice_nobody": "Ни с кем"
    }
    
    advice_option = advice_map.get(callback.data)
    if advice_option:
        if advice_option in selected:
            selected.remove(advice_option)
        else:
            if len(selected) < 2:
                selected.append(advice_option)
            else:
                await safe_answer_callback(callback, "Можно выбрать максимум 2 варианта", show_alert=True)
                return
        
        await state.update_data(health_advice_selected=selected)
        keyboard = get_health_advice_keyboard(selected)
        await safe_edit_message(callback.message, callback.message.text, reply_markup=keyboard)

# ============================================================================
# ПЕРЕХОД К ТЕСТАМ
# ============================================================================

async def start_tests(message: Message, state: FSMContext):
    """Начало психологических тестов"""
    await log_user_interaction(message.from_user.id, "tests_started")
    
    await asyncio.sleep(2)
    
    text = """🧠 <b>ПСИХОЛОГИЧЕСКИЕ ТЕСТЫ</b>

Теперь пройдём короткие тесты для оценки психологических факторов риска.

Эти тесты помогут определить:
• Уровень тревожности и депрессии
• Качество сна
• Риск апноэ
• Вредные привычки

📊 Всего 7 тестов, займет 10-15 минут.

Начнём?"""
    
    from ui.keyboards import get_test_selection_keyboard
    keyboard = get_test_selection_keyboard()
    
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(UserStates.test_selection)