import asyncio
import logging
import os
from datetime import datetime
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext

from .base import UserStates, safe_edit_message, safe_answer_callback, log_user_interaction
from ui.keyboards import get_test_selection_keyboard
from tests import (
    get_hads_questions, calculate_hads_scores, get_hads_interpretation,
    get_burns_questions, get_burns_interpretation,
    get_isi_questions, get_isi_interpretation,
    get_stop_bang_questions, get_stop_bang_interpretation
)

# Импортируем недостающие функции тестов
try:
    from tests import get_ess_questions, get_fagerstrom_questions, get_audit_questions
except ImportError:
    # Если функции не найдены в модуле tests, определяем их здесь
    import sys
    import os
    
    # Загружаем функции из tests.txt
    tests_file_path = os.path.join(os.path.dirname(__file__), '..', 'tests.txt')
    with open(tests_file_path, 'r', encoding='utf-8') as f:
        tests_content = f.read()
    exec(tests_content)

# Добавляем недостающие интерпретации
def get_ess_interpretation(score: int) -> str:
    """Интерпретация результатов ESS"""
    if score <= 10:
        level = "норма"
        recommendation = "✅ <b>Результат:</b> Ваш уровень дневной сонливости находится в пределах нормы."
    elif score <= 12:
        level = "легкая сонливость"
        recommendation = "⚠️ <b>Рекомендация:</b> Легкая дневная сонливость. Обратите внимание на качество и продолжительность ночного сна."
    elif score <= 15:
        level = "умеренная сонливость"
        recommendation = "🔶 <b>Рекомендация:</b> Умеренная дневная сонливость. Рекомендуется консультация врача для выявления причин."
    else:
        level = "выраженная сонливость"
        recommendation = "🚨 <b>Рекомендация:</b> Выраженная дневная сонливость. Необходима консультация специалиста по расстройствам сна."
    
    return f"<b>ESS (Дневная сонливость):</b> {score} баллов - {level}\n\n{recommendation}"

def get_fagerstrom_interpretation(score: int) -> str:
    """Интерпретация результатов теста Фагерстрема"""
    if score <= 2:
        level = "очень слабая зависимость"
        recommendation = "✅ <b>Результат:</b> Очень слабая никотиновая зависимость. У вас хорошие перспективы для отказа от курения."
    elif score <= 4:
        level = "слабая зависимость"
        recommendation = "🟡 <b>Результат:</b> Слабая никотиновая зависимость. Отказ от курения возможен при желании и поддержке."
    elif score <= 6:
        level = "средняя зависимость"
        recommendation = "🔶 <b>Рекомендация:</b> Средняя никотиновая зависимость. Рекомендуется поддержка специалиста при отказе от курения."
    elif score <= 8:
        level = "сильная зависимость"
        recommendation = "🚨 <b>Рекомендация:</b> Сильная никотиновая зависимость. Необходима профессиональная помощь для отказа от курения."
    else:
        level = "очень сильная зависимость"
        recommendation = "🔴 <b>Рекомендация:</b> Очень сильная никотиновая зависимость. Требуется комплексная медицинская поддержка."
    
    return f"<b>Тест Фагерстрема:</b> {score} баллов - {level}\n\n{recommendation}"

def get_audit_interpretation(score: int) -> str:
    """Интерпретация результатов AUDIT"""
    if score <= 7:
        level = "зона I: низкий риск"
        recommendation = "✅ <b>Результат:</b> Низкий риск проблем, связанных с алкоголем. Рекомендуется продолжать умеренное потребление или воздержание."
    elif score <= 15:
        level = "зона II: опасное потребление"
        recommendation = "🔶 <b>Рекомендация:</b> Опасное потребление алкоголя. Рекомендуется сократить употребление и получить краткую консультацию."
    elif score <= 19:
        level = "зона III: вредное потребление"
        recommendation = "🚨 <b>Рекомендация:</b> Вредное потребление алкоголя. Необходима краткая консультация, продолжительное наблюдение и мониторинг."
    else:
        level = "зона IV: возможная зависимость"
        recommendation = "🔴 <b>Рекомендация:</b> Возможная алкогольная зависимость. Требуется направление к специалисту для диагностического обследования и лечения."
    
    return f"<b>AUDIT (Употребление алкоголя):</b> {score} баллов - {level}\n\n{recommendation}"

from database import safe_save_user_data, get_db_sync, TestResult, mark_user_completed, User
import json

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def get_answer_keyboard(options):
    """Создает клавиатуру с вариантами ответов для тестов"""
    buttons = []
    for i, option in enumerate(options):
        text = option['text']
        score = option['score']
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"answer_{score}")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    return keyboard

# ============================================================================
# ФУНКЦИИ СОХРАНЕНИЯ ТЕСТОВ
# ============================================================================

async def save_test_results(telegram_id: int, test_results: dict) -> bool:
    """Сохранение результатов психологических тестов в базу данных"""
    db = get_db_sync()
    try:
        # Ищем существующую запись результатов тестов
        test_result = db.query(TestResult).filter(TestResult.telegram_id == telegram_id).first()
        
        if not test_result:
            # Создаем новую запись
            test_result = TestResult(telegram_id=telegram_id)
            db.add(test_result)
        
        # Обновляем поля в зависимости от переданных результатов
        for key, value in test_results.items():
            if hasattr(test_result, key):
                setattr(test_result, key, value)
                logger.info(f"Сохранен {key} = {value} для пользователя {telegram_id}")
        
        # Сохраняем интерпретации и уровни
        if 'hads_anxiety_score' in test_results and 'hads_depression_score' in test_results:
            interpretation = get_hads_interpretation(
                test_results['hads_anxiety_score'], 
                test_results['hads_depression_score']
            )
            test_result.hads_interpretation = interpretation
            # Добавляем уровни HADS
            test_result.hads_anxiety_level = get_hads_anxiety_level(test_results['hads_anxiety_score'])
            test_result.hads_depression_level = get_hads_depression_level(test_results['hads_depression_score'])
            test_result.hads_total_score = test_results['hads_anxiety_score'] + test_results['hads_depression_score']
        
        if 'burns_score' in test_results:
            test_result.burns_interpretation = get_burns_interpretation(test_results['burns_score'])
            test_result.burns_level = get_burns_level(test_results['burns_score'])
        
        if 'isi_score' in test_results:
            test_result.isi_interpretation = get_isi_interpretation(test_results['isi_score'])
            test_result.isi_level = get_isi_level(test_results['isi_score'])
        
        if 'stop_bang_score' in test_results:
            test_result.stop_bang_risk = get_stop_bang_risk(test_results['stop_bang_score'])
            
        if 'ess_score' in test_results:
            test_result.ess_level = get_ess_level(test_results['ess_score'])
            
        if 'fagerstrom_score' in test_results:
            test_result.fagerstrom_level = get_fagerstrom_level(test_results['fagerstrom_score'])
            
        if 'audit_score' in test_results:
            test_result.audit_level = get_audit_level(test_results['audit_score'])
        
        if 'stop_bang_score' in test_results:
            test_result.stop_bang_interpretation = get_stop_bang_interpretation(test_results['stop_bang_score'])
        
        # Рассчитываем общий CV риск и количество факторов риска
        risk_factors_count = calculate_risk_factors_count(test_results)
        overall_risk = calculate_risk_level(test_results)
        
        test_result.risk_factors_count = risk_factors_count
        test_result.overall_cv_risk_level = overall_risk
        test_result.overall_cv_risk_score = calculate_cv_risk_score(test_results)
        
        # Устанавливаем время завершения тестов
        test_result.completed_at = datetime.now()
        
        # Отмечаем пользователя как завершившего тесты
        user = db.query(User).filter(User.telegram_id == telegram_id).first()
        if user:
            user.tests_completed = True
            user.last_activity = datetime.now()
        
        db.commit()
        logger.info(f"✅ Результаты тестов сохранены для пользователя {telegram_id}")
        return True
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Ошибка сохранения результатов тестов для {telegram_id}: {e}")
        return False
    finally:
        db.close()

def calculate_risk_level(test_results: dict) -> str:
    """Расчет общего уровня риска на основе результатов тестов"""
    risk_score = 0
    
    # HADS тревога
    hads_anxiety = test_results.get('hads_anxiety_score', 0)
    if hads_anxiety >= 11:
        risk_score += 3
    elif hads_anxiety >= 8:
        risk_score += 1
    
    # HADS депрессия
    hads_depression = test_results.get('hads_depression_score', 0)
    if hads_depression >= 11:
        risk_score += 3
    elif hads_depression >= 8:
        risk_score += 1
    
    # Burns депрессия
    burns_score = test_results.get('burns_score', 0)
    if burns_score >= 25:
        risk_score += 2
    elif burns_score >= 11:
        risk_score += 1
    
    # ISI бессонница
    isi_score = test_results.get('isi_score', 0)
    if isi_score >= 15:
        risk_score += 2
    elif isi_score >= 8:
        risk_score += 1
    
    # STOP-BANG апноэ
    stop_bang_score = test_results.get('stop_bang_score', 0)
    if stop_bang_score >= 5:
        risk_score += 3
    elif stop_bang_score >= 3:
        risk_score += 1
    
    # Определяем уровень риска
    if risk_score <= 2:
        return "НИЗКИЙ"
    elif risk_score <= 5:
        return "УМЕРЕННЫЙ"
    elif risk_score <= 8:
        return "ВЫСОКИЙ"
    else:
        return "ОЧЕНЬ ВЫСОКИЙ"

async def mark_diagnostic_completed(telegram_id: int) -> bool:
    """Отметка завершения диагностики"""
    try:
        result = await mark_user_completed(telegram_id)
        logger.info(f"✅ Диагностика завершена для пользователя {telegram_id}")
        return result
    except Exception as e:
        logger.error(f"❌ Ошибка отметки завершения диагностики для {telegram_id}: {e}")
        return False

# ============================================================================
# ОБРАБОТЧИКИ ТЕСТОВ
# ============================================================================

@router.callback_query(F.data.startswith("test_"), StateFilter(UserStates.test_selection))
async def handle_test_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора теста"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "test_selected", callback.data)
    
    if callback.data == "test_hads":
        await start_hads_test(callback.message, state)
    elif callback.data == "test_burns":
        await start_burns_test(callback.message, state)
    elif callback.data == "test_isi":
        await start_isi_test(callback.message, state)
    elif callback.data == "test_stop_bang":
        await start_stop_bang_test(callback.message, state)
    elif callback.data == "test_ess":
        await start_ess_test(callback.message, state)
    elif callback.data == "test_fagerstrom":
        await start_fagerstrom_test(callback.message, state)
    elif callback.data == "test_fagerstrom_skip":
        await state.update_data(fagerstrom_score=None, fagerstrom_skipped=True)
        await show_test_menu(callback.message, state)
    elif callback.data == "test_audit":
        await start_audit_test(callback.message, state)
    elif callback.data == "test_audit_skip":
        await state.update_data(audit_score=None, audit_skipped=True)
        await show_test_menu(callback.message, state)
    elif callback.data == "test_complete":
        await complete_all_tests(callback.message, state)

async def show_test_menu(message: Message, state: FSMContext):
    """Показать меню выбора тестов"""
    data = await state.get_data()
    
    text = "Выберите следующий тест для прохождения:"
    keyboard = get_test_selection_keyboard(data)
    
    await safe_edit_message(message, text, reply_markup=keyboard)
    await state.set_state(UserStates.test_selection)

# ============================================================================
# ЗАПУСК ОТДЕЛЬНЫХ ТЕСТОВ
# ============================================================================

async def start_hads_test(message: Message, state: FSMContext):
    """Запуск теста HADS"""
    questions = get_hads_questions()
    await state.update_data(
        current_test="hads",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🟣 <b>Тест 1. Уровень тревоги и депрессии — HADS</b>

Тест HADS помогает понять, есть ли у вас признаки тревоги или депрессии.

Ответьте на вопросы, выбрав наиболее подходящий вариант."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.hads_test)

async def start_burns_test(message: Message, state: FSMContext):
    """Запуск теста Бернса"""
    questions = get_burns_questions()
    await state.update_data(
        current_test="burns",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🔵 <b>Тест 2. Эмоциональное выгорание — Шкала депрессии Бернса</b>

Тест Бернса поможет оценить ваш эмоциональный фон.

Ответьте на вопросы, оценив каждое утверждение по шкале от 0 до 4."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.burns_test)

async def start_isi_test(message: Message, state: FSMContext):
    """Запуск теста ISI"""
    questions = get_isi_questions()
    await state.update_data(
        current_test="isi",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🌙 <b>Тест 3. Качество сна — ISI</b>

Тест ISI поможет понять, есть у вас ли признаки бессонницы или других нарушений сна, которые могут негативно сказываться на здоровье сердца."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.isi_test)

async def start_stop_bang_test(message: Message, state: FSMContext):
    """Запуск теста STOP-BANG"""
    questions = get_stop_bang_questions()
    await state.update_data(
        current_test="stop_bang",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """😴 <b>Тест 4. Риск апноэ сна — STOP-BANG</b>

Тест STOP-BANG поможет определить, есть ли у вас риск апноэ сна — состояния, при котором дыхание во сне периодически останавливается. Его опасность часто недооценивается, хотя апноэ сна напрямую связано с риском инфаркта и инсульта."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.stop_bang_test)

async def start_ess_test(message: Message, state: FSMContext):
    """Запуск теста ESS"""
    questions = get_ess_questions()
    await state.update_data(
        current_test="ess",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """😴 <b>Тест 5. Сонливость днём — ESS</b>

Тест ESS поможет оценить уровень дневной сонливости, чтобы выявить скрытые нарушения сна, которые могут влиять на давление, работу сердца и общее самочувствие."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.ess_test)

async def start_fagerstrom_test(message: Message, state: FSMContext):
    """Запуск теста Фагерстрема"""
    questions = get_fagerstrom_questions()
    await state.update_data(
        current_test="fagerstrom",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🚬 <b>Тест 6. Никотиновая зависимость — Фагерстрем</b>

Тест для оценки степени никотиновой зависимости у курящих людей."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.fagerstrom_test)

async def start_audit_test(message: Message, state: FSMContext):
    """Запуск теста AUDIT"""
    questions = get_audit_questions()
    await state.update_data(
        current_test="audit",
        test_questions=questions,
        current_question_index=0,
        test_answers=[]
    )
    
    text = """🍷 <b>Тест 7. Употребление алкоголя — RUS-AUDIT</b>

Тест AUDIT поможет оценить влияние алкоголя на сосудистое здоровье."""
    
    await safe_edit_message(message, text)
    await asyncio.sleep(2)
    
    await show_current_question(message, state)
    await state.set_state(UserStates.audit_test)

# ============================================================================
# ОБРАБОТКА ВОПРОСОВ И ОТВЕТОВ
# ============================================================================

async def show_current_question(message: Message, state: FSMContext):
    """Показать текущий вопрос теста"""
    data = await state.get_data()
    questions = data.get('test_questions', [])
    current_index = data.get('current_question_index', 0)
    
    if current_index >= len(questions):
        await finish_current_test(message, state)
        return
    
    question = questions[current_index]
    
    text = f"""<b>Вопрос {current_index + 1} из {len(questions)}</b>

{question['text']}"""
    
    keyboard = get_answer_keyboard(question['options'])
    await safe_edit_message(message, text, reply_markup=keyboard)

@router.callback_query(F.data.startswith("answer_"))
async def handle_test_answer(callback: CallbackQuery, state: FSMContext):
    """Обработка ответа на вопрос теста с защитой от потери состояния"""
    await safe_answer_callback(callback)
    
    # Получаем индекс ответа
    try:
        answer_index = int(callback.data.split("_")[1])
    except (ValueError, IndexError):
        logger.error(f"Неверный формат callback_data: {callback.data}")
        return
    
    data = await state.get_data()
    current_test = data.get('current_test')
    questions = data.get('test_questions', [])
    current_index = data.get('current_question_index', 0)
    answers = data.get('test_answers', [])
    
    # Проверяем корректность данных
    if not current_test or not questions or current_index >= len(questions):
        logger.error(f"Некорректные данные теста для пользователя {callback.from_user.id}")
        await safe_edit_message(callback.message, "❌ Ошибка теста. Возвращаемся к меню тестов.")
        await show_test_menu(callback.message, state)
        return
    
    question = questions[current_index]
    
    # Проверяем корректность индекса ответа
    if answer_index >= len(question['options']):
        logger.error(f"Неверный индекс ответа: {answer_index} для вопроса с {len(question['options'])} ответами")
        return
    
    # Сохраняем ответ
    selected_answer = question['options'][answer_index]
    answers.append({
        'question_index': current_index,
        'answer_index': answer_index,
        'answer_text': selected_answer['text'],
        'score': selected_answer.get('score', 0)
    })
    
    await log_user_interaction(
        callback.from_user.id, 
        f"{current_test}_answer", 
        f"Q{current_index + 1}: {selected_answer['text']}"
    )
    
    # Переходим к следующему вопросу
    next_index = current_index + 1
    await state.update_data(
        current_question_index=next_index,
        test_answers=answers
    )
    
    # Показываем следующий вопрос или завершаем тест
    if next_index >= len(questions):
        await finish_current_test(callback.message, state)
    else:
        await show_current_question(callback.message, state)

# ============================================================================
# ЗАВЕРШЕНИЕ ТЕСТОВ
# ============================================================================

async def finish_current_test(message: Message, state: FSMContext):
    """Завершение текущего теста и подсчет результатов"""
    data = await state.get_data()
    current_test = data.get('current_test')
    answers = data.get('test_answers', [])
    
    # Обрабатываем результат в зависимости от типа теста
    if current_test == "hads":
        # HADS имеет особую логику - два отдельных счета
        questions = data.get('test_questions', [])
        anxiety_score = 0
        depression_score = 0
        
        for i, answer in enumerate(answers):
            if i < len(questions):
                if questions[i].get("type") == "anxiety":
                    anxiety_score += answer.get('score', 0)
                else:
                    depression_score += answer.get('score', 0)
        
        await state.update_data(**{
            "hads_anxiety_score": anxiety_score,
            "hads_depression_score": depression_score,
            "completed_hads": True
        })
        
        # Используем специальную интерпретацию HADS
        interpretation = get_hads_interpretation(anxiety_score, depression_score)
    else:
        # Обычные тесты
        total_score = sum(answer.get('score', 0) for answer in answers)
        
        # Сохраняем результат в состоянии
        test_score_key = f"{current_test}_score"
        test_completed_key = f"completed_{current_test}"
        await state.update_data(**{
            test_score_key: total_score,
            test_completed_key: True
        })
        
        # Получаем интерпретацию результата
        interpretation = get_test_interpretation(current_test, total_score)
    
    # Отправляем мини-результат как новое сообщение (остается в истории)
    mini_result_text = f"""✅ <b>Тест завершен!</b>

{interpretation}"""
    
    await message.bot.send_message(
        chat_id=message.chat.id,
        text=mini_result_text,
        parse_mode="HTML"
    )
    
    await asyncio.sleep(2)
    
    # Показываем меню тестов как новое сообщение
    menu_text = "Выберите следующий тест для прохождения:"
    data = await state.get_data()
    keyboard = get_test_selection_keyboard(data)
    
    await message.bot.send_message(
        chat_id=message.chat.id,
        text=menu_text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(UserStates.test_selection)

def get_test_interpretation(test_name: str, score: int) -> str:
    """Получить интерпретацию результата теста"""
    interpretations = {
        "hads": {
            "name": "HADS (тревога и депрессия)",
            "ranges": [
                (0, 7, "Норма"),
                (8, 10, "Субклинически выраженная тревога/депрессия"),
                (11, 21, "Клинически выраженная тревога/депрессия")
            ]
        },
        "burns": {
            "name": "Шкала депрессии Бернса",
            "ranges": [
                (0, 4, "Норма"),
                (5, 10, "Легкая депрессия"),
                (11, 25, "Умеренная депрессия"),
                (26, 50, "Выраженная депрессия"),
                (51, 75, "Тяжелая депрессия"),
                (76, 100, "Крайне тяжелая депрессия")
            ]
        },
        "isi": {
            "name": "ISI (качество сна)",
            "ranges": [
                (0, 7, "Отсутствие клинически значимой бессонницы"),
                (8, 14, "Субклиническая бессонница"),
                (15, 21, "Клиническая бессонница (умеренная)"),
                (22, 28, "Клиническая бессонница (тяжелая)")
            ]
        },
        "stop_bang": {
            "name": "STOP-BANG (риск апноэ)",
            "ranges": [
                (0, 2, "Низкий риск апноэ сна"),
                (3, 4, "Промежуточный риск апноэ сна"),
                (5, 8, "Высокий риск апноэ сна")
            ]
        },
        "ess": {
            "name": "ESS (дневная сонливость)",
            "ranges": [
                (0, 10, "Норма"),
                (11, 12, "Легкая сонливость"),
                (13, 15, "Умеренная сонливость"),
                (16, 24, "Выраженная сонливость")
            ]
        },
        "fagerstrom": {
            "name": "Тест Фагерстрема (никотиновая зависимость)",
            "ranges": [
                (0, 2, "Очень слабая зависимость"),
                (3, 4, "Слабая зависимость"),
                (5, 6, "Средняя зависимость"),
                (7, 8, "Сильная зависимость"),
                (9, 10, "Очень сильная зависимость")
            ]
        },
        "audit": {
            "name": "AUDIT (употребление алкоголя)",
            "ranges": [
                (0, 7, "Зона I: низкий риск"),
                (8, 15, "Зона II: опасное потребление"),
                (16, 19, "Зона III: вредное потребление"),
                (20, 40, "Зона IV: возможная зависимость")
            ]
        }
    }
    
    test_info = interpretations.get(test_name, {})
    test_display_name = test_info.get("name", test_name.upper())
    ranges = test_info.get("ranges", [])
    
    result_text = "Результат не определен"
    for min_score, max_score, description in ranges:
        if min_score <= score <= max_score:
            result_text = description
            break
    
    return f"<b>{test_display_name}</b>\nВаш результат: {score} баллов\nИнтерпретация: {result_text}"

# ============================================================================
# ЗАВЕРШЕНИЕ ВСЕХ ТЕСТОВ
# ============================================================================

async def send_detailed_results(message: Message, test_results: dict):
    """Отправляем детальные результаты всех тестов как отдельное сообщение"""
    try:
        detailed_text = "📋 <b>ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ</b>\n\n"
        
        # HADS результаты
        if test_results.get('hads_anxiety_score') is not None and test_results.get('hads_depression_score') is not None:
            anxiety_score = test_results['hads_anxiety_score']
            depression_score = test_results['hads_depression_score']
            total_hads = anxiety_score + depression_score
            detailed_text += f"🟣 <b>HADS (Тревога и депрессия):</b>\n"
            detailed_text += f"   • Тревога: {anxiety_score} баллов ({get_hads_anxiety_level(anxiety_score)})\n"
            detailed_text += f"   • Депрессия: {depression_score} баллов ({get_hads_depression_level(depression_score)})\n"
            detailed_text += f"   • Общий балл: {total_hads}\n\n"
        
        # Burns результаты
        if test_results.get('burns_score') is not None:
            burns_score = test_results['burns_score']
            detailed_text += f"🔵 <b>Тест Бернса (Выгорание):</b>\n"
            detailed_text += f"   • Балл: {burns_score} ({get_burns_level(burns_score)})\n\n"
        
        # ISI результаты
        if test_results.get('isi_score') is not None:
            isi_score = test_results['isi_score']
            detailed_text += f"🌙 <b>ISI (Качество сна):</b>\n"
            detailed_text += f"   • Балл: {isi_score} ({get_isi_level(isi_score)})\n\n"
        
        # STOP-BANG результаты
        if test_results.get('stop_bang_score') is not None:
            stop_bang_score = test_results['stop_bang_score']
            detailed_text += f"😴 <b>STOP-BANG (Апноэ сна):</b>\n"
            detailed_text += f"   • Балл: {stop_bang_score} ({get_stop_bang_risk(stop_bang_score)})\n\n"
        
        # ESS результаты
        if test_results.get('ess_score') is not None:
            ess_score = test_results['ess_score']
            detailed_text += f"😴 <b>ESS (Дневная сонливость):</b>\n"
            detailed_text += f"   • Балл: {ess_score} ({get_ess_level(ess_score)})\n\n"
        
        # Fagerstrom результаты
        if test_results.get('fagerstrom_score') is not None:
            fagerstrom_score = test_results['fagerstrom_score']
            detailed_text += f"🚬 <b>Фагерстрем (Никотин):</b>\n"
            detailed_text += f"   • Балл: {fagerstrom_score} ({get_fagerstrom_level(fagerstrom_score)})\n\n"
        elif test_results.get('fagerstrom_skipped'):
            detailed_text += f"🚬 <b>Фагерстрем:</b> Пропущен (не курю)\n\n"
        
        # AUDIT результаты
        if test_results.get('audit_score') is not None:
            audit_score = test_results['audit_score']
            detailed_text += f"🍷 <b>AUDIT (Алкоголь):</b>\n"
            detailed_text += f"   • Балл: {audit_score} ({get_audit_level(audit_score)})\n\n"
        elif test_results.get('audit_skipped'):
            detailed_text += f"🍷 <b>AUDIT:</b> Пропущен (не употребляю)\n\n"
        
        # Общая оценка риска
        overall_risk = calculate_risk_level(test_results)
        risk_factors = calculate_risk_factors_count(test_results)
        
        detailed_text += f"🎯 <b>ОБЩАЯ ОЦЕНКА:</b>\n"
        detailed_text += f"   • Сердечно-сосудистый риск: <b>{overall_risk}</b>\n"
        detailed_text += f"   • Факторов риска выявлено: {risk_factors}\n\n"
        
        detailed_text += f"📌 <i>Эти результаты помогут вам лучше подготовиться к вебинару и получить персональные рекомендации от врачей.</i>"
        
        await message.bot.send_message(
            chat_id=message.chat.id,
            text=detailed_text,
            parse_mode="HTML"
        )
        logger.info(f"✅ Отправлены детальные результаты пользователю {message.chat.id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки детальных результатов: {e}")

async def complete_all_tests(message: Message, state: FSMContext):
    """Завершение всех тестов и генерация итогового отчета"""
    await log_user_interaction(message.from_user.id, "all_tests_completed")
    
    data = await state.get_data()
    
    # Собираем результаты всех тестов
    test_results = {
        'hads_anxiety_score': data.get('hads_anxiety_score'),
        'hads_depression_score': data.get('hads_depression_score'),
        'burns_score': data.get('burns_score'),
        'isi_score': data.get('isi_score'),
        'stop_bang_score': data.get('stop_bang_score'),
        'ess_score': data.get('ess_score'),
        'fagerstrom_score': data.get('fagerstrom_score'),
        'audit_score': data.get('audit_score'),
        'fagerstrom_skipped': data.get('fagerstrom_skipped', False),
        'audit_skipped': data.get('audit_skipped', False)
    }
    
    # Сохраняем результаты в базе данных
    try:
        save_success = await save_test_results(message.chat.id, test_results)
        if save_success:
            logger.info(f"✅ Результаты тестов сохранены для пользователя {message.chat.id}")
        else:
            logger.error(f"❌ Ошибка сохранения результатов тестов для пользователя {message.chat.id}")
    except Exception as e:
        logger.error(f"❌ Исключение при сохранении результатов тестов: {e}")
    
    # Рассчитываем общий уровень риска
    overall_risk = calculate_risk_level(test_results)
    
    text = f"""🫀 Отлично! Вы прошли предварительную диагностику — и это отличный фундамент для следующего шага.

На вебинаре вы сможете:
✔️ рассчитать персональный суммарный риск сердечно-сосудистых заболеваний и возможных катастроф
✔️ собрать маршрутную карту действий — пошаговый план, который поможет защитить сердце и сохранить активность на годы вперёд
✔️ получить от врачей ответы на ваши вопросы

Теперь вы подготовлены, и на вебинаре получите максимум пользы.

🎯 <b>Ваш сердечно-сосудистый риск:</b> {overall_risk}

📩 А сейчас — как и обещали:
📌 Список базовых анализов для подготовки к вебинару (можно пройти по желанию)
📌 Бонус: чек-лист «Препараты и методики, которые не лечат сердце и сосуды»"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Посмотреть полные результаты", callback_data="show_full_results")],
        [InlineKeyboardButton(text="📋 Материалы к вебинару", callback_data="show_materials")]
    ])
    
    await safe_edit_message(message, text, reply_markup=keyboard)
    
    # ДОПОЛНИТЕЛЬНО: отправляем детальные результаты как отдельное сообщение
    await send_detailed_results(message, test_results)
    
    # Отправляем бонусные материалы
    try:
        bot = message.bot
        chat_id = message.chat.id
        
        # Абсолютный путь к папке с материалами
        materials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'materials'))
        logger.info(f"🔍 Ищем материалы в папке: {materials_path}")
        
        # Проверяем существование папки
        if not os.path.exists(materials_path):
            logger.error(f"❌ Папка с материалами не найдена: {materials_path}")
            await bot.send_message(chat_id, "⚠️ Материалы временно недоступны. Обратитесь к администратору.")
            return
        
        # Отправляем список анализов
        analyses_file = os.path.join(materials_path, 'analyses_list.pdf')
        logger.info(f"🔍 Проверяем файл анализов: {analyses_file}")
        
        if os.path.exists(analyses_file):
            analyses_input = FSInputFile(analyses_file)
            await bot.send_document(
                chat_id=chat_id,
                document=analyses_input,
                caption="📌 Список базовых анализов для подготовки к вебинару"
            )
            logger.info(f"✅ Отправлен список анализов пользователю {chat_id}")
        else:
            logger.error(f"❌ Файл с анализами не найден: {analyses_file}")
        
        # Отправляем бонус чек-лист
        bonus_file = os.path.join(materials_path, 'webinar_preparation.png')
        logger.info(f"🔍 Проверяем бонус файл: {bonus_file}")
        
        if os.path.exists(bonus_file):
            bonus_input = FSInputFile(bonus_file)
            await bot.send_photo(
                chat_id=chat_id,
                photo=bonus_input,
                caption="📌 Бонус: чек-лист «Препараты и методики, которые не лечат сердце и сосуды»"
            )
            logger.info(f"✅ Отправлен бонус чек-лист пользователю {chat_id}")
        else:
            logger.error(f"❌ Бонус файл не найден: {bonus_file}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка отправки бонусных материалов: {e}")
        import traceback
        logger.error(f"Подробности ошибки: {traceback.format_exc()}")
    
    # Отмечаем диагностику как завершенную
    try:
        await mark_diagnostic_completed(message.chat.id)
    except Exception as e:
        logger.error(f"Ошибка отметки завершения диагностики: {e}")

# ============================================================================
# ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ
# ============================================================================

@router.callback_query(F.data == "retry_save_tests")
async def retry_save_tests(callback: CallbackQuery, state: FSMContext):
    """ИСПРАВЛЕННЫЙ повтор сохранения - сразу к материалам"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "retry_save_tests")
    
    text = """✅ <b>ДИАГНОСТИКА ЗАВЕРШЕНА!</b>

Все данные сохранены. Теперь вы можете ознакомиться с материалами к вебинару.

🗓 <b>Вебинар "Умный кардиочекап":</b>
📅 3 августа в 12:00 МСК
📍 Ссылка появится здесь за час до начала

💡 Подготовьте к вебинару результаты анализов (если есть) и вопросы врачам."""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Материалы к вебинару", callback_data="show_materials")],
        [InlineKeyboardButton(text="📊 Мои результаты", callback_data="show_full_results")]
    ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)

@router.callback_query(F.data == "test_complete")
async def handle_test_complete_button(callback: CallbackQuery, state: FSMContext):
    """ИСПРАВЛЕННЫЙ обработчик кнопки завершения тестов"""
    await safe_answer_callback(callback)
    await complete_all_tests(callback.message, state)

@router.callback_query(F.data == "test_check_completion")
async def check_test_completion(callback: CallbackQuery, state: FSMContext):
    """ИСПРАВЛЕННАЯ проверка - требуем минимум 5 тестов"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "test_completion_check")
    
    data = await state.get_data()
    
    # Подсчитываем завершенные обязательные тесты
    completed_tests = []
    if data.get('hads_anxiety_score') is not None or data.get('completed_hads'):
        completed_tests.append("HADS")
    if data.get('burns_score') is not None or data.get('completed_burns'):
        completed_tests.append("Бернса")
    if data.get('isi_score') is not None or data.get('completed_isi'):
        completed_tests.append("ISI")
    if data.get('stop_bang_score') is not None or data.get('completed_stop_bang'):
        completed_tests.append("STOP-BANG")
    if data.get('ess_score') is not None or data.get('completed_ess'):
        completed_tests.append("ESS")
    
    completed_count = len(completed_tests)
    
    # Учитываем опциональные тесты (включая пропущенные)
    optional_tests_done = 0
    if data.get('fagerstrom_score') is not None or data.get('fagerstrom_skipped') or data.get('completed_fagerstrom'):
        optional_tests_done += 1
    if data.get('audit_score') is not None or data.get('audit_skipped') or data.get('completed_audit'):
        optional_tests_done += 1
        
    total_tests_done = completed_count + optional_tests_done
    
    if completed_count >= 5:
        text = f"""✅ <b>ТЕСТЫ МОЖНО ЗАВЕРШИТЬ</b>

Вы прошли все 5 обязательных тестов!
Завершено тестов: {total_tests_done} из 7 (включая опциональные)

❓ Хотите завершить тестирование и получить материалы?"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Да, завершить и получить материалы", callback_data="test_complete")],
            [InlineKeyboardButton(text="🔙 Продолжить тесты", callback_data="continue_tests")]
        ])
    else:
        missing_count = 5 - completed_count
        text = f"""⚠️ <b>НУЖНО ПРОЙТИ ЕЩЕ ТЕСТЫ</b>

Завершено: {completed_count} из 5 обязательных тестов
Опциональных: {optional_tests_done} из 2
Осталось обязательных: {missing_count} тестов

Для получения материалов нужно пройти все 5 обязательных тестов.
Фагерстрем и AUDIT можно пропустить, если не применимо."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Продолжить тестирование", callback_data="continue_tests")]
        ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)

@router.callback_query(F.data == "continue_tests")
async def continue_to_test_menu(callback: CallbackQuery, state: FSMContext):
    """ИСПРАВЛЕННЫЙ обработчик продолжения к меню тестов"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "continue_tests")
    
    await show_test_menu(callback.message, state)

@router.callback_query(F.data == "back_to_tests")
async def back_to_test_selection(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору тестов"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "back_to_tests")
    
    await show_test_menu(callback.message, state)

# ============================================================================
# ФУНКЦИИ ПОДДЕРЖКИ
# ============================================================================

def get_risk_emoji(risk_level: str) -> str:
    """Получить эмодзи для уровня риска"""
    risk_emojis = {
        "Низкий": "🟢",
        "Умеренный": "🟡", 
        "Высокий": "🟠",
        "Очень высокий": "🔴"
    }
    return risk_emojis.get(risk_level, "⚪")

def get_risk_explanation(risk_level: str) -> str:
    """Получить объяснение уровня риска"""
    explanations = {
        "Низкий": "Отличная работа! Продолжайте в том же духе.",
        "Умеренный": "Есть возможности для улучшения здоровья.",
        "Высокий": "Рекомендуется обратить внимание на факторы риска.",
        "Очень высокий": "Важно принять меры для снижения рисков."
    }
    return explanations.get(risk_level, "Проконсультируйтесь с врачом.")

# ============================================================================
# ФУНКЦИИ ДЛЯ ОПРЕДЕЛЕНИЯ УРОВНЕЙ ТЕСТОВ
# ============================================================================

def get_hads_anxiety_level(score: int) -> str:
    """Определить уровень тревоги HADS"""
    if score <= 7:
        return "норма"
    elif score <= 10:
        return "субклинический"
    else:
        return "клинический"

def get_hads_depression_level(score: int) -> str:
    """Определить уровень депрессии HADS"""
    if score <= 7:
        return "норма"
    elif score <= 10:
        return "субклинический"
    else:
        return "клинический"

def get_burns_level(score: int) -> str:
    """Определить уровень выгорания Бернса"""
    if score <= 25:
        return "низкий"
    elif score <= 50:
        return "умеренный"
    else:
        return "высокий"

def get_isi_level(score: int) -> str:
    """Определить уровень нарушений сна ISI"""
    if score <= 7:
        return "норма"
    elif score <= 14:
        return "легкие нарушения"
    elif score <= 21:
        return "умеренные нарушения"
    else:
        return "тяжелые нарушения"

def get_stop_bang_risk(score: int) -> str:
    """Определить риск апноэ STOP-BANG"""
    if score <= 2:
        return "низкий риск"
    elif score <= 4:
        return "средний риск"
    else:
        return "высокий риск"

def get_ess_level(score: int) -> str:
    """Определить уровень дневной сонливости ESS"""
    if score <= 10:
        return "норма"
    else:
        return "повышенная сонливость"

def get_fagerstrom_level(score: int) -> str:
    """Определить уровень никотиновой зависимости"""
    if score <= 2:
        return "очень слабая"
    elif score <= 4:
        return "слабая"
    elif score <= 6:
        return "средняя"
    elif score <= 8:
        return "сильная"
    else:
        return "очень сильная"

def get_audit_level(score: int) -> str:
    """Определить уровень проблем с алкоголем AUDIT"""
    if score <= 7:
        return "низкий риск"
    elif score <= 15:
        return "опасное потребление"
    elif score <= 19:
        return "вредное потребление"
    else:
        return "возможная зависимость"

def calculate_cv_risk_score(test_results: dict) -> int:
    """Рассчитать общий балл CV риска на основе тестов"""
    total_score = 0
    
    # HADS (тревога + депрессия)
    if test_results.get('hads_anxiety_score'):
        total_score += test_results['hads_anxiety_score']
    if test_results.get('hads_depression_score'):
        total_score += test_results['hads_depression_score']
    
    # Другие тесты - нормализуем к шкале 0-10
    if test_results.get('burns_score'):
        total_score += min(10, test_results['burns_score'] // 5)  # 0-50 -> 0-10
    
    if test_results.get('isi_score'):
        total_score += min(10, test_results['isi_score'] // 3)  # 0-28 -> 0-10
        
    if test_results.get('stop_bang_score'):
        total_score += test_results['stop_bang_score']  # 0-8
        
    if test_results.get('ess_score'):
        total_score += min(10, test_results['ess_score'] // 2)  # 0-24 -> 0-10
    
    return total_score

def calculate_risk_factors_count(test_results: dict) -> int:
    """Подсчитать количество факторов риска"""
    count = 0
    
    # Тревога (HADS)
    if test_results.get('hads_anxiety_score', 0) > 7:
        count += 1
        
    # Депрессия (HADS)  
    if test_results.get('hads_depression_score', 0) > 7:
        count += 1
        
    # Выгорание (Бернс)
    if test_results.get('burns_score', 0) > 25:
        count += 1
        
    # Нарушения сна (ISI)
    if test_results.get('isi_score', 0) > 7:
        count += 1
        
    # Апноэ сна (STOP-BANG)
    if test_results.get('stop_bang_score', 0) > 2:
        count += 1
        
    # Дневная сонливость (ESS)
    if test_results.get('ess_score', 0) > 10:
        count += 1
        
    # Курение (Фагерстрем)
    if test_results.get('fagerstrom_score', 0) > 0 and not test_results.get('fagerstrom_skipped'):
        count += 1
        
    # Алкоголь (AUDIT)
    if test_results.get('audit_score', 0) > 7 and not test_results.get('audit_skipped'):
        count += 1
    
    return count