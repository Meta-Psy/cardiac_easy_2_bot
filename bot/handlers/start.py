import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart, StateFilter, Command
from aiogram.fsm.context import FSMContext

from .base import UserStates, safe_edit_message, safe_answer_callback, log_user_interaction
from ui.keyboards import get_start_keyboard
from database import get_user_data, check_user_completed

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# КОМАНДЫ БОТА (УПРОЩЕННЫЕ)
# ============================================================================

# ============================================================================
# ЗАЩИЩЕННЫЙ ОБРАБОТЧИК /start
# ============================================================================

@router.message(CommandStart())
async def start_command_protected(message: Message, state: FSMContext):
    """Защищенный обработчик команды /start"""
    await log_user_interaction(message.from_user.id, "start_command")
    
    # Получаем текущее состояние
    current_state = await state.get_state()
    
    # Проверяем, завершил ли пользователь диагностику
    user_completed = check_user_completed(message.from_user.id)
    
    if user_completed:
        # Пользователь уже завершил диагностику
        await show_completed_user_info(message, state)
        return
    
    # Проверяем, в каком состоянии находится пользователь
    if current_state:
        await handle_start_during_process(message, state, current_state)
    else:
        # Пользователь не в процессе - показываем стартовое сообщение
        await show_start_message(message, state)

async def handle_start_during_process(message: Message, state: FSMContext, current_state: str):
    """Обработка /start во время процесса диагностики"""
    
    # Определяем, на каком этапе пользователь
    if "waiting_name" in current_state or "waiting_email" in current_state or "waiting_phone" in current_state:
        stage = "регистрации"
        current_step = "заполнение контактных данных"
    elif "survey" in current_state:
        stage = "опроса"
        current_step = "ответы на вопросы о здоровье"
    elif "test" in current_state:
        stage = "тестирования"
        current_step = "психологические тесты"
    else:
        stage = "диагностики"
        current_step = "неизвестный этап"
    
    text = f"""🔄 <b>ВЫ УЖЕ В ПРОЦЕССЕ ДИАГНОСТИКИ</b>

⏳ Сейчас вы проходите этап: <b>{stage}</b>
📍 Текущий шаг: {current_step}

❓ <b>Что вы хотите сделать?</b>"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить с текущего места", callback_data="continue_current")],
        [InlineKeyboardButton(text="🔄 Начать заново (удалить прогресс)", callback_data="restart_from_beginning")],
        [InlineKeyboardButton(text="📊 Посмотреть мой статус", callback_data="show_status")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

async def show_completed_user_info(message: Message, state: FSMContext):
    """Показать информацию для завершившего диагностику пользователя"""
    
    try:
        data = get_user_data(message.from_user.id)
        user = data.get('user')
        tests = data.get('tests')
        
        name = user.name if user else "Пользователь"
        risk_level = tests.overall_cv_risk_level if tests else "не определен"
        
        text = f"""🎉 <b>Добро пожаловать, {name}!</b>

✅ Вы уже завершили диагностику!
🎯 Ваш сердечно-сосудистый риск: <b>{risk_level}</b>

🎥 <b>Вебинар "Умный Кардиочекап":</b>
📅 Состоялся 3 августа 2025 года
📺 Запись: https://novikova-diana.ru/kardiochekup_record

💡 <b>Что вы можете сделать:</b>
• Посмотреть запись вебинара
• Изучить ваши результаты диагностики
• Применить рекомендации на практике
• Использовать SCORE2 калькулятор (/score)

Берегите здоровье! 💪"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Посмотреть полные результаты", callback_data="show_full_results")],
            [InlineKeyboardButton(text="🔄 Пройти диагностику заново", callback_data="restart_from_beginning")],
            [InlineKeyboardButton(text="📋 Материалы к вебинару", callback_data="show_materials")]
        ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в show_completed_user_info для пользователя {message.from_user.id}: {e}")
        await message.answer("❌ Ошибка получения данных. Попробуйте /status для проверки статуса.")

async def show_start_message(message: Message, state: FSMContext):
    """НОВОЕ стартовое приветствие согласно ТЗ"""
    
    # Сообщение 0: вводное - что умеет этот бот
    text0 = """🤖 Приветствую! Я — бот-помощник Дианы Новиковой и Елены Удачкиной, авторов вебинара «Умный кардиочекап».

❣️ Помогу вам подготовиться к просмотру вебинара: пришлю необходимую диагностику, а также выдам вам предварительный список анализов.

 👉 Нажмите «Старт», чтобы включить меня и начать подготовку."""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Старт", callback_data="start_welcome_flow")]
    ])
    
    await message.answer(text0, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(UserStates.waiting_start)

# ============================================================================
# НОВЫЙ WELCOME FLOW СОГЛАСНО ТЗ
# ============================================================================

@router.callback_query(F.data == "start_welcome_flow")
async def start_welcome_flow(callback: CallbackQuery, state: FSMContext):
    """Запуск нового welcome flow"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "start_welcome_flow")
    
    # Сообщение 1: Приветствие + ценность + подводка к следующему шагу
    text1 = """На вебинаре «Умный Кардиочекап» вы получите пошаговый алгоритм оценки риска развития сердечно-сосудистых заболеваний и их осложнений (в т.ч. инфаркт, инсульт, хроническая сердечная недостаточность), что позволит вовремя принять меры, сохранив здоровье и активность на годы вперёд без лишних затрат и избыточных обследований.

‼️ Небольшой организационный момент

Чтобы вы получили всё без сбоев:
✔️ ссылку на за запись вебинара и запись
✔️ список анализов и подготовительные материалы

давайте с вами познакомимся 🤝

Мне важно обращаться к вам по имени — так общение становится теплее и человечнее.

1️⃣ Напишите, пожалуйста, как к вам обращаться.

✍️ Введите ваше имя"""
    
    await safe_edit_message(callback.message, text1)
    await state.set_state(UserStates.waiting_name)

# ============================================================================
# ОБРАБОТЧИКИ CALLBACK'ОВ ДЛЯ ЗАЩИТЫ СОСТОЯНИЙ
# ============================================================================

@router.callback_query(F.data == "continue_current")
async def continue_current_process(callback: CallbackQuery, state: FSMContext):
    """Продолжить с текущего места"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "continue_current")

    current_state = await state.get_state()

    if "waiting_name" in current_state:
        text = """✍️ <b>Продолжаем регистрацию</b>

1️⃣ Напишите, пожалуйста, как к вам обращаться.

✍️ Введите ваше имя"""
        await safe_edit_message(callback.message, text)

    elif "waiting_email" in current_state:
        text = """✍️ <b>Продолжаем регистрацию</b>

2️⃣ Укажите, пожалуйста, ваш e-mail.

✍️ Введите ваш e-mail"""
        await safe_edit_message(callback.message, text)

    elif "waiting_phone" in current_state:
        text = """✍️ <b>Продолжаем регистрацию</b>

3️⃣ И последний шаг — оставьте, пожалуйста, ваш номер телефона.

📱 Введите ваш номер телефона (например: +79991234567):"""
        await safe_edit_message(callback.message, text)

    elif "survey" in current_state:
        # Для опроса просто показываем сообщение - следующее действие продолжит опрос
        text = """📝 <b>Продолжаем опрос</b>

Пожалуйста, ответьте на текущий вопрос, чтобы продолжить."""
        await safe_edit_message(callback.message, text)

    elif "test" in current_state:
        # Показываем меню тестов или текущий вопрос
        from .tests import show_test_menu
        from handlers.tests import show_current_question

        data = await state.get_data()
        current_test = data.get('current_test')

        if current_test:
            # Если идет тест - показываем текущий вопрос
            await safe_edit_message(callback.message, "🧪 <b>Продолжаем тестирование</b>")
            await show_current_question(callback.message, state)
        else:
            # Если в меню выбора тестов
            await show_test_menu(callback.message, state)

    else:
        await safe_edit_message(callback.message, "Продолжаем с того места, где остановились...")


@router.callback_query(F.data == "restart_from_beginning")
async def restart_from_beginning(callback: CallbackQuery, state: FSMContext):
    """Перезапуск диагностики с начала"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "restart_from_beginning")

    # Очищаем состояние
    await state.clear()

    text = """🔄 <b>ПРОГРЕСС СБРОШЕН</b>

Все данные удалены. Начинаем сначала!

Нажмите кнопку ниже, чтобы начать заново:"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Начать заново", callback_data="start_welcome_flow")]
    ])

    await safe_edit_message(callback.message, text, reply_markup=keyboard)


@router.callback_query(F.data == "show_status")
async def show_status_callback(callback: CallbackQuery, state: FSMContext):
    """Показать текущий статус пользователя"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_status")

    current_state = await state.get_state()
    data = await state.get_data()

    # Проверяем завершение
    user_completed = check_user_completed(callback.from_user.id)

    if user_completed:
        status_text = "✅ Завершено"
        stage_text = "Диагностика пройдена"
    elif not current_state:
        status_text = "⚪ Не начато"
        stage_text = "Вы еще не начали диагностику"
    elif "waiting_name" in current_state:
        status_text = "🟡 В процессе"
        stage_text = "Регистрация: ввод имени"
    elif "waiting_email" in current_state:
        status_text = "🟡 В процессе"
        stage_text = "Регистрация: ввод email"
    elif "waiting_phone" in current_state:
        status_text = "🟡 В процессе"
        stage_text = "Регистрация: ввод телефона"
    elif "survey" in current_state:
        status_text = "🟡 В процессе"
        stage_text = "Опрос о здоровье"
    elif "test" in current_state:
        status_text = "🟡 В процессе"
        completed_tests = []
        if data.get('completed_hads'):
            completed_tests.append("HADS")
        if data.get('completed_burns'):
            completed_tests.append("Burns")
        if data.get('completed_isi'):
            completed_tests.append("ISI")
        if data.get('completed_stop_bang'):
            completed_tests.append("STOP-BANG")
        if data.get('completed_ess'):
            completed_tests.append("ESS")

        stage_text = f"Психологические тесты ({len(completed_tests)}/7 завершено)"
    else:
        status_text = "🟡 В процессе"
        stage_text = "Неизвестный этап"

    text = f"""📊 <b>ВАШ СТАТУС</b>

🎯 <b>Статус:</b> {status_text}
📍 <b>Этап:</b> {stage_text}

❓ <b>Что дальше?</b>"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить", callback_data="continue_current")],
        [InlineKeyboardButton(text="🔄 Начать заново", callback_data="restart_from_beginning")]
    ])

    await safe_edit_message(callback.message, text, reply_markup=keyboard)



@router.callback_query(F.data == "show_full_results")
async def show_full_results(callback: CallbackQuery, state: FSMContext):
    """Показать полные результаты диагностики"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_full_results")
    
    try:
        # Временная заглушка - используем простую версию results summary
        data = get_user_data(callback.from_user.id)
        
        if data and data.get('user'):
            user = data['user']
            tests = data.get('tests')
            name = user.name or "Пользователь"
            risk_level = tests.overall_cv_risk_level if tests else "не определен"
            
            summary = f"""🎉 <b>РЕЗУЛЬТАТЫ ДИАГНОСТИКИ</b>

👤 <b>Участник:</b> {name}
🎯 <b>Сердечно-сосудистый риск:</b> {risk_level}

✅ <b>Диагностика завершена!</b>

🎥 <b>Вебинар "Умный Кардиочекап":</b>
📅 Состоялся 3 августа 2025 года
📺 Запись: https://novikova-diana.ru/kardiochekup_record

💡 Детальные результаты разобраны в записи вебинара с врачами-кардиологами."""
        else:
            summary = """❌ <b>ОШИБКА ПОЛУЧЕНИЯ РЕЗУЛЬТАТОВ</b>

Попробуйте /status для проверки данных или обратитесь к администратору."""
        
        await safe_edit_message(callback.message, summary)
        
    except Exception as e:
        logger.error(f"Ошибка в show_full_results для пользователя {callback.from_user.id}: {e}")
        await safe_edit_message(callback.message, "❌ Ошибка получения результатов")

@router.callback_query(F.data == "show_materials")
async def show_materials_callback(callback: CallbackQuery, state: FSMContext):
    """Показать материалы к вебинару"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_materials")
    
    text = """📋 <b>МАТЕРИАЛЫ ВЕБИНАРА</b>

🎥 <b>Вебинар "Умный Кардиочекап"</b>
📅 Состоялся 3 августа 2025 года
📺 Запись: https://novikova-diana.ru/kardiochekup_record

🎁 <b>Доступные материалы:</b>
• Запись полного вебинара с врачами-кардиологами
• Чек-лист подготовки к кардиочекапу  
• Список анализов для проверки сердца
• Калькулятор сердечно-сосудистого риска SCORE2 (/score)
• Персональные результаты диагностики
• Рекомендации по профилактике

💡 <b>Как использовать:</b>
• Изучите запись вебинара
• Ознакомьтесь с вашими результатами
• Следуйте персональным рекомендациям
• Регулярно отслеживайте показатели"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к результатам", callback_data="show_full_results")]
    ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)


# Callback для "start_bot" удален - теперь переход прямой

# ============================================================================
# ОБРАБОТЧИК НЕИЗВЕСТНЫХ СООБЩЕНИЙ
# ============================================================================

# УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ПЕРЕНЕСЕН В ОТДЕЛЬНЫЙ МОДУЛЬ
# Чтобы не перехватывать сообщения регистрации, опросов и тестов