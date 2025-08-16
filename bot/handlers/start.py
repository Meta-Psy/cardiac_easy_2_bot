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
# КОМАНДЫ БОТА (С ЗАЩИТОЙ)
# ============================================================================

@router.message(Command("help"))
async def help_command(message: Message, state: FSMContext):
    """Команда помощи"""
    await log_user_interaction(message.from_user.id, "help_requested")
    
    # Проверяем статус пользователя
    user_completed = check_user_completed(message.from_user.id)
    current_state = await state.get_state()
    
    if user_completed:
        # Пользователь уже завершил диагностику
        text = """❓ <b>ПОМОЩЬ - Диагностика завершена</b>

✅ Поздравляем! Вы успешно прошли полную диагностику.

🗓 <b>Что дальше:</b>
• Вебинар "Умный кардиочекап": 3 августа в 12:00 МСК
• Ссылка на эфир появится здесь за час до начала
• Подготовьте результаты анализов (если есть)

📋 <b>Доступные команды:</b>
/start - Посмотреть результаты диагностики
/status - Проверить статус
/score - SCORE2 калькулятор сердечно-сосудистого риска
/restart - Пройти диагностику заново

💡 <b>Нужна помощь?</b>
Просто напишите ваш вопрос, и мы постараемся помочь."""

    elif current_state and ("survey" in current_state or "test" in current_state):
        # Пользователь в процессе диагностики
        text = """❓ <b>ПОМОЩЬ - Диагностика в процессе</b>

📝 Вы сейчас проходите диагностику. Это важно для получения максимальной пользы от вебинара!

🔄 <b>Что делать:</b>
• Продолжайте отвечать на вопросы
• Каждый ответ важен для точной оценки
• Процесс займет 10-15 минут

⚠️ <b>Если что-то пошло не так:</b>
/restart - Начать диагностику заново
/status - Посмотреть прогресс

💡 <b>Не можете ответить на вопрос?</b>
Выберите наиболее подходящий вариант или тот, который ближе всего к вашей ситуации."""

    else:
        # Пользователь еще не начал или прервал диагностику
        text = """❓ <b>ПОМОЩЬ - Добро пожаловать!</b>

🫀 Этот бот поможет вам подготовиться к вебинару <b>"Умный кардиочекап"</b> с врачами-кардиологами.

🎯 <b>Что вы получите:</b>
• Полную диагностику состояния сердечно-сосудистой системы
• Оценку рисков и факторов
• Персональные рекомендации на вебинаре
• Бонусные материалы и список анализов

🚀 <b>Как начать:</b>
1. Нажмите /start
2. Введите контактные данные (имя, email, телефон)
3. Пройдите опрос (18 вопросов, 5-7 минут)
4. Выполните психологические тесты (7 тестов, 10-15 минут)
5. Получите результаты и материалы

📋 <b>Команды:</b>
/start - Начать диагностику
/score - SCORE2 калькулятор сердечно-сосудистого риска
/restart - Начать заново"""

    await message.answer(text, parse_mode="HTML")

@router.message(Command("status"))
async def status_command(message: Message, state: FSMContext):
    """Команда проверки статуса"""
    await log_user_interaction(message.from_user.id, "status_requested")
    
    try:
        # Получаем данные пользователя
        data = get_user_data(message.from_user.id)
        user = data.get('user')
        survey = data.get('survey') 
        tests = data.get('tests')
        
        if not user:
            # Пользователь не зарегистрирован
            text = """📊 <b>ВАШ СТАТУС</b>

❌ Вы еще не начали диагностику

🚀 <b>Что нужно сделать:</b>
1. Нажмите /start для начала
2. Заполните контактные данные
3. Пройдите опрос и тесты

💡 Это займет всего 15-20 минут, но поможет получить максимум пользы от вебинара!"""
            
        else:
            # Формируем статус
            text = f"""📊 <b>ВАШ СТАТУС ДИАГНОСТИКИ</b>

👤 <b>Пользователь:</b> {user.name or 'Не указано'}
📧 Email: {user.email or 'Не указан'}
📱 Телефон: {user.phone or 'Не указан'}

<b>ПРОГРЕСС:</b>"""
            
            # Регистрация
            if user.registration_completed:
                text += "\n✅ Регистрация завершена"
            else:
                text += "\n❌ Регистрация не завершена"
            
            # Опрос
            if user.survey_completed:
                text += "\n✅ Опрос пройден (18/18 вопросов)"
                if survey and survey.age:
                    text += f"\n   • Возраст: {survey.age} лет, пол: {survey.gender or 'не указан'}"
            else:
                text += "\n❌ Опрос не пройден (0/18 вопросов)"
            
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"Ошибка в status_command для пользователя {message.from_user.id}: {e}")
        await message.answer(f"❌ Ошибка получения статуса. Попробуйте /start")

@router.message(Command("restart"))
async def restart_command(message: Message, state: FSMContext):
    """Команда перезапуска диагностики"""
    await log_user_interaction(message.from_user.id, "restart_requested")
    
    text = """🔄 <b>ПЕРЕЗАПУСК ДИАГНОСТИКИ</b>

⚠️ Вы уверены, что хотите начать диагностику заново?

❗ <b>Внимание:</b>
• Все ваши текущие ответы будут удалены
• Вам нужно будет снова пройти весь процесс
• Это займет 15-20 минут

✅ Если вы готовы, нажмите кнопку ниже:"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Да, начать заново", callback_data="confirm_restart")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_restart")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

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

🗓 <b>Вебинар "Умный Кардиочекап":</b>
📅 3 августа в 12:00 МСК
📍 Ссылка на эфир появится здесь за час до начала

💡 <b>Что вы можете сделать:</b>
• Подготовить результаты анализов (если есть)
• Приготовить блокнот для записей
• Пригласить близких к просмотру

До встречи на вебинаре! 💪"""
        
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

❣️ Помогу вам подготовиться к вебинару, пройти мини-диагностику, получить бонусы, список анализов, нужные ссылки и материалы.

👉 Нажмите «Старт», чтобы включить меня и начать подготовку к самому важному вебинару этого лета."""
    
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
    text1 = """👋 Здравствуйте!
Вы зарегистрированы на вебинар «Умный Кардиочекап» с врачами-кардиологами Дианой Новиковой и Еленой Удачкиной.

Вы получите пошаговый алгоритм оценки риска развития сердечно-сосудистых заболеваний и их осложнений (в т.ч. инфаркт, инсульт, хроническая сердечная недостаточность), что позволит вовремя принять меры, сохранив здоровье и активность на годы вперёд без лишних затрат и избыточных обследований.

🗓 Дата и время проведения: 3 августа в 12:00 по МСК (запись будет)

📍 Здесь, в боте, вы получите:
 — ссылку на вебинар и его запись
 — список базовых анализов
 — чек-листы
 — бонусные материалы от врачей
Мы уже всё приготовили 👌 Но сначала ― очень важный шаг, к которому мы просим отнестись серьёзно: без него вы не сможете получить максимум пользы от вебинара.


Сейчас расскажу 👇"""
    
    await safe_edit_message(callback.message, text1)
    
    # Задержка 15 секунд
    import asyncio
    await asyncio.sleep(15)
    
    # Начинаем сбор контактных данных
    text2 = """‼️ Небольшой организационный момент

Чтобы вы получили всё без сбоев:
✔️ ссылку на вебинар и запись
✔️ список анализов и бонусные материалы
✔️ напоминания и доступ к платформе

давайте с вами познакомимся 🤝

Мне важно обращаться к вам по имени — так общение становится теплее и человечнее.

1️⃣ Напишите, пожалуйста, как к вам обращаться.

✍️ Введите ваше имя"""
    
    await callback.message.answer(text2, parse_mode="HTML")
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

3️⃣ Поделитесь вашим номером телефона.

📱 Нажмите кнопку ниже, чтобы поделиться номером телефона:"""
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поделиться номером телефона", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await safe_edit_message(callback.message, text)
        await callback.message.answer("👆 Используйте кнопку выше", reply_markup=keyboard)
        
    elif "survey" in current_state:
        text = """📝 <b>Продолжаем опрос</b>

Вы проходили опрос о вашем здоровье. Продолжайте отвечать на вопросы."""
        await safe_edit_message(callback.message, text)
        
    elif "test" in current_state:
        text = """🧪 <b>Продолжаем тестирование</b>

Вы проходили психологические тесты. Продолжайте тестирование."""
        await safe_edit_message(callback.message, text)
        
    else:
        await safe_edit_message(callback.message, "Продолжаем с того места, где остановились...")

@router.callback_query(F.data == "restart_from_beginning")
async def restart_from_beginning(callback: CallbackQuery, state: FSMContext):
    """Начать заново с самого начала"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "restart_from_beginning")
    
    # Полностью очищаем состояние
    await state.clear()
    
    text = """🔄 <b>ДИАГНОСТИКА ПЕРЕЗАПУЩЕНА</b>

Начинаем с самого начала!

🤖 Приветствую! Я — бот-помощник Дианы Новиковой и Елены Удачкиной, авторов вебинара <b>«Умный кардиочекап»</b>.

👉 Нажмите <b>«Старт»</b>, чтобы начать подготовку к вебинару."""
    
    keyboard = get_start_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.waiting_start)

@router.callback_query(F.data == "show_status")
async def show_status_callback(callback: CallbackQuery, state: FSMContext):
    """Показать статус через callback"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "show_status_callback")
    
    try:
        data = get_user_data(callback.from_user.id)
        user = data.get('user')
        
        if not user:
            text = """📊 <b>ВАШ СТАТУС</b>

❌ Регистрация не начата

🚀 Нажмите "Начать заново" для начала диагностики."""
        else:
            text = f"""📊 <b>ВАШ СТАТУС</b>

👤 Имя: {user.name or 'Не указано'}
📧 Email: {user.email or 'Не указан'}
📱 Телефон: {user.phone or 'Не указан'}

<b>ПРОГРЕСС:</b>
{'✅' if user.registration_completed else '❌'} Регистрация
{'✅' if user.survey_completed else '❌'} Опрос
{'✅' if user.tests_completed else '❌'} Тесты
{'✅' if user.completed_diagnostic else '❌'} Диагностика"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="▶️ Продолжить", callback_data="continue_current")],
            [InlineKeyboardButton(text="🔄 Начать заново", callback_data="restart_from_beginning")]
        ])
        
        await safe_edit_message(callback.message, text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка в show_status_callback для пользователя {callback.from_user.id}: {e}")
        await safe_edit_message(callback.message, "❌ Ошибка получения статуса")

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

🗓 <b>Вебинар "Умный Кардиочекап":</b>
📅 3 августа в 12:00 МСК
📍 Ссылка появится здесь за час до начала

💡 Детальные результаты будут разобраны на вебинаре с врачами-кардиологами."""
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
    
    text = """📋 <b>МАТЕРИАЛЫ К ВЕБИНАРУ</b>

🗓 <b>Вебинар "Умный Кардиочекап"</b>
📅 3 августа в 12:00 МСК

🎁 <b>Бонусные материалы:</b>
• Чек-лист подготовки к кардиочекапу
• Список анализов для проверки сердца
• Калькулятор сердечно-сосудистого риска SCORE2
• Рекомендации по профилактике

💊 <b>Что подготовить к вебинару:</b>
• Результаты последних анализов (если есть)
• Блокнот для записей
• Вопросы врачам

📍 Ссылка на эфир появится здесь за час до начала."""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к результатам", callback_data="show_full_results")]
    ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)

@router.callback_query(F.data == "confirm_restart")
async def confirm_restart(callback: CallbackQuery, state: FSMContext):
    """Подтверждение перезапуска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "confirm_restart")
    
    # Полностью очищаем состояние
    await state.clear()
    
    text = """🔄 <b>ДИАГНОСТИКА ПЕРЕЗАПУЩЕНА</b>

Все данные удалены. Начинаем заново!

🤖 Приветствую! Я — бот-помощник Дианы Новиковой и Елены Удачкиной, авторов вебинара <b>«Умный кардиочекап»</b>.

👉 Нажмите <b>«Старт»</b>, чтобы начать подготовку к вебинару."""
    
    keyboard = get_start_keyboard()
    await safe_edit_message(callback.message, text, reply_markup=keyboard)
    await state.set_state(UserStates.waiting_start)

@router.callback_query(F.data == "cancel_restart")
async def cancel_restart(callback: CallbackQuery, state: FSMContext):
    """Отмена перезапуска"""
    await safe_answer_callback(callback)
    await log_user_interaction(callback.from_user.id, "cancel_restart")
    
    text = """❌ <b>ПЕРЕЗАПУСК ОТМЕНЕН</b>

Ваши данные сохранены. Продолжайте с того места, где остановились.

📊 Используйте /status для проверки текущего прогресса."""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Продолжить диагностику", callback_data="continue_current")],
        [InlineKeyboardButton(text="📊 Посмотреть статус", callback_data="show_status")]
    ])
    
    await safe_edit_message(callback.message, text, reply_markup=keyboard)

# Callback для "start_bot" удален - теперь переход прямой

# ============================================================================
# ОБРАБОТЧИК НЕИЗВЕСТНЫХ СООБЩЕНИЙ
# ============================================================================

# УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК ПЕРЕНЕСЕН В ОТДЕЛЬНЫЙ МОДУЛЬ
# Чтобы не перехватывать сообщения регистрации, опросов и тестов