import logging
from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardRemove
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext

from .base import UserStates, log_user_interaction
from database import safe_save_user_data

# Настройка логирования
logger = logging.getLogger(__name__)

router = Router()

# ============================================================================
# ОБРАБОТЧИКИ РЕГИСТРАЦИИ
# ============================================================================

@router.message(StateFilter(UserStates.waiting_name))
async def handle_name(message: Message, state: FSMContext):
    """Обработка имени пользователя с валидацией"""
    await log_user_interaction(message.from_user.id, "name_entered", message.text)
    
    name = message.text.strip()
    
    # Валидация имени
    if not name:
        await message.answer("❌ Пожалуйста, введите ваше имя для продолжения регистрации.")
        return
    
    if len(name) < 2:
        await message.answer("❌ Пожалуйста, введите ваше полное имя (минимум 2 символа).")
        return
    
    if len(name) > 50:
        await message.answer("❌ Имя слишком длинное. Пожалуйста, введите короткое имя.")
        return
    
    # Проверяем на спам и некорректные символы
    if any(char in name for char in '@#$%^&*()+=[]{}|;:,.<>?/~`'):
        await message.answer("❌ Пожалуйста, введите ваше настоящее имя без специальных символов.")
        return
    
    # Сохраняем имя в состоянии
    await state.update_data(name=name)
    logger.info(f"Имя сохранено для пользователя {message.from_user.id}: {name}")
    
    text = f"""2️⃣ Укажите, пожалуйста, ваш e-mail.

На него придет доступ к платформе, где будут храниться все необходимые материалы, которые мы добавим по завершении вебинара.

✍️ Введите ваш e-mail"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(UserStates.waiting_email)

@router.message(StateFilter(UserStates.waiting_email))
async def handle_email(message: Message, state: FSMContext):
    """Обработка email пользователя"""
    await log_user_interaction(message.from_user.id, "email_entered", message.text)
    
    email = message.text.strip()
    
    # Простая валидация email
    if "@" not in email or "." not in email:
        await message.answer("Пожалуйста, введите корректный email адрес.")
        return
    
    # Сохраняем email в состоянии
    await state.update_data(email=email)
    
    text = """3️⃣ И последний шаг — оставьте, пожалуйста, ваш номер телефона.

❗Он нужен не для звонков и рекламы, а чтобы убедиться, что вы — настоящий человек, а не бот. Также он поможет, если возникнут проблемы с доступом к вебинару и другим важным материалам.

Всё конфиденциально и в рамках этики врача. Обещаю — никаких звонков.

📱 Введите ваш номер телефона (например: +79991234567):"""

    await message.answer(text, parse_mode="HTML")
    await state.set_state(UserStates.waiting_phone)
    
@router.message(StateFilter(UserStates.waiting_phone))
async def handle_phone(message: Message, state: FSMContext):
    """Обработчик телефона - принимает ручной ввод или контакт"""

    REAL_USER_ID = message.from_user.id

    logger.info(f"Обработка телефона для пользователя {REAL_USER_ID}")

    await log_user_interaction(REAL_USER_ID, "phone_processing")

    # Получаем номер телефона из контакта или текста
    if message.contact:
        phone = message.contact.phone_number
        logger.info(f"Получен номер через контакт: {phone}")
    elif message.text:
        phone = message.text.strip()
        logger.info(f"Получен номер вручную: {phone}")

        # Базовая валидация номера телефона
        # Убираем все символы кроме цифр и +
        cleaned_phone = ''.join(char for char in phone if char.isdigit() or char == '+')

        # Проверяем что это похоже на номер телефона
        if len(cleaned_phone) < 10:
            await message.answer("❌ Пожалуйста, введите корректный номер телефона (минимум 10 цифр).\n\nНапример: +79991234567 или 89991234567")
            return

        phone = cleaned_phone
    else:
        await message.answer("❌ Пожалуйста, отправьте ваш номер телефона текстом.")
        return
    
    await message.answer("Данные получены! Обрабатываю...", reply_markup=ReplyKeyboardRemove())
    await state.update_data(phone=phone)
    
    # Получаем данные из состояния
    data = await state.get_data()
    name = data.get('name', f'Пользователь_{REAL_USER_ID}')
    email = data.get('email', f'user_{REAL_USER_ID}@bot.com')
    
    logger.info(f"🔍 ИСПРАВЛЕННЫЕ данные для сохранения:")
    logger.info(f"   telegram_id: {REAL_USER_ID}")
    logger.info(f"   name: {name}")
    logger.info(f"   email: {email}")
    logger.info(f"   phone: {phone}")
    
    try:
        # ИСПОЛЬЗУЕМ ИСПРАВЛЕННУЮ ФУНКЦИЮ
        save_result = await safe_save_user_data(
            telegram_id=REAL_USER_ID,  # ТОЛЬКО НАСТОЯЩИЙ ID
            name=name,
            email=email,
            phone=phone
        )
        
        logger.info(f"✅ ИСПРАВЛЕННЫЙ результат сохранения: {save_result}")
        
        if save_result:  # Если пользователь создан успешно
            success_message = "✅ Отлично! Регистрация завершена!"
        else:
            success_message = "✅ Данные получены! Продолжаем..."
        
        await message.answer(success_message)
        
    except Exception as e:
        logger.error(f"❌ ОШИБКА исправленного сохранения: {e}")
        await message.answer("✅ Данные получены! Продолжаем...")
    
    # Переход к тестам
    await start_tests(message, state)

# ============================================================================
# ПЕРЕХОД К ОПРОСУ
# ============================================================================

async def start_tests(message: Message, state: FSMContext):
    """Переход к тестам согласно recomendations.txt"""
    await log_user_interaction(message.from_user.id, "tests_started")

    # Сообщение о готовности к тестам
    text1 = """✅ Спасибо, полный доступ активирован!

Но сначала ― очень важный шаг, к которому мы просим отнестись серьёзно: без него вы не сможете получить максимум пользы от вебинара.
Сейчас расскажу 👇

Диагностика скрытых факторов риска, которые часто остаются вне фокуса, но напрямую влияют на здоровье сердца и сосудов.
На вебинаре эти тесты помогут более точно рассчитать ваш суммарный риск с учетом не только анализов, но и качества сна, уровня тревоги, депрессии, вредных привычек и др.
Пожалуйста, пройдите их до просмотра вебинара — так вы извлечете гораздо больше пользы и сможете применить полученные рекомендации к своему случаю.

👉 После этого я пришлю вам список базовых анализов"""

    await message.answer(text1, parse_mode="HTML")

    # Задержка 5 секунд
    import asyncio
    await asyncio.sleep(5)

    # Начинаем цепочку тестов (опросников)
    text2 = """<b>Цепочка тестов:</b>

Пожалуйста, пройдите психологические тесты для оценки факторов риска.

Это займет 10-15 минут, но поможет вам получить персональные рекомендации.

Выберите тест из списка ниже:"""

    from ui.keyboards import get_test_selection_keyboard
    # Получаем текущие данные из состояния
    current_data = await state.get_data()
    keyboard = get_test_selection_keyboard(current_data)

    await message.answer(text2, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(UserStates.test_selection)