import os
import json
import asyncio
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv

load_dotenv()
admin_router = Router()

class AdminStates(StatesGroup):
    waiting_password = State()
    waiting_broadcast_text = State()
    waiting_manual_ids = State()
    waiting_import_file = State()

# Получаем пароль из переменных окружения
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

def get_admin_keyboard():
    """Главная клавиатура админки"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📥 Экспорт данных", callback_data="admin_export")],
        [InlineKeyboardButton(text="📤 РАССЫЛКИ", callback_data="admin_broadcast_menu")],
        [InlineKeyboardButton(text="💾 Импорт БД", callback_data="admin_import_menu")],
        [InlineKeyboardButton(text="🗑 Очистить старые данные", callback_data="admin_clean")],
        [InlineKeyboardButton(text="🔍 Диагностика ID", callback_data="admin_id_diagnostic")],
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_logout")]
    ])
    return keyboard

def get_broadcast_menu():
    """Меню рассылок"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Рассылка по БД", callback_data="broadcast_from_db")],
        [InlineKeyboardButton(text="📋 Рассылка по ID", callback_data="broadcast_manual_ids")],
        [InlineKeyboardButton(text="🧪 Тестовая рассылка", callback_data="broadcast_test")],
        [InlineKeyboardButton(text="🫀 РАССЫЛКА ВЕБИНАРА (ТОЧКА 2)", callback_data="broadcast_webinar")],
        [InlineKeyboardButton(text="📅 РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)", callback_data="broadcast_followup")],
        [InlineKeyboardButton(text="🔄 НАПОМИНАНИЕ (не прошли ТОЧКА 3)", callback_data="broadcast_followup_reminder")],
        [InlineKeyboardButton(text="💙 ИЗВИНЕНИЯ (прошли ТОЧКА 3, без бонуса)", callback_data="broadcast_apology")],
        [InlineKeyboardButton(text="📕 МЕТОДИЧКА (прошли ТОЧКА 3)", callback_data="broadcast_methodichka")],
        [InlineKeyboardButton(text="🔔 МЯГКОЕ НАПОМИНАНИЕ (ТОЧКА 3)", callback_data="broadcast_soft_reminder")],
        [InlineKeyboardButton(text="📊 История рассылок", callback_data="broadcast_history")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
    ])
    return keyboard

def get_db_filter_menu():
    """Меню фильтров для рассылки из БД"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Всем пользователям", callback_data="db_filter_all")],
        [InlineKeyboardButton(text="✅ Завершившим диагностику", callback_data="db_filter_completed")],
        [InlineKeyboardButton(text="⏳ Не завершившим", callback_data="db_filter_uncompleted")],
        [InlineKeyboardButton(text="📝 Прошедшим опрос", callback_data="db_filter_survey")],
        [InlineKeyboardButton(text="🧪 Прошедшим тесты", callback_data="db_filter_tests")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_broadcast_menu")]
    ])
    return keyboard

# =========================== ОСНОВНАЯ АДМИНКА ===========================

@admin_router.message(Command("admin"))
async def admin_panel(message: Message, state: FSMContext, is_admin: bool = False):
    """Административная панель"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    admin_session = await state.get_data()
    if admin_session.get('admin_authenticated'):
        await show_admin_panel(message)
    else:
        await request_admin_password(message, state)

async def request_admin_password(message: Message, state: FSMContext):
    """Запрос пароля"""
    text = """🔐 <b>Доступ к административной панели</b>

Введите пароль администратора:"""
    
    await message.answer(text, parse_mode="HTML")
    await state.set_state(AdminStates.waiting_password)

@admin_router.message(AdminStates.waiting_password)
async def handle_admin_password(message: Message, state: FSMContext, is_admin: bool = False):
    """Обработка пароля"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        await state.clear()
        return
    
    password = message.text.strip()
    
    try:
        await message.delete()
    except:
        pass
    
    if str(password) == str(ADMIN_PASSWORD):
        await state.update_data(admin_authenticated=True)
        text = "✅ Доступ разрешен!"
        sent_message = await message.answer(text)
        await asyncio.sleep(1)
        await show_admin_panel(sent_message)
    else:
        await message.answer("❌ Неверный пароль. Попробуйте снова.")

async def show_admin_panel(message: Message):
    """Главная панель админки"""
    text = """🔧 <b>Административная панель v2.0</b>

📊 <b>Статистика</b> - просмотр данных пользователей
📥 <b>Экспорт данных</b> - выгрузка БД в Excel
📤 <b>РАССЫЛКИ</b> - система массовых рассылок
💾 <b>Импорт БД</b> - восстановление из Excel
🗑 <b>Очистка</b> - удаление старых данных
🚪 <b>Выйти</b> - завершение сессии"""
    
    keyboard = get_admin_keyboard()
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

# =========================== СИСТЕМА РАССЫЛОК ===========================

@admin_router.callback_query(F.data == "admin_broadcast_menu")
async def broadcast_menu(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Меню рассылок"""
    from database.analytics import admin_get_stats
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    # Получаем статистику для информации
    try:
        stats = await admin_get_stats()
        
        text = f"""📤 <b>СИСТЕМА РАССЫЛОК</b>

📊 <b>Доступно пользователей:</b>
• Всего: {stats['total_users']}
• Завершили диагностику: {stats['completed_diagnostic']}
• Не завершили: {stats['total_users'] - stats['completed_diagnostic']}
• Прошли опрос: {stats['completed_surveys']}
• Прошли тесты: {stats['completed_tests']}

<b>Выберите тип рассылки:</b>

📤 <b>По БД</b> - рассылка пользователям из базы
📋 <b>По ID</b> - рассылка конкретным ID через запятую
🧪 <b>Тест</b> - отправка только админам
📊 <b>История</b> - логи предыдущих рассылок"""
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_broadcast_menu())
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка загрузки меню: {e}")

@admin_router.callback_query(F.data == "broadcast_from_db")
async def broadcast_db_filter(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Выбор фильтра для рассылки из БД"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    text = """📤 <b>РАССЫЛКА ПО БАЗЕ ДАННЫХ</b>

Выберите целевую аудиторию:

👥 <b>Всем</b> - всем зарегистрированным пользователям
✅ <b>Завершившим</b> - только завершившим диагностику
⏳ <b>Не завершившим</b> - не завершившим диагностику
📝 <b>Прошедшим опрос</b> - только прошедшим опрос
🧪 <b>Прошедшим тесты</b> - только прошедшим тесты"""
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_db_filter_menu())

@admin_router.callback_query(F.data.startswith("db_filter_"))
async def request_broadcast_text(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Запрос текста рассылки"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    filter_type = callback.data.replace("db_filter_", "")
    await state.update_data(broadcast_filter=filter_type)
    
    filter_names = {
        "all": "всем пользователям",
        "completed": "завершившим диагностику",
        "uncompleted": "не завершившим диагностику", 
        "survey": "прошедшим опрос",
        "tests": "прошедшим тесты"
    }
    
    text = f"""✍️ <b>ТЕКСТ РАССЫЛКИ</b>

<b>Целевая аудитория:</b> {filter_names.get(filter_type, filter_type)}

Теперь напишите текст сообщения, которое будет отправлено пользователям.

<b>Поддерживается Markdown разметка:</b>
• **жирный текст**
• *курсив*
• [ссылка](https://example.com)
• `код`

Отправьте текст сообщения:"""
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.set_state(AdminStates.waiting_broadcast_text)

@admin_router.callback_query(F.data == "broadcast_manual_ids")
async def request_manual_ids(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Запрос ручного ввода ID"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    text = """📋 <b>РАССЫЛКА ПО ID</b>

Введите ID пользователей через запятую.

<b>Пример:</b>
123456789, 987654321, 555444333

<b>Формат:</b>
• ID разделяются запятыми
• Пробелы игнорируются
• Некорректные ID будут пропущены

Введите список ID:"""
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.update_data(broadcast_filter="manual")
    await state.set_state(AdminStates.waiting_manual_ids)

@admin_router.message(AdminStates.waiting_manual_ids)
async def handle_manual_ids(message: Message, state: FSMContext, is_admin: bool = False):
    """Обработка ручного ввода ID"""
    if not is_admin:
        await message.answer("❌ Нет прав доступа.")
        await state.clear()
        return
    
    ids_text = message.text.strip()
    
    # Парсим ID
    try:
        id_list = []
        for id_str in ids_text.split(','):
            id_str = id_str.strip()
            if id_str.isdigit():
                id_list.append(int(id_str))
        
        if not id_list:
            await message.answer("❌ Некорректные ID. Попробуйте снова.")
            return
        
        await state.update_data(manual_ids=id_list)
        
        text = f"""✅ <b>ID приняты</b>

<b>Список получателей ({len(id_list)} ID):</b>
{', '.join(map(str, id_list[:10]))}{'...' if len(id_list) > 10 else ''}

Теперь введите текст сообщения:"""
        
        await message.answer(text, parse_mode="HTML")
        await state.set_state(AdminStates.waiting_broadcast_text)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка обработки ID: {e}")

@admin_router.message(AdminStates.waiting_broadcast_text)
async def handle_broadcast_text(message: Message, state: FSMContext, is_admin: bool = False):
    """Обработка текста рассылки"""
    if not is_admin:
        await message.answer("❌ Нет прав доступа.")
        await state.clear()
        return
    
    broadcast_text = message.text
    state_data = await state.get_data()
    
    filter_type = state_data.get('broadcast_filter')
    manual_ids = state_data.get('manual_ids', [])
    
    # Показываем превью
    if filter_type == "manual":
        target_info = f"Ручной список ({len(manual_ids)} ID)"
    else:
        target_info = {
            "all": "Всем пользователям",
            "completed": "Завершившим диагностику",
            "uncompleted": "Не завершившим диагностику",
            "survey": "Прошедшим опрос",
            "tests": "Прошедшим тесты"
        }.get(filter_type, filter_type)
    
    preview_text = f"""📋 <b>ПОДТВЕРЖДЕНИЕ РАССЫЛКИ</b>

<b>Целевая аудитория:</b> {target_info}

<b>Текст сообщения:</b>
━━━━━━━━━━━━━━━━━━━
{broadcast_text}
━━━━━━━━━━━━━━━━━━━

⚠️ <b>Внимание!</b> Рассылка будет отправлена немедленно после подтверждения."""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ОТПРАВИТЬ", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
    ])
    
    await state.update_data(broadcast_text=broadcast_text)
    await message.answer(preview_text, parse_mode="HTML", reply_markup=keyboard)

@admin_router.callback_query(F.data == "confirm_broadcast")
async def execute_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Выполнение рассылки"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("⏳ Выполняю рассылку...")
    
    try:
        state_data = await state.get_data()
        broadcast_text = state_data.get('broadcast_text')
        filter_type = state_data.get('broadcast_filter')
        manual_ids = state_data.get('manual_ids', [])
        
        # Получаем список получателей
        if filter_type == "manual":
            target_ids = manual_ids
        else:
            target_ids = await get_user_ids_by_filter(filter_type)
        
        # Выполняем рассылку
        result = await send_broadcast_to_ids(callback.bot, target_ids, broadcast_text)
        
        # Результат
        success_rate = (result['sent'] / result['total'] * 100) if result['total'] > 0 else 0
        
        result_text = f"""✅ <b>РАССЫЛКА ЗАВЕРШЕНА</b>

📊 <b>Результаты:</b>
• Получателей: {result['total']}
• Отправлено: {result['sent']}
• Ошибок: {result['errors']}
• Успешность: {success_rate:.1f}%

🕐 Время выполнения: {datetime.now().strftime('%H:%M:%S')}

{result.get('details', '')}"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📤 Новая рассылка", callback_data="admin_broadcast_menu")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)
        
        # Очищаем состояние
        await state.clear()
        await state.update_data(admin_authenticated=True)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка рассылки: {e}")

# =========================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===========================

async def get_user_ids_by_filter(filter_type: str) -> list:
    """Получение ID пользователей по фильтру"""
    from database import get_db_sync, User
    def _get_ids():
        db = get_db_sync()
        try:
            # Базовый фильтр для корректных telegram_id (6-10 цифр)
            base_filter = db.query(User).filter(
                User.telegram_id >= 100000,
                User.telegram_id <= 9999999999
            )
            
            if filter_type == "all":
                users = base_filter.filter(User.registration_completed == True).all()
            elif filter_type == "completed":
                users = base_filter.filter(User.completed_diagnostic == True).all()
            elif filter_type == "uncompleted":
                users = base_filter.filter(
                    User.registration_completed == True,
                    User.completed_diagnostic == False
                ).all()
            elif filter_type == "survey":
                users = base_filter.filter(User.survey_completed == True).all()
            elif filter_type == "tests":
                users = base_filter.filter(User.tests_completed == True).all()
            else:
                return []
            
            valid_ids = []
            for user in users:
                # Дополнительная проверка на случай если в базе остались некорректные ID
                if 100000 <= user.telegram_id <= 9999999999:
                    valid_ids.append(user.telegram_id)
            
            return valid_ids
        finally:
            db.close()
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_ids)

async def send_broadcast_to_ids(bot, target_ids: list, message_text: str) -> dict:
    """Отправка рассылки по списку ID"""
    total = len(target_ids)
    sent = 0
    errors = 0
    error_details = []
    
    for user_id in target_ids:
        try:
            await bot.send_message(user_id, message_text, parse_mode="Markdown")
            sent += 1
            # Задержка для избежания лимитов
            await asyncio.sleep(0.05)
        except Exception as e:
            errors += 1
            error_details.append(f"ID {user_id}: {str(e)[:50]}")
    
    # Формируем детали для отчета
    details = ""
    if error_details and len(error_details) <= 5:
        details = "\n\n🔍 <b>Детали ошибок:</b>\n" + "\n".join(error_details)
    elif error_details:
        details = f"\n\n🔍 <b>Ошибки:</b> {len(error_details)} (показаны первые 5)\n" + "\n".join(error_details[:5])
    
    return {
        'total': total,
        'sent': sent,
        'errors': errors,
        'details': details
    }

@admin_router.callback_query(F.data == "broadcast_test")
async def test_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Тестовая рассылка админам"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("🧪 Отправляю тестовое сообщение...")
    
    try:
        admin_ids_str = os.getenv("ADMIN_IDS", "")
        admin_ids = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip()]
        
        test_message = f"""🧪 **ТЕСТОВОЕ СООБЩЕНИЕ**

Время отправки: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}

✅ Система рассылок работает корректно!

Это сообщение получили только администраторы."""
        
        result = await send_broadcast_to_ids(callback.bot, admin_ids, test_message)
        
        result_text = f"""✅ <b>ТЕСТ ЗАВЕРШЕН</b>

📊 Отправлено админам: {result['sent']}/{result['total']}
❌ Ошибок: {result['errors']}

Проверьте свои сообщения!"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_broadcast_menu")]
        ])
        
        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка теста: {e}")

@admin_router.callback_query(F.data == "broadcast_webinar")
async def webinar_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Запуск рассылки вебинара ТОЧКА 2"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    # Проверяем статистику
    try:
        from database import get_db_sync, User, WebinarStatus
        
        def _get_stats():
            db = get_db_sync()
            try:
                total_users = db.query(User).filter(User.registration_completed == True).count()
                already_sent = db.query(WebinarStatus).filter(WebinarStatus.initial_message_sent == True).count()
                return total_users, already_sent
            finally:
                db.close()
        
        total_users, already_sent = await asyncio.get_event_loop().run_in_executor(None, _get_stats)
        will_send = total_users - already_sent
        
        text = f"""🫀 <b>РАССЫЛКА ВЕБИНАРА (ТОЧКА 2)</b>

📊 <b>Статистика:</b>
• Всего пользователей: {total_users}
• Уже получили сообщение: {already_sent}
• Будет отправлено: {will_send}

⚠️ <b>ВНИМАНИЕ!</b>
Это рассылка первого сообщения о просмотре вебинара согласно ТЗ ТОЧКА 2.

Отправится сообщение:
"Здравствуйте! На связи врачи-кардиологи Диана Сергеевна И Елена Васильевна..."

С кнопками:
🔘 Да, посмотрел(а)
🔘 Нет, ещё не успел(а)

Продолжить?"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ЗАПУСТИТЬ РАССЫЛКУ", callback_data="confirm_webinar_broadcast")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "confirm_webinar_broadcast")
async def confirm_webinar_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Подтверждение и выполнение рассылки вебинара"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("⏳ Запускаю рассылку вебинара...")
    
    try:
        # Импортируем функцию рассылки
        from handlers.webinar_broadcast import broadcast_initial_message_to_all
        
        # Выполняем рассылку
        result = await broadcast_initial_message_to_all(callback.bot)
        
        result_text = f"""✅ <b>РАССЫЛКА ВЕБИНАРА ЗАВЕРШЕНА</b>

📊 <b>Результаты:</b>
• Всего пользователей: {result['total']}
• Успешно отправлено: {result['sent']}
• Ошибок отправки: {result['failed']}
• Успешность: {(result['sent']/result['total']*100 if result['total'] > 0 else 0):.1f}%

🚀 <b>Что будет дальше:</b>
• Пользователи получат сообщение с кнопками
• При нажатии "Да" → сразу опрос ТОЧКА 2
• При нажатии "Нет" → через сутки напоминание
• Планировщик автоматически отслеживает напоминания

✅ Рассылка ТОЧКА 2 активирована!"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка рассылки вебинара: {e}")

@admin_router.callback_query(F.data == "broadcast_followup")
async def followup_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Запуск рассылки опроса ТОЧКА 3 (3+ месяца после вебинара)"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()

    # Проверяем статистику
    try:
        from database import get_db_sync, User, FollowUpStatus

        def _get_stats():
            db = get_db_sync()
            try:
                total_users = db.query(User).filter(User.registration_completed == True).count()
                already_sent = db.query(FollowUpStatus).filter(FollowUpStatus.initial_message_sent == True).count()
                return total_users, already_sent
            finally:
                db.close()

        total_users, already_sent = await asyncio.get_event_loop().run_in_executor(None, _get_stats)
        will_send = total_users - already_sent

        text = f"""📅 <b>РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)</b>

📊 <b>Статистика:</b>
• Всего пользователей: {total_users}
• Уже получили сообщение: {already_sent}
• Будет отправлено: {will_send}

⚠️ <b>ВНИМАНИЕ!</b>
Это рассылка опроса через 3+ месяца после вебинара согласно ТЗ ТОЧКА 3.

Отправится сообщение:
"Уже прошло больше трёх месяцев после нашей встречи на вебинаре «Умный кардиочекап»..."

С кнопкой:
🔘 Начать опрос

После прохождения 13 вопросов пользователи получат методичку ЗОЖ.

Продолжить?"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ЗАПУСТИТЬ РАССЫЛКУ", callback_data="confirm_followup_broadcast")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
        ])

        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "confirm_followup_broadcast")
async def confirm_followup_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Подтверждение и выполнение рассылки опроса ТОЧКА 3"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Запускаю рассылку опроса ТОЧКА 3...")

    try:
        # Импортируем функцию рассылки
        from handlers.followup_broadcast import broadcast_followup_to_all

        # Выполняем рассылку
        result = await broadcast_followup_to_all(callback.bot)

        result_text = f"""✅ <b>РАССЫЛКА ОПРОСА ТОЧКА 3 ЗАВЕРШЕНА</b>

📊 <b>Результаты:</b>
• Всего пользователей в БД: {result['total_users']}
• Целевая аудитория: {result['total']}
• Успешно отправлено: {result['sent']}
• Ошибок отправки: {result['failed']}
• Успешность: {(result['sent']/result['total']*100 if result['total'] > 0 else 0):.1f}%

🚀 <b>Что будет дальше:</b>
• Пользователи получат начальное сообщение с кнопкой "Начать опрос"
• При нажатии → опрос из 13 вопросов
• После завершения → автоматическая отправка методички ЗОЖ

✅ Рассылка ТОЧКА 3 активирована!"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])

        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка рассылки опроса ТОЧКА 3: {e}")

@admin_router.callback_query(F.data == "broadcast_followup_reminder")
async def followup_reminder_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Рассылка напоминания тем, кто не прошёл опрос ТОЧКА 3"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()

    try:
        from database import get_db_sync, User, FollowUpStatus, FollowUpSurvey

        def _get_stats():
            db = get_db_sync()
            try:
                # Пользователи, которым отправлено сообщение
                sent_users = db.query(FollowUpStatus).filter(
                    FollowUpStatus.initial_message_sent == True
                ).all()
                sent_ids = {s.telegram_id for s in sent_users}

                # Пользователи, которые прошли опрос
                completed_users = db.query(FollowUpSurvey).filter(
                    FollowUpSurvey.completed_at.isnot(None)
                ).all()
                completed_ids = {c.telegram_id for c in completed_users}

                # Те, кому отправлено, но не прошли
                not_completed_ids = sent_ids - completed_ids

                return len(sent_ids), len(completed_ids), len(not_completed_ids), list(not_completed_ids)
            finally:
                db.close()

        total_sent, total_completed, not_completed_count, not_completed_ids = await asyncio.get_event_loop().run_in_executor(None, _get_stats)

        text = f"""🔄 <b>НАПОМИНАНИЕ ДЛЯ НЕ ПРОШЕДШИХ ТОЧКА 3</b>

📊 <b>Статистика:</b>
• Всего получили приглашение: {total_sent}
• Прошли опрос: {total_completed}
• <b>НЕ прошли опрос: {not_completed_count}</b>

⚠️ Будет отправлено напоминание {not_completed_count} пользователям, которые получили приглашение, но ещё не прошли опрос ТОЧКА 3.

Продолжить?"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ОТПРАВИТЬ НАПОМИНАНИЕ", callback_data="confirm_followup_reminder")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
        ])

        # Сохраняем список ID для отправки
        await state.update_data(followup_reminder_ids=not_completed_ids)

        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "confirm_followup_reminder")
async def confirm_followup_reminder(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Подтверждение и выполнение рассылки напоминания"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Отправляю напоминания...")

    try:
        from handlers.followup_broadcast import FOLLOWUP_INITIAL_MESSAGE, get_followup_start_keyboard

        state_data = await state.get_data()
        target_ids = state_data.get('followup_reminder_ids', [])

        if not target_ids:
            await callback.message.edit_text("❌ Нет пользователей для отправки напоминания")
            return

        keyboard = get_followup_start_keyboard()
        sent = 0
        failed = 0

        for user_id in target_ids:
            try:
                await callback.bot.send_message(
                    user_id,
                    FOLLOWUP_INITIAL_MESSAGE,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
                sent += 1
            except Exception as e:
                failed += 1

            await asyncio.sleep(0.05)

        result_text = f"""✅ <b>НАПОМИНАНИЕ ОТПРАВЛЕНО</b>

📊 <b>Результаты:</b>
• Всего получателей: {len(target_ids)}
• Успешно отправлено: {sent}
• Ошибок: {failed}
• Успешность: {(sent/len(target_ids)*100 if target_ids else 0):.1f}%

Пользователи получили повторное приглашение пройти опрос ТОЧКА 3."""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])

        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка отправки напоминаний: {e}")

# =========================== РАССЫЛКА ИЗВИНЕНИЙ (прошли опрос, но без бонуса) ===========================

APOLOGY_MESSAGE = """Дорогие участники!

Приносим искренние извинения за доставленные неудобства.

Вы уже прошли наш опрос, и мы очень ценим ваше время и участие! К сожалению, по техническим причинам бонус не был отправлен сразу.

Мы обязательно отправим вам обещанный подарок — <b>«Руководство для женщин 40+: как защитить сердце в перименопаузе и менопаузе»</b> — в ближайшие дни.

Благодарим за понимание и терпение! Ваш вклад в наше исследование бесценен.

С уважением и заботой о вашем здоровье 💙"""

@admin_router.callback_query(F.data == "broadcast_apology")
async def apology_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Рассылка извинений тем, кто прошёл опрос ТОЧКА 3, но не получил бонус"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()

    try:
        from database import get_db_sync, FollowUpSurvey

        def _get_stats():
            db = get_db_sync()
            try:
                # Пользователи, которые ПОЛНОСТЬЮ прошли опрос
                # (main_change - это 20-й вопрос, последний в опросе)
                completed_users = db.query(FollowUpSurvey).filter(
                    FollowUpSurvey.main_change.isnot(None)
                ).all()
                completed_ids = [c.telegram_id for c in completed_users]

                return len(completed_ids), completed_ids
            finally:
                db.close()

        total_completed, completed_ids = await asyncio.get_event_loop().run_in_executor(None, _get_stats)

        text = f"""💙 <b>РАССЫЛКА ИЗВИНЕНИЙ</b>

📊 <b>Статистика:</b>
• <b>Полностью прошли опрос ТОЧКА 3 (ответили на 20-й вопрос): {total_completed}</b>

⚠️ Будет отправлено извинительное сообщение {total_completed} пользователям, которые полностью прошли опрос.

<b>Текст сообщения:</b>
<i>{APOLOGY_MESSAGE[:200]}...</i>

Продолжить?"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ОТПРАВИТЬ ИЗВИНЕНИЯ", callback_data="confirm_apology_broadcast")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
        ])

        # Сохраняем список ID для отправки
        await state.update_data(apology_broadcast_ids=completed_ids)

        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "confirm_apology_broadcast")
async def confirm_apology_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Подтверждение и выполнение рассылки извинений"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Отправляю извинительные сообщения...")

    try:
        state_data = await state.get_data()
        target_ids = state_data.get('apology_broadcast_ids', [])

        if not target_ids:
            await callback.message.edit_text("❌ Нет пользователей для отправки")
            return

        sent = 0
        failed = 0

        for user_id in target_ids:
            try:
                await callback.bot.send_message(
                    user_id,
                    APOLOGY_MESSAGE,
                    parse_mode="HTML"
                )
                sent += 1
            except Exception as e:
                failed += 1

            await asyncio.sleep(0.05)

        result_text = f"""✅ <b>ИЗВИНЕНИЯ ОТПРАВЛЕНЫ</b>

📊 <b>Результаты:</b>
• Всего получателей: {len(target_ids)}
• Успешно отправлено: {sent}
• Ошибок: {failed}
• Успешность: {(sent/len(target_ids)*100 if target_ids else 0):.1f}%

Пользователи уведомлены, что бонус будет отправлен в ближайшие дни."""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])

        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка отправки: {e}")

# =========================== РАССЫЛКА МЕТОДИЧКИ (ТОЧКА 3) ===========================

METHODICHKA_MESSAGE = """Здравствуйте! 💚

Как и обещали — отправляем вам <b>«Руководство для женщин 40+: как защитить сердце в перименопаузе и менопаузе»</b>.

Спасибо за ваше участие в нашем исследовании и за то, что прошли опрос! Надеемся, это руководство будет для вас полезным.

Берегите себя и своё сердце 🫀"""

@admin_router.callback_query(F.data == "broadcast_methodichka")
async def methodichka_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Рассылка методички тем, кто прошёл опрос ТОЧКА 3"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()

    try:
        from database import get_db_sync, FollowUpSurvey

        def _get_stats():
            db = get_db_sync()
            try:
                completed_users = db.query(FollowUpSurvey).filter(
                    FollowUpSurvey.main_change.isnot(None)
                ).all()
                completed_ids = [c.telegram_id for c in completed_users]
                return len(completed_ids), completed_ids
            finally:
                db.close()

        total_completed, completed_ids = await asyncio.get_event_loop().run_in_executor(None, _get_stats)

        # Проверяем наличие файла
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        file_path = os.path.join(project_root, "materials", "Методичка менопауза 1.pdf")
        file_exists = os.path.exists(file_path)

        text = f"""📕 <b>РАССЫЛКА МЕТОДИЧКИ</b>

📊 <b>Статистика:</b>
• <b>Полностью прошли опрос ТОЧКА 3: {total_completed}</b>
• Файл методички: {"✅ найден" if file_exists else "❌ НЕ НАЙДЕН"}

⚠️ Будет отправлено сообщение с методичкой {total_completed} пользователям.

<b>Текст сообщения:</b>
<i>{METHODICHKA_MESSAGE[:200]}...</i>

Продолжить?"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ОТПРАВИТЬ МЕТОДИЧКУ", callback_data="confirm_methodichka_broadcast")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
        ])

        await state.update_data(methodichka_broadcast_ids=completed_ids)
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "confirm_methodichka_broadcast")
async def confirm_methodichka_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Подтверждение и выполнение рассылки методички"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Отправляю методичку...")

    try:
        state_data = await state.get_data()
        target_ids = state_data.get('methodichka_broadcast_ids', [])

        if not target_ids:
            await callback.message.edit_text("❌ Нет пользователей для отправки")
            return

        # Проверяем файл
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))
        file_path = os.path.join(project_root, "materials", "Методичка менопауза 1.pdf")

        if not os.path.exists(file_path):
            await callback.message.edit_text("❌ Файл методички не найден: materials/Методичка менопауза 1.pdf")
            return

        sent = 0
        failed = 0
        file_id = None  # Кешируем file_id после первой отправки

        for user_id in target_ids:
            try:
                if file_id:
                    # Используем кешированный file_id для быстрой отправки
                    await callback.bot.send_document(
                        user_id,
                        document=file_id,
                        caption=METHODICHKA_MESSAGE,
                        parse_mode="HTML"
                    )
                else:
                    # Первая отправка — загружаем файл
                    document = FSInputFile(file_path, filename="Руководство_для_женщин_40+.pdf")
                    result = await callback.bot.send_document(
                        user_id,
                        document=document,
                        caption=METHODICHKA_MESSAGE,
                        parse_mode="HTML"
                    )
                    file_id = result.document.file_id
                sent += 1
            except Exception as e:
                failed += 1

            await asyncio.sleep(0.05)

        result_text = f"""✅ <b>МЕТОДИЧКА ОТПРАВЛЕНА</b>

📊 <b>Результаты:</b>
• Всего получателей: {len(target_ids)}
• Успешно отправлено: {sent}
• Ошибок: {failed}
• Успешность: {(sent/len(target_ids)*100 if target_ids else 0):.1f}%"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])

        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка отправки: {e}")

# =========================== МЯГКОЕ НАПОМИНАНИЕ (ТОЧКА 3) ===========================

SOFT_REMINDER_MESSAGE = """Здравствуйте! 💛

Некоторое время назад мы отправляли вам приглашение пройти небольшой опрос — и будем очень рады, если вы найдёте несколько минут, чтобы его заполнить.

Ваши ответы действительно важны для нас — они помогают делать наши программы лучше и полезнее.

🎁 А после прохождения вас ждёт подарок — <b>«Руководство для женщин 40+: как защитить сердце в перименопаузе и менопаузе»</b>.

Спасибо, что вы с нами 🤍"""

@admin_router.callback_query(F.data == "broadcast_soft_reminder")
async def soft_reminder_broadcast(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Мягкое напоминание тем, кто не прошёл опрос ТОЧКА 3"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()

    try:
        from database import get_db_sync, FollowUpStatus, FollowUpSurvey

        def _get_stats():
            db = get_db_sync()
            try:
                sent_users = db.query(FollowUpStatus).filter(
                    FollowUpStatus.initial_message_sent == True
                ).all()
                sent_ids = {s.telegram_id for s in sent_users}

                completed_users = db.query(FollowUpSurvey).filter(
                    FollowUpSurvey.completed_at.isnot(None)
                ).all()
                completed_ids = {c.telegram_id for c in completed_users}

                not_completed_ids = sent_ids - completed_ids
                return len(sent_ids), len(completed_ids), len(not_completed_ids), list(not_completed_ids)
            finally:
                db.close()

        total_sent, total_completed, not_completed_count, not_completed_ids = await asyncio.get_event_loop().run_in_executor(None, _get_stats)

        text = f"""🔔 <b>МЯГКОЕ НАПОМИНАНИЕ (ТОЧКА 3)</b>

📊 <b>Статистика:</b>
• Всего получили приглашение: {total_sent}
• Прошли опрос: {total_completed}
• <b>НЕ прошли опрос: {not_completed_count}</b>

⚠️ Будет отправлено мягкое напоминание {not_completed_count} пользователям.

<b>Текст сообщения:</b>
<i>{SOFT_REMINDER_MESSAGE[:200]}...</i>

Продолжить?"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ОТПРАВИТЬ НАПОМИНАНИЕ", callback_data="confirm_soft_reminder")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_menu")]
        ])

        await state.update_data(soft_reminder_ids=not_completed_ids)
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка получения статистики: {e}")

@admin_router.callback_query(F.data == "confirm_soft_reminder")
async def confirm_soft_reminder(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Подтверждение и выполнение мягкого напоминания"""
    if not await check_admin_auth(callback, state, is_admin):
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Отправляю напоминания...")

    try:
        from handlers.followup_broadcast import get_followup_start_keyboard

        state_data = await state.get_data()
        target_ids = state_data.get('soft_reminder_ids', [])

        if not target_ids:
            await callback.message.edit_text("❌ Нет пользователей для отправки")
            return

        keyboard = get_followup_start_keyboard()
        sent = 0
        failed = 0

        for user_id in target_ids:
            try:
                await callback.bot.send_message(
                    user_id,
                    SOFT_REMINDER_MESSAGE,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
                sent += 1
            except Exception as e:
                failed += 1

            await asyncio.sleep(0.05)

        result_text = f"""✅ <b>МЯГКОЕ НАПОМИНАНИЕ ОТПРАВЛЕНО</b>

📊 <b>Результаты:</b>
• Всего получателей: {len(target_ids)}
• Успешно отправлено: {sent}
• Ошибок: {failed}
• Успешность: {(sent/len(target_ids)*100 if target_ids else 0):.1f}%"""

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
        ])

        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка отправки: {e}")

# =========================== ПРОВЕРКА АВТОРИЗАЦИИ ===========================

async def check_admin_auth(callback: CallbackQuery, state: FSMContext, is_admin: bool) -> bool:
    """Проверка авторизации админа"""
    from database.analytics import admin_get_stats
    if not is_admin:
        await callback.answer("❌ Нет прав доступа", show_alert=True)
        return False
    
    admin_session = await state.get_data()
    if not admin_session.get('admin_authenticated'):
        await callback.answer("❌ Необходимо повторно ввести пароль", show_alert=True)
        await request_admin_password(callback.message, state)
        return False
    
    return True

# =========================== НАВИГАЦИЯ ===========================

@admin_router.callback_query(F.data == "admin_back")
async def back_to_admin(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Возврат в главное меню админки"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await show_admin_panel(callback.message)

@admin_router.callback_query(F.data == "admin_logout")
async def admin_logout(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Выход из админки"""
    if not is_admin:
        await callback.answer("❌ Нет прав доступа", show_alert=True)
        return
    
    await callback.answer()
    await state.clear()
    
    text = """👋 <b>Выход из админки</b>

Сессия завершена. Для повторного входа используйте /admin"""
    
    await callback.message.edit_text(text, parse_mode="HTML")

# =========================== СУЩЕСТВУЮЩИЕ ФУНКЦИИ ===========================

@admin_router.callback_query(F.data == "admin_stats")
async def show_stats(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Показать статистику"""
    from database.analytics import admin_get_stats
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    try:
        stats = await admin_get_stats()
        
        text = f"""📊 <b>Статистика бота</b>

👥 <b>Пользователи:</b>
• Всего: {stats['total_users']}
• Завершили регистрацию: {stats['completed_registration']}
• Завершили опрос: {stats['completed_surveys']}
• Прошли тесты: {stats['completed_tests']}
• Завершили диагностику: {stats['completed_diagnostic']}

📈 <b>Конверсия:</b>
• Регистрация: {stats['completed_registration']/max(stats['total_users'], 1)*100:.1f}%
• Опрос: {stats['completed_surveys']/max(stats['total_users'], 1)*100:.1f}%
• Тесты: {stats['completed_tests']/max(stats['total_users'], 1)*100:.1f}%
• Диагностика: {stats['completed_diagnostic']/max(stats['total_users'], 1)*100:.1f}%"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}")

# =========================== СИСТЕМА ИМПОРТА БД ===========================

@admin_router.callback_query(F.data == "admin_import_menu")
async def import_menu(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Меню импорта базы данных"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    text = """💾 <b>ИМПОРТ БАЗЫ ДАННЫХ</b>

⚠️ <b>ВНИМАНИЕ!</b> Импорт заменит существующие данные!

<b>Поддерживаемые форматы:</b>
• Excel файлы (.xlsx) из экспорта бота
• Можно использовать лист "Все данные" или отдельные листы

<b>Процесс импорта:</b>
1️⃣ Отправьте Excel файл
2️⃣ Система проанализирует структуру
3️⃣ Подтвердите импорт
4️⃣ Данные будут загружены

<b>Что будет импортировано:</b>
• Пользователи и их данные
• Результаты опросов
• Результаты тестов
• Статусы прохождения

📤 Отправьте Excel файл для импорта:"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Инструкция", callback_data="import_help")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(AdminStates.waiting_import_file)

@admin_router.callback_query(F.data == "import_help")
async def import_help(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Помощь по импорту"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    text = """📋 <b>ИНСТРУКЦИЯ ПО ИМПОРТУ</b>

<b>1. Подготовка файла:</b>
• Используйте Excel файл из экспорта бота
• Главный лист "Все данные" содержит полную информацию
• Можно редактировать данные, но сохраните структуру колонок

<b>2. Обязательные колонки:</b>
• telegram_id - ID пользователя в Telegram
• name - имя пользователя
• email - email (можно автогенерированный)
• phone - телефон (можно автогенерированный)

<b>3. Дополнительные колонки:</b>
• Все колонки из опросов (age, gender, location и т.д.)
• Результаты тестов (hads_anxiety_score, burns_score и т.д.)
• Статусы (survey_completed, tests_completed и т.д.)

<b>4. Формат данных:</b>
• Даты в формате YYYY-MM-DD HH:MM:SS
• Булевы значения: True/False или 1/0
• Числовые значения без пробелов
• JSON поля как текст (будут обработаны автоматически)

<b>5. Безопасность:</b>
• Создается резервная копия перед импортом
• Можно откатить изменения
• Проверка целостности данных

Отправьте файл после прочтения инструкции."""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ К импорту", callback_data="admin_import_menu")]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@admin_router.message(AdminStates.waiting_import_file)
async def handle_import_file(message: Message, state: FSMContext, is_admin: bool = False):
    """Обработка файла для импорта"""
    if not is_admin:
        await message.answer("❌ Нет прав доступа.")
        await state.clear()
        return
    
    if not message.document:
        await message.answer("❌ Пожалуйста, отправьте Excel файл (.xlsx)")
        return
    
    if not message.document.file_name.endswith('.xlsx'):
        await message.answer("❌ Поддерживаются только файлы Excel (.xlsx)")
        return
    
    await message.answer("⏳ Анализирую файл...")
    
    try:
        # Скачиваем файл
        file = await message.bot.get_file(message.document.file_id)
        file_path = f"import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await message.bot.download_file(file.file_path, file_path)
        
        # Анализируем структуру файла
        analysis = await analyze_import_file(file_path)
        
        if not analysis['success']:
            os.remove(file_path)
            await message.answer(f"❌ Ошибка анализа файла: {analysis['error']}")
            return
        
        # Сохраняем путь к файлу в состоянии
        await state.update_data(import_file_path=file_path, import_analysis=analysis)
        
        # Показываем превью импорта
        preview_text = f"""📊 <b>АНАЛИЗ ФАЙЛА ЗАВЕРШЕН</b>

<b>📁 Файл:</b> {message.document.file_name}
<b>📏 Размер:</b> {message.document.file_size / 1024:.1f} КБ

<b>📋 Найденные листы:</b>
{chr(10).join([f"• {sheet}" for sheet in analysis['sheets']])}

<b>📊 Статистика данных:</b>
• Пользователей: {analysis['users_count']}
• Опросов: {analysis['surveys_count']}
• Результатов тестов: {analysis['tests_count']}

<b>✅ Валидных записей:</b> {analysis['valid_records']}
<b>⚠️ Проблемных записей:</b> {analysis['problematic_records']}

{analysis.get('warnings', '')}

⚠️ <b>ВНИМАНИЕ!</b> Текущие данные будут заменены!"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ ИМПОРТ", callback_data="confirm_import")],
            [InlineKeyboardButton(text="📋 Детали анализа", callback_data="import_details")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_import")]
        ])
        
        await message.answer(preview_text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка обработки файла: {e}")
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)

@admin_router.callback_query(F.data == "import_details")
async def show_import_details(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Показать детали анализа импорта"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    state_data = await state.get_data()
    analysis = state_data.get('import_analysis', {})
    
    details_text = f"""🔍 <b>ДЕТАЛЬНЫЙ АНАЛИЗ ИМПОРТА</b>

<b>📋 Структура данных:</b>
• Найдено колонок: {len(analysis.get('columns', []))}
• Обязательные колонки: {'✅' if analysis.get('has_required_columns') else '❌'}

<b>📊 Распределение данных:</b>
• Строк в "Все данные": {analysis.get('all_data_rows', 0)}
• Уникальных telegram_id: {analysis.get('unique_telegram_ids', 0)}
• Дубликатов: {analysis.get('duplicate_ids', 0)}

<b>📈 Качество данных:</b>
• Полных записей: {analysis.get('complete_records', 0)}
• Частично заполненных: {analysis.get('partial_records', 0)}
• Пустых/некорректных: {analysis.get('empty_records', 0)}

<b>🔗 Связанность данных:</b>
• Пользователи с опросами: {analysis.get('users_with_surveys', 0)}
• Пользователи с тестами: {analysis.get('users_with_tests', 0)}
• Завершенные диагностики: {analysis.get('completed_diagnostics', 0)}

<b>⚠️ Потенциальные проблемы:</b>
{chr(10).join([f"• {issue}" for issue in analysis.get('issues', [])])}

<b>💡 Рекомендации:</b>
{chr(10).join([f"• {rec}" for rec in analysis.get('recommendations', [])])}"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Продолжить импорт", callback_data="confirm_import")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_import_menu")]
    ])
    
    await callback.message.edit_text(details_text, parse_mode="HTML", reply_markup=keyboard)

@admin_router.callback_query(F.data == "confirm_import")
async def execute_import(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Выполнение импорта данных"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("⏳ Выполняю импорт данных...")
    
    try:
        state_data = await state.get_data()
        file_path = state_data.get('import_file_path')
        
        if not file_path or not os.path.exists(file_path):
            await callback.message.edit_text("❌ Файл не найден")
            return
        
        # Создаем резервную копию
        backup_path = await create_database_backup()
        
        # Выполняем импорт
        import_result = await perform_database_import(file_path)
        
        # Удаляем временный файл
        os.remove(file_path)
        
        if import_result['success']:
            result_text = f"""✅ <b>ИМПОРТ ЗАВЕРШЕН УСПЕШНО</b>

<b>📊 Импортировано:</b>
• Пользователей: {import_result['imported_users']}
• Опросов: {import_result['imported_surveys']}
• Результатов тестов: {import_result['imported_tests']}

<b>🔄 Обновлено записей:</b> {import_result['updated_records']}
<b>🆕 Создано новых:</b> {import_result['created_records']}

<b>💾 Резервная копия:</b> {backup_path}

<b>⏱ Время импорта:</b> {import_result['import_time']:.2f} сек

База данных успешно восстановлена из Excel файла!"""
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📊 Проверить статистику", callback_data="admin_stats")],
                [InlineKeyboardButton(text="📤 Тестовая рассылка", callback_data="broadcast_test")],
                [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
            ])
        else:
            result_text = f"""❌ <b>ОШИБКА ИМПОРТА</b>

<b>Причина:</b> {import_result['error']}

<b>💾 Резервная копия создана:</b> {backup_path}
База данных не изменена.

<b>🔧 Рекомендации:</b>
• Проверьте формат файла
• Убедитесь в корректности данных
• Обратитесь к инструкции по импорту"""
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Инструкция", callback_data="import_help")],
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="admin_import_menu")],
                [InlineKeyboardButton(text="⬅️ В админку", callback_data="admin_back")]
            ])
        
        await callback.message.edit_text(result_text, parse_mode="HTML", reply_markup=keyboard)
        
        # Очищаем состояние
        await state.clear()
        await state.update_data(admin_authenticated=True)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Критическая ошибка импорта: {e}")

@admin_router.callback_query(F.data == "cancel_import")
async def cancel_import(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Отмена импорта"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    # Удаляем временный файл
    state_data = await state.get_data()
    file_path = state_data.get('import_file_path')
    if file_path and os.path.exists(file_path):
        os.remove(file_path)
    
    await callback.message.edit_text("❌ Импорт отменен. Данные не изменены.")
    
    # Возвращаемся в админку
    await asyncio.sleep(2)
    await show_admin_panel(callback.message)

# =========================== ФУНКЦИИ ИМПОРТА ===========================

async def analyze_import_file(file_path: str) -> dict:
    """Анализ файла для импорта"""
    def _analyze():
        try:
            # Читаем Excel файл
            excel_file = pd.ExcelFile(file_path)
            sheets = excel_file.sheet_names
            
            # Пробуем найти лист "Все данные" или первый доступный
            main_sheet = None
            if "Все данные" in sheets:
                main_sheet = "Все данные"
            elif "All data" in sheets:
                main_sheet = "All data"
            elif len(sheets) > 0:
                main_sheet = sheets[0]
            
            if not main_sheet:
                return {"success": False, "error": "Не найдены подходящие листы"}
            
            # Читаем основные данные
            df = pd.read_excel(file_path, sheet_name=main_sheet)
            
            # Анализируем структуру
            columns = list(df.columns)
            required_columns = ['telegram_id', 'name', 'email', 'phone']
            has_required = all(col in columns for col in required_columns)
            
            # Статистика данных
            total_rows = len(df)
            unique_telegram_ids = df['telegram_id'].nunique() if 'telegram_id' in columns else 0
            duplicate_ids = total_rows - unique_telegram_ids
            
            # Подсчет записей по типам
            users_count = len(df[df['telegram_id'].notna()]) if 'telegram_id' in columns else 0
            surveys_count = len(df[df['age'].notna()]) if 'age' in columns else 0
            tests_count = len(df[df['hads_anxiety_score'].notna()]) if 'hads_anxiety_score' in columns else 0
            
            # Проверка качества данных
            complete_records = 0
            partial_records = 0
            empty_records = 0
            
            for _, row in df.iterrows():
                filled_cols = sum(1 for val in row if pd.notna(val) and val != '')
                if filled_cols >= len(required_columns):
                    complete_records += 1
                elif filled_cols > 0:
                    partial_records += 1
                else:
                    empty_records += 1
            
            # Поиск проблем
            issues = []
            recommendations = []
            
            if not has_required:
                missing = [col for col in required_columns if col not in columns]
                issues.append(f"Отсутствуют обязательные колонки: {', '.join(missing)}")
                recommendations.append("Добавьте отсутствующие колонки в файл")
            
            if duplicate_ids > 0:
                issues.append(f"Найдены дубликаты telegram_id: {duplicate_ids}")
                recommendations.append("Удалите дублированные записи")
            
            if empty_records > total_rows * 0.1:  # Более 10% пустых
                issues.append(f"Много пустых записей: {empty_records}")
                recommendations.append("Очистите пустые строки")
            
            # Дополнительная статистика
            users_with_surveys = len(df[(df['telegram_id'].notna()) & (df['age'].notna())]) if all(col in columns for col in ['telegram_id', 'age']) else 0
            users_with_tests = len(df[(df['telegram_id'].notna()) & (df['hads_anxiety_score'].notna())]) if all(col in columns for col in ['telegram_id', 'hads_anxiety_score']) else 0
            completed_diagnostics = len(df[df['completed_diagnostic'] == True]) if 'completed_diagnostic' in columns else 0
            
            warnings = ""
            if issues:
                warnings = f"\n⚠️ <b>Внимание:</b>\n{chr(10).join([f'• {issue}' for issue in issues[:3]])}"
            
            return {
                "success": True,
                "sheets": sheets,
                "main_sheet": main_sheet,
                "columns": columns,
                "has_required_columns": has_required,
                "users_count": users_count,
                "surveys_count": surveys_count,
                "tests_count": tests_count,
                "all_data_rows": total_rows,
                "unique_telegram_ids": unique_telegram_ids,
                "duplicate_ids": duplicate_ids,
                "complete_records": complete_records,
                "partial_records": partial_records,
                "empty_records": empty_records,
                "valid_records": complete_records + partial_records,
                "problematic_records": empty_records + duplicate_ids,
                "users_with_surveys": users_with_surveys,
                "users_with_tests": users_with_tests,
                "completed_diagnostics": completed_diagnostics,
                "issues": issues,
                "recommendations": recommendations,
                "warnings": warnings
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _analyze)

async def create_database_backup() -> str:
    """Создание резервной копии базы данных"""
    def _backup():
        import shutil
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"backup_before_import_{timestamp}.db"
        
        if os.path.exists("cardio_bot.db"):
            shutil.copy2("cardio_bot.db", backup_path)
            return backup_path
        return None
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _backup)

async def perform_database_import(file_path: str) -> dict:
    """Выполнение импорта данных в базу"""
    def _import():
        import time
        start_time = time.time()
        
        try:
            from database import get_db_sync, User, Survey, TestResult
            
            # Читаем Excel файл
            df = pd.read_excel(file_path, sheet_name="Все данные" if "Все данные" in pd.ExcelFile(file_path).sheet_names else 0)
            
            # Подготавливаем данные
            df = df.where(pd.notnull(df), None)  # Заменяем NaN на None
            
            db = get_db_sync()
            
            try:
                imported_users = 0
                imported_surveys = 0
                imported_tests = 0
                updated_records = 0
                created_records = 0
                
                # Очищаем существующие данные
                db.query(TestResult).delete()
                db.query(Survey).delete()
                db.query(User).delete()
                db.commit()
                
                # Импортируем пользователей
                processed_users = set()
                
                for _, row in df.iterrows():
                    telegram_id = row.get('telegram_id')
                    
                    if pd.isna(telegram_id) or telegram_id in processed_users:
                        continue
                    
                    processed_users.add(telegram_id)
                    
                    # Создаем пользователя
                    user = User(
                        telegram_id=int(telegram_id),
                        name=row.get('name') or f"User_{int(telegram_id)}",
                        email=row.get('email') or f"user_{int(telegram_id)}@bot.com",
                        phone=row.get('phone') or f"+{int(telegram_id)}",
                        completed_diagnostic=bool(row.get('completed_diagnostic', False)),
                        registration_completed=bool(row.get('registration_completed', True)),
                        survey_completed=bool(row.get('survey_completed', False)),
                        tests_completed=bool(row.get('tests_completed', False)),
                        created_at=pd.to_datetime(row.get('registration_date', datetime.now())),
                        updated_at=datetime.now(),
                        last_activity=pd.to_datetime(row.get('last_activity', datetime.now()))
                    )
                    db.add(user)
                    imported_users += 1
                    created_records += 1
                
                db.commit()
                
                # Импортируем опросы
                for _, row in df.iterrows():
                    telegram_id = row.get('telegram_id')
                    age = row.get('age')
                    
                    if pd.isna(telegram_id) or pd.isna(age):
                        continue
                    
                    # Обработка JSON полей
                    def safe_json_field(value):
                        if pd.isna(value) or value == '':
                            return None
                        if isinstance(value, str) and (value.startswith('[') or value.startswith('{')):
                            return value
                        return json.dumps([value] if value else [], ensure_ascii=False)
                    
                    survey = Survey(
                        telegram_id=int(telegram_id),
                        age=int(age) if not pd.isna(age) else None,
                        gender=row.get('gender'),
                        location=row.get('location'),
                        education=row.get('education'),
                        family_status=row.get('family_status'),
                        children=row.get('children'),
                        income=row.get('income'),
                        health_rating=int(row.get('health_rating')) if not pd.isna(row.get('health_rating')) else None,
                        death_cause=row.get('death_cause'),
                        heart_disease=row.get('heart_disease'),
                        cv_risk=row.get('cv_risk'),
                        cv_knowledge=row.get('cv_knowledge'),
                        heart_danger=safe_json_field(row.get('heart_danger')),
                        health_importance=row.get('health_importance'),
                        checkup_history=row.get('checkup_history'),
                        checkup_content=safe_json_field(row.get('checkup_content')),
                        prevention_barriers=safe_json_field(row.get('prevention_barriers')),
                        prevention_barriers_other=row.get('prevention_barriers_other'),
                        health_advice=safe_json_field(row.get('health_advice')),
                        created_at=datetime.now(),
                        completed_at=pd.to_datetime(row.get('survey_completed_at', datetime.now()))
                    )
                    db.add(survey)
                    imported_surveys += 1
                    created_records += 1
                
                db.commit()
                
                # Импортируем результаты тестов
                for _, row in df.iterrows():
                    telegram_id = row.get('telegram_id')
                    hads_score = row.get('hads_anxiety_score')
                    
                    if pd.isna(telegram_id) or pd.isna(hads_score):
                        continue
                    
                    test_result = TestResult(
                        telegram_id=int(telegram_id),
                        hads_anxiety_score=int(hads_score) if not pd.isna(hads_score) else None,
                        hads_depression_score=int(row.get('hads_depression_score')) if not pd.isna(row.get('hads_depression_score')) else None,
                        hads_total_score=int(row.get('hads_total_score')) if not pd.isna(row.get('hads_total_score')) else None,
                        hads_anxiety_level=row.get('hads_anxiety_level'),
                        hads_depression_level=row.get('hads_depression_level'),
                        burns_score=int(row.get('burns_score')) if not pd.isna(row.get('burns_score')) else None,
                        burns_level=row.get('burns_level'),
                        isi_score=int(row.get('isi_score')) if not pd.isna(row.get('isi_score')) else None,
                        isi_level=row.get('isi_level'),
                        stop_bang_score=int(row.get('stop_bang_score')) if not pd.isna(row.get('stop_bang_score')) else None,
                        stop_bang_risk=row.get('stop_bang_risk'),
                        ess_score=int(row.get('ess_score')) if not pd.isna(row.get('ess_score')) else None,
                        ess_level=row.get('ess_level'),
                        fagerstrom_score=int(row.get('fagerstrom_score')) if not pd.isna(row.get('fagerstrom_score')) else None,
                        fagerstrom_level=row.get('fagerstrom_level'),
                        fagerstrom_skipped=bool(row.get('fagerstrom_skipped', False)),
                        audit_score=int(row.get('audit_score')) if not pd.isna(row.get('audit_score')) else None,
                        audit_level=row.get('audit_level'),
                        audit_skipped=bool(row.get('audit_skipped', False)),
                        overall_cv_risk_score=int(row.get('overall_cv_risk_score')) if not pd.isna(row.get('overall_cv_risk_score')) else None,
                        overall_cv_risk_level=row.get('overall_cv_risk_level'),
                        risk_factors_count=int(row.get('risk_factors_count')) if not pd.isna(row.get('risk_factors_count')) else None,
                        created_at=datetime.now(),
                        completed_at=pd.to_datetime(row.get('tests_completed_at', datetime.now()))
                    )
                    db.add(test_result)
                    imported_tests += 1
                    created_records += 1
                
                db.commit()
                
                import_time = time.time() - start_time
                
                return {
                    "success": True,
                    "imported_users": imported_users,
                    "imported_surveys": imported_surveys,
                    "imported_tests": imported_tests,
                    "updated_records": updated_records,
                    "created_records": created_records,
                    "import_time": import_time
                }
                
            except Exception as e:
                db.rollback()
                raise e
            finally:
                db.close()
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _import)

# =========================== ИСТОРИЯ РАССЫЛОК ===========================

@admin_router.callback_query(F.data == "broadcast_history")
async def broadcast_history(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """История рассылок"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    try:
        # Получаем историю рассылок из БД
        def _get_history():
            from database import BroadcastLog, get_db_sync
            db = get_db_sync()
            try:
                logs = db.query(BroadcastLog).order_by(BroadcastLog.created_at.desc()).limit(10).all()
                return logs
            finally:
                db.close()
        
        loop = asyncio.get_event_loop()
        logs = await loop.run_in_executor(None, _get_history)
        
        if not logs:
            text = """📊 <b>ИСТОРИЯ РАССЫЛОК</b>

📭 История рассылок пуста.

Все отправленные рассылки будут отображаться здесь."""
        else:
            text = """📊 <b>ИСТОРИЯ РАССЫЛОК</b>

<b>Последние 10 рассылок:</b>

"""
            
            for i, log in enumerate(logs, 1):
                status = "✅" if log.sent_count > 0 else "❌"
                date_str = log.created_at.strftime('%d.%m %H:%M')
                success_rate = (log.sent_count / log.total_users * 100) if log.total_users > 0 else 0
                
                text += f"""{status} <b>{i}. {log.broadcast_type}</b>
📅 {date_str} | 👥 {log.sent_count}/{log.total_users} ({success_rate:.1f}%)
📝 {log.message_text[:50]}{'...' if len(log.message_text) > 50 else ''}

"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="broadcast_history")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_broadcast_menu")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка загрузки истории: {e}")

# =========================== БЫСТРЫЕ КОМАНДЫ ===========================

@admin_router.message(Command("stats"))
async def quick_stats(message: Message, state: FSMContext, is_admin: bool = False):
    """Быстрая статистика"""
    from database.analytics import admin_get_stats
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    try:
        stats = await admin_get_stats()
        
        text = f"""📊 <b>Статистика бота</b>

👥 Всего: {stats['total_users']}
✅ Завершили диагностику: {stats['completed_diagnostic']}
📝 Прошли опрос: {stats['completed_surveys']}
🧪 Прошли тесты: {stats['completed_tests']}

📈 Конверсия: {stats['completed_diagnostic']/max(stats['total_users'], 1)*100:.1f}%"""
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@admin_router.message(Command("export"))
async def quick_export(message: Message, state: FSMContext, is_admin: bool = False):
    """Быстрый экспорт"""
    from database.analytics import admin_export_data
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    await message.answer("⏳ Создаю экспорт...")
    
    try:
        filename = await admin_export_data()
        
        if os.path.exists(filename):
            document = FSInputFile(filename)
            await message.answer_document(
                document, 
                caption="📥 Экспорт базы данных готов"
            )
            os.remove(filename)
        else:
            await message.answer("❌ Ошибка создания файла")
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@admin_router.message(Command("broadcast"))
async def quick_broadcast_menu(message: Message, state: FSMContext, is_admin: bool = False):
    """Быстрый доступ к рассылкам"""
    from database.analytics import admin_get_stats
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    # Проверяем авторизацию
    admin_session = await state.get_data()
    if not admin_session.get('admin_authenticated'):
        await message.answer("🔐 Для доступа к рассылкам введите /admin")
        return
    
    # Показываем меню рассылок
    try:
        stats = await admin_get_stats()
        
        text = f"""📤 <b>БЫСТРЫЕ РАССЫЛКИ</b>

📊 Доступно пользователей: {stats['total_users']}

Выберите тип рассылки:"""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Всем пользователям", callback_data="quick_broadcast_all")],
            [InlineKeyboardButton(text="✅ Завершившим", callback_data="quick_broadcast_completed")],
            [InlineKeyboardButton(text="📋 Ручной список ID", callback_data="broadcast_manual_ids")],
            [InlineKeyboardButton(text="🧪 Тест админам", callback_data="broadcast_test")]
        ])
        
        await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@admin_router.callback_query(F.data.startswith("quick_broadcast_"))
async def quick_broadcast_filter(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Быстрые фильтры рассылки"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    filter_type = callback.data.replace("quick_broadcast_", "")
    await state.update_data(broadcast_filter=filter_type)
    
    filter_names = {
        "all": "всем пользователям",
        "completed": "завершившим диагностику"
    }
    
    text = f"""✍️ <b>БЫСТРАЯ РАССЫЛКА</b>

<b>Получатели:</b> {filter_names.get(filter_type, filter_type)}

Введите текст сообщения:"""
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await state.set_state(AdminStates.waiting_broadcast_text)

# =========================== СПРАВКА ===========================

@admin_router.message(Command("adminhelp"))
async def admin_help(message: Message, state: FSMContext, is_admin: bool = False):
    """Справка по админке"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    text = """🔧 <b>АДМИНКА v2.0 - СПРАВКА</b>

<b>🎛 Основные команды:</b>
/admin - Главная панель админки
/stats - Быстрая статистика
/export - Экспорт базы в Excel
/broadcast - Быстрые рассылки
/adminhelp - Эта справка

<b>📤 СИСТЕМА РАССЫЛОК:</b>
• <b>По БД</b> - выбор фильтра из базы
• <b>По ID</b> - ручной список через запятую
• <b>Тест</b> - отправка только админам
• <b>История</b> - логи предыдущих рассылок

<b>💾 ИМПОРТ БАЗЫ:</b>
• Загрузка из Excel файлов экспорта
• Автоматическое создание бэкапа
• Валидация данных перед импортом
• Детальный анализ структуры файла

<b>📊 ЭКСПОРТ ВКЛЮЧАЕТ:</b>
• Лист "Все данные" - полная информация
• Отдельные листы по категориям
• Статистику и аналитику
• Историю активности

<b>🔐 БЕЗОПАСНОСТЬ:</b>
• Авторизация по паролю
• Права доступа по ADMIN_IDS
• Автоматические бэкапы при импорте
• Логирование всех операций

<b>⚡ ФИЛЬТРЫ РАССЫЛОК:</b>
• Все пользователи
• Завершившие диагностику
• Не завершившие диагностику
• Прошедшие только опрос
• Прошедшие только тесты
• Ручной список ID

<b>📋 ФОРМАТ РУЧНЫХ ID:</b>
123456789, 987654321, 555444333

<b>🎯 ПОДДЕРЖКА MARKDOWN:</b>
**жирный**, *курсив*, [ссылка](url)

Все рассылки логируются и доступны в истории."""
    
    await message.answer(text, parse_mode="HTML")

# =========================== УПРАВЛЕНИЕ РАССЫЛКАМИ ДЛЯ ЭКСТРЕННЫХ СЛУЧАЕВ ===========================

@admin_router.message(Command("emergency_broadcast"))
async def emergency_broadcast(message: Message, state: FSMContext, is_admin: bool = False):
    """Экстренная рассылка без дополнительных проверок"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    # Проверяем формат команды: /emergency_broadcast ID1,ID2,ID3 Текст сообщения
    parts = message.text.split(' ', 2)
    if len(parts) < 3:
        await message.answer("""🚨 <b>ЭКСТРЕННАЯ РАССЫЛКА</b>

<b>Формат команды:</b>
/emergency_broadcast ID1,ID2,ID3 Текст сообщения

<b>Пример:</b>
/emergency_broadcast 123456789,987654321 Важное сообщение для пользователей

<b>⚠️ Внимание:</b>
• Рассылка выполняется немедленно
• Без дополнительных подтверждений
• Только для экстренных случаев""", parse_mode="HTML")
        return
    
    ids_str = parts[1]
    message_text = parts[2]
    
    # Парсим ID
    try:
        target_ids = []
        for id_str in ids_str.split(','):
            id_str = id_str.strip()
            if id_str.isdigit():
                target_ids.append(int(id_str))
        
        if not target_ids:
            await message.answer("❌ Некорректные ID")
            return
        
        await message.answer(f"🚨 Экстренная рассылка для {len(target_ids)} получателей...")
        
        # Выполняем рассылку
        result = await send_broadcast_to_ids(message.bot, target_ids, message_text)
        
        result_text = f"""✅ <b>ЭКСТРЕННАЯ РАССЫЛКА ЗАВЕРШЕНА</b>

📊 Результат:
• Получателей: {result['total']}
• Отправлено: {result['sent']}
• Ошибок: {result['errors']}
• Успешность: {(result['sent']/result['total']*100 if result['total'] > 0 else 0):.1f}%

🕐 {datetime.now().strftime('%H:%M:%S')}"""
        
        await message.answer(result_text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка экстренной рассылки: {e}")

@admin_router.message(Command("db_broadcast"))
async def db_broadcast_command(message: Message, state: FSMContext, is_admin: bool = False):
    """Быстрая рассылка по базе через команду"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    # Формат: /db_broadcast all|completed|uncompleted Текст сообщения
    parts = message.text.split(' ', 2)
    if len(parts) < 3:
        await message.answer("""📤 <b>РАССЫЛКА ПО БАЗЕ</b>

<b>Формат команды:</b>
/db_broadcast фильтр Текст сообщения

<b>Доступные фильтры:</b>
• all - всем пользователям
• completed - завершившим диагностику
• uncompleted - не завершившим
• survey - прошедшим опрос
• tests - прошедшим тесты

<b>Пример:</b>
/db_broadcast completed Поздравляем с завершением диагностики!""", parse_mode="HTML")
        return
    
    filter_type = parts[1]
    message_text = parts[2]
    
    if filter_type not in ['all', 'completed', 'uncompleted', 'survey', 'tests']:
        await message.answer("❌ Некорректный фильтр. Доступны: all, completed, uncompleted, survey, tests")
        return
    
    try:
        await message.answer(f"📤 Выполняю рассылку по фильтру '{filter_type}'...")
        
        # Получаем ID пользователей
        target_ids = await get_user_ids_by_filter(filter_type)
        
        if not target_ids:
            await message.answer("❌ Пользователи по данному фильтру не найдены")
            return
        
        # Выполняем рассылку
        result = await send_broadcast_to_ids(message.bot, target_ids, message_text)
        
        result_text = f"""✅ <b>РАССЫЛКА ПО БД ЗАВЕРШЕНА</b>

<b>Фильтр:</b> {filter_type}
<b>Получателей:</b> {result['total']}
<b>Отправлено:</b> {result['sent']}
<b>Ошибок:</b> {result['errors']}
<b>Успешность:</b> {(result['sent']/result['total']*100 if result['total'] > 0 else 0):.1f}%

🕐 {datetime.now().strftime('%H:%M:%S')}"""
        
        await message.answer(result_text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка рассылки по БД: {e}")

# =========================== СИСТЕМА РЕЗЕРВНОГО КОПИРОВАНИЯ ===========================

@admin_router.message(Command("backup"))
async def manual_backup(message: Message, state: FSMContext, is_admin: bool = False):
    """Ручное создание бэкапа"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    await message.answer("💾 Создаю резервную копию...")
    
    try:
        backup_path = await create_database_backup()
        
        if backup_path and os.path.exists(backup_path):
            document = FSInputFile(backup_path)
            await message.answer_document(
                document,
                caption=f"💾 Резервная копия базы данных\n🕐 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
            )
            # Удаляем временный файл бэкапа после отправки
            os.remove(backup_path)
        else:
            await message.answer("❌ Не удалось создать резервную копию")
            
    except Exception as e:
        await message.answer(f"❌ Ошибка создания бэкапа: {e}")

# =========================== ОТЛАДОЧНЫЕ КОМАНДЫ ===========================

@admin_router.message(Command("debug_db"))
async def debug_database(message: Message, state: FSMContext, is_admin: bool = False):
    """Отладочная информация о базе данных"""
    if not is_admin:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    try:
        def _debug():
            from database import get_db_sync, User, Survey, TestResult, ActivityLog
            db = get_db_sync()
            try:
                users_count = db.query(User).count()
                surveys_count = db.query(Survey).count()
                tests_count = db.query(TestResult).count()
                logs_count = db.query(ActivityLog).count()
                
                # Проверяем целостность
                users_with_surveys = db.query(User).filter(User.survey_completed == True).count()
                actual_surveys = surveys_count
                
                users_with_tests = db.query(User).filter(User.tests_completed == True).count()
                actual_tests = tests_count
                
                return {
                    'users_count': users_count,
                    'surveys_count': surveys_count,
                    'tests_count': tests_count,
                    'logs_count': logs_count,
                    'users_with_surveys': users_with_surveys,
                    'actual_surveys': actual_surveys,
                    'users_with_tests': users_with_tests,
                    'actual_tests': actual_tests,
                    'db_size': os.path.getsize("cardio_bot.db") / 1024 / 1024 if os.path.exists("cardio_bot.db") else 0
                }
            finally:
                db.close()
        
        loop = asyncio.get_event_loop()
        debug_info = await loop.run_in_executor(None, _debug)
        
        text = f"""🐛 <b>ОТЛАДКА БАЗЫ ДАННЫХ</b>

<b>📊 Количество записей:</b>
• Пользователи: {debug_info['users_count']}
• Опросы: {debug_info['surveys_count']}
• Результаты тестов: {debug_info['tests_count']}
• Логи активности: {debug_info['logs_count']}

<b>🔗 Целостность данных:</b>
• Пользователей с флагом опроса: {debug_info['users_with_surveys']}
• Фактических опросов: {debug_info['actual_surveys']}
• Соответствие опросов: {'✅' if debug_info['users_with_surveys'] == debug_info['actual_surveys'] else '❌'}

• Пользователей с флагом тестов: {debug_info['users_with_tests']}
• Фактических тестов: {debug_info['actual_tests']}
• Соответствие тестов: {'✅' if debug_info['users_with_tests'] == debug_info['actual_tests'] else '❌'}

<b>💾 Размер БД:</b> {debug_info['db_size']:.2f} МБ

<b>📁 Файл БД:</b> {'✅' if os.path.exists("cardio_bot.db") else '❌'} cardio_bot.db"""
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка отладки: {e}")

@admin_router.callback_query(F.data == "admin_export")
async def export_data(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Экспорт данных"""
    from database.analytics import admin_export_data

    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("⏳ Подготавливаю экспорт...")
    
    try:
        filename = await admin_export_data()
        
        if os.path.exists(filename):
            document = FSInputFile(filename)
            await callback.message.answer_document(
                document, 
                caption="📥 Полный экспорт базы данных готов"
            )
            os.remove(filename)
            await show_admin_panel(callback.message)
        else:
            await callback.message.edit_text("❌ Ошибка создания файла")
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}")

@admin_router.callback_query(F.data == "admin_clean")
async def clean_data_menu(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Меню очистки данных"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    text = """🗑 <b>Очистка старых данных</b>

⚠️ Внимание! Это действие нельзя отменить.

Будут удалены только технические данные:
• Старые логи активности
• Старые логи рассылок
• Устаревшая системная статистика

<b>Данные пользователей остаются нетронутыми!</b>"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 30 дней", callback_data="clean_30")],
        [InlineKeyboardButton(text="🗑 60 дней", callback_data="clean_60")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)

@admin_router.callback_query(F.data.startswith("clean_"))
async def clean_old_data_action(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Очистка старых данных"""
    from .migration_utils import clean_duplicate_users
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    
    days = int(callback.data.split("_")[1])
    await callback.message.edit_text(f"⏳ Удаляю данные старше {days} дней...")
    
    try:
        def _clean():
            return clean_duplicate_users()
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, _clean)
        
        text = f"""✅ <b>Очистка завершена</b>

Удалено за {days} дней:
• Логов активности: {result.get('deleted_activity_logs', 0)}
• Логов рассылок: {result.get('deleted_broadcast_logs', 0)}
• Системной статистики: {result.get('deleted_system_stats', 0)}

💾 Основные данные сохранены."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}")

@admin_router.callback_query(F.data == "admin_id_diagnostic")
async def id_diagnostic(callback: CallbackQuery, state: FSMContext, is_admin: bool = False):
    """Диагностика пользовательских ID"""
    if not await check_admin_auth(callback, state, is_admin):
        return
    
    await callback.answer()
    await callback.message.edit_text("🔍 Анализирую пользовательские ID...")
    
    try:
        from .migration_utils import admin_id_diagnostic
        
        # Быстрая диагностика
        diagnostic = await admin_id_diagnostic()
        
        text = f"""🔍 <b>ДИАГНОСТИКА ПОЛЬЗОВАТЕЛЬСКИХ ID</b>

👥 <b>Общая статистика:</b>
• Всего пользователей: {diagnostic['total_users']}
• Корректных ID: {diagnostic['valid_users']}
• Проблемных ID: {diagnostic['problematic_users']}
• Готовых для рассылки: {diagnostic['broadcast_ready_ids']}

📊 <b>Статус:</b> {diagnostic['summary']}

{('⚠️ <b>Обнаружены проблемы с ID!</b>' if diagnostic['has_problems'] else '✅ <b>Все ID корректны</b>')}

💡 <b>Информация:</b>
Корректные telegram_id пользователей имеют длину 6-10 цифр.
Меньшие значения могут быть message_id или chat_id."""
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_back")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка диагностики: {e}")

