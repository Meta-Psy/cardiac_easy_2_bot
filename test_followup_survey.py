"""
Скрипт для тестирования опроса ТОЧКА 3
Отправляет начальное сообщение вашему Telegram ID
"""
import sys
import os
import asyncio
import io

# Настройка кодировки для Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Добавляем bot в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from aiogram import Bot
from config import BOT_TOKEN
from handlers.followup_broadcast import broadcast_followup_initial_message

async def test_send_message():
    """Отправка тестового сообщения"""
    bot = Bot(token=BOT_TOKEN)

    # ЗАМЕНИТЕ на ваш Telegram ID
    YOUR_TELEGRAM_ID = input("Введите ваш Telegram ID: ")

    try:
        user_id = int(YOUR_TELEGRAM_ID)
        print(f"\n📤 Отправляю начальное сообщение пользователю {user_id}...")

        success = await broadcast_followup_initial_message(bot, user_id)

        if success:
            print("✅ Сообщение успешно отправлено!")
            print("\n📱 Проверьте свой Telegram - должно прийти сообщение с кнопкой 'Начать опрос'")
            print("\n🔬 Теперь пройдите весь опрос и проверьте:")
            print("   1. Все 13 вопросов работают")
            print("   2. Логика переходов (пропуск вопросов 2-6 если не ходили к врачу)")
            print("   3. Множественный выбор работает")
            print("   4. Шкалы оценки (0-10, 1-10) работают")
            print("   5. Текстовые ответы принимаются")
            print("   6. В конце приходит методичка ЗОЖ")
        else:
            print("❌ Ошибка отправки (возможно уже отправляли)")

    except ValueError:
        print("❌ Неверный формат Telegram ID")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await bot.session.close()

if __name__ == "__main__":
    print("🧪 ТЕСТИРОВАНИЕ ОПРОСА ТОЧКА 3")
    print("="*50)
    print("\nЭтот скрипт отправит вам начальное сообщение.")
    print("Вам нужно узнать свой Telegram ID.")
    print("\nКак узнать свой Telegram ID:")
    print("1. Напишите боту @userinfobot в Telegram")
    print("2. Он покажет ваш ID")
    print("\nИли используйте ваш ID из базы данных бота.\n")

    asyncio.run(test_send_message())
