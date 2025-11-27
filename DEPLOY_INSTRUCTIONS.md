# 🚀 Инструкция по деплою опроса ТОЧКА 3 на сервер

## Список файлов для деплоя

### Новые файлы (обязательно загрузить):
```
bot/handlers/followup_survey.py          # Handler опроса (1076 строк)
bot/handlers/followup_broadcast.py       # Рассылка начального сообщения
apply_migration.py                        # Скрипт миграции БД
materials/Методичка ЗОЖ.pdf              # Бонусный материал
```

### Измененные файлы (обязательно обновить):
```
bot/handlers/__init__.py                  # Добавлены импорты followup модулей
bot/database/models.py                    # Добавлены модели FollowUpSurvey, FollowUpStatus
bot/database/__init__.py                  # Добавлены экспорты новых моделей
bot/admin/handlers.py                     # Добавлены обработчики для рассылки ТОЧКА 3
```

### Вспомогательные файлы (опционально):
```
FOLLOWUP_SURVEY_README.md                 # Документация
test_followup_survey.py                   # Тестовый скрипт
check_users.py                            # Проверка пользователей
full_migration.py                         # Полная миграция (резервная)
```

---

## Шаги на сервере

### Шаг 1: Остановить бота

```bash
# Если бот запущен через systemd
sudo systemctl stop cardio_bot

# Если через screen/tmux - найдите процесс и остановите
# Или просто Ctrl+C в сессии где запущен бот
```

### Шаг 2: Создать резервную копию

```bash
# Резервная копия базы данных (ОБЯЗАТЕЛЬНО!)
cd /path/to/cardio_2_easy_bot/bot
cp cardio_bot.db cardio_bot.db.backup_$(date +%Y%m%d_%H%M%S)

# Резервная копия кода (опционально)
cd /path/to/cardio_2_easy_bot
tar -czf backup_$(date +%Y%m%d_%H%M%S).tar.gz bot/ materials/
```

### Шаг 3: Загрузить новые/измененные файлы

```bash
# Через SCP (с локального компьютера)
scp bot/handlers/followup_survey.py user@server:/path/to/cardio_2_easy_bot/bot/handlers/
scp bot/handlers/followup_broadcast.py user@server:/path/to/cardio_2_easy_bot/bot/handlers/
scp apply_migration.py user@server:/path/to/cardio_2_easy_bot/
scp "materials/Методичка ЗОЖ.pdf" user@server:/path/to/cardio_2_easy_bot/materials/

# Обновить измененные файлы
scp bot/handlers/__init__.py user@server:/path/to/cardio_2_easy_bot/bot/handlers/
scp bot/database/models.py user@server:/path/to/cardio_2_easy_bot/bot/database/
scp bot/database/__init__.py user@server:/path/to/cardio_2_easy_bot/bot/database/
scp bot/admin/handlers.py user@server:/path/to/cardio_2_easy_bot/bot/admin/

# Или через Git (если используете)
cd /path/to/cardio_2_easy_bot
git pull origin master
```

### Шаг 4: Проверить права доступа

```bash
cd /path/to/cardio_2_easy_bot

# Проверить что файлы читаемы
ls -la bot/handlers/followup_*.py
ls -la "materials/Методичка ЗОЖ.pdf"

# Если нужно - поправить владельца
sudo chown -R your_user:your_group bot/
sudo chown -R your_user:your_group materials/
```

### Шаг 5: Применить миграцию базы данных

```bash
cd /path/to/cardio_2_easy_bot

# ВАЖНО: Убедитесь что база данных не используется (бот остановлен!)

# Применить миграцию
python3 apply_migration.py
```

**Ожидаемый вывод:**
```
>> Начинаю миграцию базы данных...
[OK] Миграция успешно применена!

Добавлены следующие таблицы:
  - followup_surveys (опрос ТОЧКА 3, 13 вопросов)
  - followup_status (статус рассылки и прохождения)

>> Проверяю создание таблиц...
[OK] Обе таблицы успешно созданы!

[SUCCESS] Миграция завершена! Бот готов к работе с опросом ТОЧКА 3
```

**Если ошибка:**
```bash
# Проверить структуру БД
cd bot
sqlite3 cardio_bot.db ".tables"

# Должны быть таблицы: followup_surveys, followup_status

# Если таблиц нет - запустить миграцию еще раз
cd ..
python3 apply_migration.py
```

### Шаг 6: Проверить базу данных

```bash
cd /path/to/cardio_2_easy_bot/bot

# Проверить созданные таблицы
sqlite3 cardio_bot.db "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'followup%';"

# Должен вывести:
# followup_surveys
# followup_status

# Проверить количество зарегистрированных пользователей
sqlite3 cardio_bot.db "SELECT COUNT(*) FROM users WHERE registration_completed = 1;"
```

### Шаг 7: Запустить бота

```bash
cd /path/to/cardio_2_easy_bot/bot

# Если через systemd
sudo systemctl start cardio_bot
sudo systemctl status cardio_bot

# Если через screen
screen -S cardio_bot
python3 main.py
# Ctrl+A, D для detach

# Если через tmux
tmux new -s cardio_bot
python3 main.py
# Ctrl+B, D для detach

# Если через nohup
nohup python3 main.py > bot.log 2>&1 &
```

### Шаг 8: Проверить логи

```bash
# Если через systemd
sudo journalctl -u cardio_bot -f

# Если через screen/tmux
screen -r cardio_bot  # или tmux attach -t cardio_bot

# Если через nohup
tail -f bot.log
```

**Ищите в логах:**
```
INFO:__main__:Bot started successfully
```

**НЕ должно быть:**
```
ERROR: ... followup_surveys ...
ModuleNotFoundError: No module named 'handlers.followup_survey'
```

---

## Тестирование на сервере

### Тест 1: Проверка через админ-панель

1. Откройте бота в Telegram
2. Отправьте `/admin`
3. Введите пароль администратора
4. Выберите **"Рассылки"**
5. Должна быть кнопка: **"📅 РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)"**

**Если кнопки нет:**
- Проверьте что файл `bot/admin/handlers.py` обновлен
- Проверьте логи на ошибки
- Перезапустите бота

### Тест 2: Проверка статистики

1. Нажмите на кнопку **"📅 РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)"**
2. Должно показать статистику:
```
📅 РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)

📊 Статистика:
• Всего пользователей: X
• Уже получили сообщение: 0
• Будет отправлено: X
```

**Если ошибка:**
- Проверьте что миграция применена
- Проверьте логи

### Тест 3: Тестовая рассылка ОДНОМУ пользователю

**ВАЖНО: НЕ запускайте массовую рассылку сразу!**

Создайте тестовый скрипт на сервере:

```bash
cd /path/to/cardio_2_easy_bot
nano test_single_user.py
```

Вставьте код:
```python
import sys
import os
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'bot'))

from aiogram import Bot
from config import BOT_TOKEN
from handlers.followup_broadcast import broadcast_followup_initial_message

async def test():
    bot = Bot(token=BOT_TOKEN)

    # ЗАМЕНИТЕ на ваш Telegram ID
    YOUR_ID = 71353121  # <-- ВАШ ID ЗДЕСЬ

    print(f"Sending message to {YOUR_ID}...")
    success = await broadcast_followup_initial_message(bot, YOUR_ID)

    if success:
        print("SUCCESS! Check your Telegram")
    else:
        print("FAILED or already sent")

    await bot.session.close()

asyncio.run(test())
```

Запустите:
```bash
python3 test_single_user.py
```

### Тест 4: Пройти опрос

После получения сообщения:

1. ✅ Проверьте текст начального сообщения
2. ✅ Нажмите кнопку **"Начать опрос"**
3. ✅ Пройдите все 13 вопросов:
   - Вопрос 1: Выберите любой вариант
   - Если выбрали "Да" → пройдите вопросы 2-6
   - Если выбрали "Нет" → сразу вопрос 7
   - Вопросы с множественным выбором - выберите несколько
   - Вопросы со шкалой - выберите цифру
   - Текстовые вопросы - введите любой текст
4. ✅ После вопроса 13 должны получить:
   - Благодарственное сообщение
   - Файл **"Методичка_ЗОЖ.pdf"**

### Тест 5: Проверить данные в БД

```bash
cd /path/to/cardio_2_easy_bot/bot

# Проверить что опрос сохранен
sqlite3 cardio_bot.db "SELECT telegram_id, completed_at FROM followup_surveys ORDER BY id DESC LIMIT 1;"

# Проверить статус
sqlite3 cardio_bot.db "SELECT telegram_id, initial_message_sent, survey_completed, bonus_sent FROM followup_status ORDER BY id DESC LIMIT 1;"
```

**Ожидается:**
- `completed_at` - заполнена дата
- `survey_completed` - 1
- `bonus_sent` - 1

### Тест 6: Повторная отправка (защита)

Запустите еще раз:
```bash
python3 test_single_user.py
```

Должно вывести:
```
FAILED or already sent
```

И в логах бота должно быть:
```
INFO: Пользователю X уже отправлено начальное сообщение ТОЧКА 3
```

---

## Массовая рассылка (после успешных тестов)

### ⚠️ ВНИМАНИЕ: Делайте только после всех проверок!

1. Войдите в админ-панель `/admin`
2. Рассылки → **"📅 РАССЫЛКА ОПРОСА 3+ МЕСЯЦА (ТОЧКА 3)"**
3. Проверьте статистику
4. Нажмите **"✅ ЗАПУСТИТЬ РАССЫЛКУ"**
5. Дождитесь завершения (может занять несколько минут)
6. Получите отчет:
```
✅ РАССЫЛКА ОПРОСА ТОЧКА 3 ЗАВЕРШЕНА

📊 Результаты:
• Всего пользователей в БД: X
• Целевая аудитория: Y
• Успешно отправлено: Z
• Ошибок отправки: 0
• Успешность: 100%
```

---

## Troubleshooting

### Проблема: "ModuleNotFoundError: No module named 'handlers.followup_survey'"

**Решение:**
```bash
# Проверить что файлы загружены
ls -la bot/handlers/followup_*.py

# Проверить что __init__.py обновлен
grep "followup" bot/handlers/__init__.py

# Перезапустить бота
```

### Проблема: "no such table: followup_surveys"

**Решение:**
```bash
# Применить миграцию
python3 apply_migration.py

# Проверить таблицы
cd bot
sqlite3 cardio_bot.db ".tables"
```

### Проблема: "Файл методички не найден"

**Решение:**
```bash
# Проверить файл
ls -la "materials/Методичка ЗОЖ.pdf"

# Если нет - загрузить заново
scp "materials/Методичка ЗОЖ.pdf" user@server:/path/to/cardio_2_easy_bot/materials/

# Проверить права
chmod 644 "materials/Методичка ЗОЖ.pdf"
```

### Проблема: Кнопка не появляется в админ-панели

**Решение:**
```bash
# Проверить что admin/handlers.py обновлен
grep "broadcast_followup" bot/admin/handlers.py

# Должно найти:
# - def followup_broadcast
# - def confirm_followup_broadcast
# - callback_data="broadcast_followup"

# Перезапустить бота
```

### Проблема: Рассылка не запускается

**Решение:**
```bash
# Проверить логи
tail -f bot/bot.log  # или journalctl -u cardio_bot -f

# Проверить что миграция применена
cd bot
sqlite3 cardio_bot.db "SELECT COUNT(*) FROM followup_status;"

# Если ошибка - применить миграцию заново
cd ..
python3 apply_migration.py
```

---

## Откат изменений (если что-то пошло не так)

### Быстрый откат:

```bash
# Остановить бота
sudo systemctl stop cardio_bot

# Восстановить базу данных
cd /path/to/cardio_2_easy_bot/bot
cp cardio_bot.db.backup_YYYYMMDD_HHMMSS cardio_bot.db

# Восстановить код
cd ..
tar -xzf backup_YYYYMMDD_HHMMSS.tar.gz

# Запустить бота
sudo systemctl start cardio_bot
```

---

## Финальный чеклист перед массовой рассылкой

- [ ] Бот запущен и работает без ошибок
- [ ] Миграция применена успешно
- [ ] Таблицы `followup_surveys` и `followup_status` созданы
- [ ] Файл методички на месте
- [ ] Тестовая отправка одному пользователю прошла успешно
- [ ] Опрос пройден полностью (13 вопросов)
- [ ] Методичка получена
- [ ] Данные сохранены в БД
- [ ] Повторная отправка заблокирована
- [ ] Нет ошибок в логах

**Только после всех галочек запускайте массовую рассылку!**

---

## Контакты для поддержки

Если возникли проблемы:
1. Проверьте логи бота
2. Проверьте базу данных
3. Проверьте что все файлы на месте
4. Изучите раздел Troubleshooting выше

**Успешного деплоя! 🚀**
