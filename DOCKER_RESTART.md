# Команды для перезапуска бота на Docker

## 📋 Быстрый перезапуск (одна команда)

```bash
cd /path/to/cardio_2_easy_bot && \
git pull origin master && \
docker-compose down && \
docker-compose build --no-cache && \
docker-compose up -d && \
docker-compose logs -f --tail=50
```

---

## 📝 Пошаговые команды

### 1. Подключиться к серверу (если на удаленном сервере)
```bash
ssh user@your-server-ip
```

### 2. Перейти в директорию проекта
```bash
cd /path/to/cardio_2_easy_bot
```

### 3. Обновить код из GitHub
```bash
git pull origin master
```

**Ожидаемый вывод:**
```
remote: Enumerating objects: X, done.
remote: Counting objects: 100% (X/X), done.
...
Updating 3464aa6..6dd5c36
Fast-forward
 9 files changed, 612 insertions(+), 475 deletions(-)
```

### 4. Остановить текущий контейнер
```bash
docker-compose down
```

**Ожидаемый вывод:**
```
Stopping cardio_bot ... done
Removing cardio_bot ... done
Removing network cardio_2_easy_bot_default
```

### 5. Пересобрать Docker образ
```bash
docker-compose build --no-cache
```

**Флаг `--no-cache`** - пересобирает образ с нуля (рекомендуется после изменений кода)

**Ожидаемый вывод:**
```
Building cardio_bot
Step 1/9 : FROM python:3.11-slim
...
Successfully built abc123def456
Successfully tagged cardio_2_easy_bot_cardio_bot:latest
```

### 6. Запустить контейнер
```bash
docker-compose up -d
```

**Флаг `-d`** - запуск в фоновом режиме (detached)

**Ожидаемый вывод:**
```
Creating network "cardio_2_easy_bot_default" with the default driver
Creating cardio_bot ... done
```

### 7. Проверить логи (ВАЖНО!)
```bash
docker-compose logs -f --tail=50
```

**Флаги:**
- `-f` - следить за логами в реальном времени (Ctrl+C для выхода)
- `--tail=50` - показать последние 50 строк

**Что должно быть в логах:**
```
✅ База данных инициализирована
✅ Команды бота настроены
УСПЕХ: Подключение к Telegram. Бот: @your_bot_name
✅ Защищенный middleware зарегистрирован
✅ Административный middleware зарегистрирован
✅ Диспетчер настроен с защитой состояний
Запуск polling с защитой от зацикливания...
```

---

## 🔍 Дополнительные команды для проверки

### Проверить статус контейнера
```bash
docker-compose ps
```

**Ожидаемый вывод:**
```
   Name                 Command            State    Ports
-----------------------------------------------------------
cardio_bot   python main.py             Up
```

### Проверить логи (без follow)
```bash
docker-compose logs --tail=100
```

### Войти в контейнер (для отладки)
```bash
docker-compose exec cardio_bot bash
```

### Проверить переменные окружения
```bash
docker-compose exec cardio_bot env | grep BOT
```

### Перезапустить без пересборки
```bash
docker-compose restart
```

---

## ⚠️ Решение проблем

### Если контейнер не запускается

**1. Проверить логи ошибок:**
```bash
docker-compose logs cardio_bot
```

**2. Проверить переменные окружения:**
```bash
cat .env
```

Убедитесь, что есть:
```
BOT_TOKEN=ваш_токен
ADMIN_IDS=123456789,987654321
ADMIN_PASSWORD=ваш_пароль
```

**3. Очистить все Docker ресурсы (крайняя мера):**
```bash
docker-compose down -v
docker system prune -a
docker-compose up -d --build
```

### Если бот не отвечает

**1. Проверить подключение к Telegram:**
```bash
docker-compose logs | grep "Telegram"
```

Должно быть: `УСПЕХ: Подключение к Telegram`

**2. Проверить, что контейнер запущен:**
```bash
docker ps | grep cardio_bot
```

**3. Проверить токен:**
```bash
docker-compose exec cardio_bot python -c "import os; print('Token exists:', bool(os.getenv('BOT_TOKEN')))"
```

---

## 📊 Проверка после деплоя

### 1. Отправить боту `/start`
Должно прийти приветственное сообщение из recomendations.txt

### 2. Проверить команды
```
/start - должно работать ✅
/score - должно работать ✅
/help - НЕ должно работать (удалена) ❌
/status - НЕ должно работать (удалена) ❌
```

### 3. Пройти регистрацию
- Ввести имя
- Ввести email
- Отправить телефон

### 4. Проверить переход к тестам
После регистрации должны СРАЗУ появиться тесты (БЕЗ опроса)

### 5. Проверить тесты
Должны быть доступны:
- ✅ HADS (тревога и депрессия)
- ✅ Бернса (выгорание)
- ✅ ISI (качество сна)
- ✅ STOP-BANG (апноэ)
- ✅ ESS (сонливость)
- ✅ Фагерстрема (курение) + кнопка "Я не курю"
- ✅ AUDIT (алкоголь) + кнопка "Я не употребляю"

---

## 🔄 Откат на предыдущую версию (если что-то пошло не так)

```bash
cd /path/to/cardio_2_easy_bot
git checkout 3464aa6
docker-compose down
docker-compose build --no-cache
docker-compose up -d
docker-compose logs -f
```

Для возврата на новую версию:
```bash
git checkout master
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

---

## 📝 Заметки

- **База данных** сохраняется в `./data` (volume)
- **Материалы** берутся из `./materials` (volume)
- **Логи** можно посмотреть в любой момент: `docker-compose logs -f`
- **Автоперезапуск** настроен (`restart: unless-stopped`)
- **Пересборка обязательна** после изменений в коде

---

## ✅ Чек-лист успешного деплоя

- [ ] git pull выполнен успешно
- [ ] docker-compose build завершился без ошибок
- [ ] docker-compose up запустил контейнер
- [ ] В логах есть "УСПЕХ: Подключение к Telegram"
- [ ] В логах есть "Запуск polling"
- [ ] Бот отвечает на /start
- [ ] Бот отвечает на /score
- [ ] После регистрации сразу тесты (без опроса)
- [ ] Материалы отправляются после тестов
- [ ] В БД сохраняются результаты

---

## 🆘 Контакты для помощи

Если возникли проблемы:
1. Проверьте логи: `docker-compose logs -f`
2. Проверьте статус: `docker-compose ps`
3. Проверьте .env файл
4. Откатитесь на предыдущую версию
5. Свяжитесь с разработчиком

**Последний коммит:** 6dd5c36 - "Приведение бота в соответствие с recomendations.txt"
**Дата:** 19.12.2025
