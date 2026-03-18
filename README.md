# Discord Registration Bot

Бот для регистрации участников Discord-сервера с проверкой имени, авто-сменой ника, выдачей ролей, логами, бэкапами, базой SQLite и админ-панелью.

Основа бота:
- токен берётся из `DISCORD_TOKEN`
- настройки читаются из `config.json`
- база хранится в `registrations.db`
- badwords хранятся в `badwords.json`
- используется `discord.py` и `python-dotenv`

## Что умеет бот

### Регистрация участников
- создаёт постоянное сообщение регистрации с кнопками
- принимает имя через modal-окно
- проверяет имя на:
  - только русские буквы, пробелы и дефис
  - мусорные/случайные имена
  - мат и замаскированный мат
- автоматически меняет ник в формате `БазовыйНик (Имя)`
- снимает роль незарегистрированного и выдаёт роль участника
- поддерживает смену имени с кулдауном
- при повторном входе восстанавливает ник и роли из базы

### Каналы и сообщения
- поддерживает каналы:
  - регистрации
  - правил
  - welcome
  - логов
- автоматически пересоздаёт сообщение регистрации и сообщение правил, если их удалили
- удаляет обычные сообщения пользователей в канале регистрации

### База данных
- хранит регистрации в SQLite
- хранит историю смен имени
- умеет экспортировать базу в CSV
- умеет удалять, сбрасывать и восстанавливать пользователей из базы

### Badwords
- встроенный список запрещённых слов
- отдельный пользовательский список
- экспорт в JSON
- импорт из JSON

### Админка
- `/admin_panel` открывает панель управления
- есть разделы пользователей, каналов, ролей, сообщений, badwords, бэкапов, диагностики и быстрых действий
- есть кнопки lock/unlock для registration/rules/logs через панель

### Бэкапы
- создаёт zip-бэкапы
- запускает автобэкап по интервалу
- хранит ограниченное число архивов

## Slash-команды

### Основные
- `/setup_registration` — создать или обновить сообщение регистрации
- `/admin_panel` — открыть админ-панель
- `/system_check` — диагностика прав, ролей и каналов
- `/check_name name:<имя>` — проверить, пропустит ли бот имя
- `/name_history user:<пользователь>` — показать историю смен имени

### Настройка каналов и ролей
- `/set_registration_channel channel:<канал>`
- `/set_log_channel channel:<канал>`
- `/set_welcome_channel channel:<канал>`
- `/set_unregistered_role role:<роль>`
- `/set_member_role role:<роль>`

### Настройка поведения
- `/set_rename_cooldown hours:<число>`
- `/set_backup_interval hours:<число>`
- `/set_backup_max_files count:<число>`
- `/backup_now`

### База пользователей
- `/whois user:<пользователь>`
- `/registrations_count`
- `/db_user user:<пользователь>`
- `/db_recent`
- `/db_export`
- `/db_delete_user user:<пользователь>`
- `/db_reset_user user:<пользователь>`
- `/db_restore_user user:<пользователь>`

### Badwords
- `/badwords_add word:<слово>`
- `/badwords_remove word:<слово>`
- `/badwords_list`
- `/badwords_export`
- `/badwords_import file:<json>`

## Важная заметка по текущему файлу

В коде используется `rules_channel_id` и постоянное сообщение правил, но отдельной slash-команды `/set_rules_channel` в текущем `bot.py` нет. Канал правил сейчас настраивается через UI-панель, а не отдельной slash-командой.

Ещё один момент: функция `ensure_message_pinned()` сейчас заглушка и просто возвращает `True`, то есть фактического закрепления сообщения в текущем файле нет.

## Установка

```bash
pip install -r requirements.txt
```

Создай `.env` рядом с `bot.py`:

```env
DISCORD_TOKEN=твой_токен_бота
```

Потом запусти:

```bash
python bot.py
```

## Настройка Discord Developer Portal

### 1. Создание приложения
1. Открой Discord Developer Portal.
2. Нажми **New Application**.
3. Задай название.
4. Открой раздел **Bot**.
5. Нажми **Add Bot**.
6. Скопируй токен и вставь его в `.env` как `DISCORD_TOKEN`.

### 2. Intents
В разделе **Bot** включи:
- **Server Members Intent** — обязательно

Можно оставить выключенными:
- **Message Content Intent** — в текущем коде он не используется
- **Presence Intent** — не нужен

### 3. OAuth2 / приглашение бота
В **OAuth2 -> URL Generator** выбери scopes:
- `bot`
- `applications.commands`

Рекомендуемые права бота:
- View Channels
- Send Messages
- Embed Links
- Attach Files
- Read Message History
- Manage Roles
- Manage Nicknames
- Manage Channels
- Manage Messages
- View Audit Log

После этого открой сгенерированную ссылку и добавь бота на сервер.

## Первый запуск

1. Заполни `config.json` или начни с дефолтного файла.
2. Запусти бота.
3. Выполни настройку:
   - задай каналы
   - задай роли
   - создай сообщение регистрации
4. Проверь `/system_check`.
5. Открой `/admin_panel`.

## Пример `.env`

```env
DISCORD_TOKEN=PASTE_BOT_TOKEN_HERE
```

## Структура файлов

- `bot.py` — основной код бота
- `config.json` — конфиг
- `registrations.db` — база SQLite
- `badwords.json` — пользовательские badwords
- `exports/` — экспорты CSV/JSON
- `backups/` — zip-бэкапы

## Пример config.json

Смотри готовый файл `config.json` рядом.
