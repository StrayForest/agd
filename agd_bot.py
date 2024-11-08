import logging
import psycopg2
import psycopg2.extras  # Для работы с DictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, filters
from datetime import date
import asyncio

COLUMNS = [
    "ФИО", "Документ", "Направление", "Должность", "Уровень", "Специализация", "Gmail",
    "Контактная почта", "Git email", "GitHub ник", "Портфолио 1", "Портфолио 2", 
    "Телеграм", "Discord", "Страна", "Город", "День рождения", "CV", "Источник", 
    "Метка", "Steam", "Телефон", "VK", "LinkedIn", "Комментарий", "Проекты"
]

COLUMN_TO_DB = {
    "ФИО": "name",
    "Документ": "doc_is",
    "Направление": "direction",
    "Должность": "job_position_name",
    "Уровень": "level_name",
    "Специализация": "specialization",
    "Gmail": "gmail",
    "Контактная почта": "contact_email",
    "Git email": "git_email",
    "GitHub ник": "nick_github",
    "Портфолио 1": "portfolio_1",
    "Портфолио 2": "portfolio_2",
    "Телеграм": "telegram",
    "Discord": "discord",
    "Страна": "country",
    "Город": "city",
    "День рождения": "birthday",
    "CV": "cv",
    "Источник": "referral_source",
    "Метка": "label",
    "Steam": "steam",
    "Телефон": "phone",
    "VK": "vk",
    "LinkedIn": "linkedin",
    "Комментарий": "comment",
    "Проекты": "projects"
}

def format_contact_data(contact):
    # Форматируем данные, заменяя None на "Не указано"
    formatted_contact = {
        'ФИО': contact.get('name', 'Не указано'),
        'Документ': contact.get('doc_is', 'Не указано'),
        'Направление': contact.get('direction', 'Не указано'),
        'Должность': contact.get('job_position_name', 'Не указано'),
        'Уровень': contact.get('level_name', 'Не указано'),
        'Специализация': contact.get('specialization', 'Не указано'),
        'Gmail': contact.get('gmail', 'Не указано'),
        'Контактная почта': contact.get('contact_email', 'Не указано'),
        'Git email': contact.get('git_email', 'Не указано'),
        'GitHub ник': contact.get('nick_github', 'Не указано'),
        'Портфолио 1': contact.get('portfolio_1', 'Не указано'),
        'Портфолио 2': contact.get('portfolio_2', 'Не указано'),
        'Телеграм': contact.get('telegram', 'Не указано'),
        'Discord': contact.get('discord', 'Не указано'),
        'Страна': contact.get('country', 'Не указано'),
        'Город': contact.get('city', 'Не указано'),
        'День рождения': contact.get('birthday', 'Не указано'),
        'CV': contact.get('cv', 'Не указано'),
        'Источник': contact.get('referral_source', 'Не указано'),
        'Метка': contact.get('label', 'Не указано'),
        'Steam': contact.get('steam', 'Не указано'),
        'Телефон': contact.get('phone', 'Не указано'),
        'VK': contact.get('vk', 'Не указано'),
        'LinkedIn': contact.get('linkedin', 'Не указано'),
        'Комментарий': contact.get('comment', 'Не указано'),
        'Проекты': contact.get('projects', 'Не указано'),
    }
    return formatted_contact


search_conditions = {
    "ФИО": "name ILIKE %s",
    "Проекты": "projects ILIKE %s",
    "Должность": "job_position_name ILIKE %s",
    "Отдел": "direction ILIKE %s",
    "Телеграм": "telegram ILIKE %s",
}

# Параметры подключения к базе данных
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'agd_db',
    'user': 'postgres',
    'password': '1488'
}

# Максимальная длина сообщения для Telegram
MAX_MESSAGE_LENGTH = 4096

# Функция для поиска информации по запросу
def search_contact_info(query: str, search_type: str):
    try:
        logging.info(f"Начинаем поиск по запросу: {query} для типа поиска: {search_type}")

        # Подключаемся к базе данных
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Проверка типа поиска
        if search_type not in COLUMN_TO_DB:
            logging.error(f"Неверный тип поиска: {search_type}")
            return "Неверный тип поиска."

        # Получаем имя столбца для запроса
        db_column_name = COLUMN_TO_DB[search_type]
        
        # Формируем SQL запрос
        query_string = f"SELECT * FROM contacts WHERE {db_column_name} ILIKE %s"
        like_query = f"%{query.strip()}%"  # Убираем лишние пробелы

        logging.info(f"Исполняем запрос: {query_string} с параметрами: {like_query}")

        # Выполняем запрос
        cursor.execute(query_string, (like_query,))
        result = cursor.fetchall()

        # Логируем полученные данные из базы данных
        logging.info(f"Полученные данные из базы: {result}")

        # Логируем количество полученных результатов
        logging.info(f"Получены результаты: {len(result)}")

        if result:
            # Форматируем и логируем данные
            formatted_result = [format_contact_data(contact) for contact in result]
            logging.info(f"Форматированные данные: {formatted_result}")
            
            return formatted_result
        else:
            logging.info("Контакты не найдены.")
            return "Контакты не найдены."

    except Exception as e:
        logging.error(f"Ошибка при поиске данных: {e}")
        return f"Ошибка при поиске данных: {e}"

    finally:
        logging.info("Закрытие соединения с базой данных")
        cursor.close()
        conn.close()

# Функция для разбиения длинного текста на несколько частей
def split_message(message: str):
    # Разбиваем сообщение на части, если оно слишком длинное
    parts = []
    while len(message) > MAX_MESSAGE_LENGTH:
        parts.append(message[:MAX_MESSAGE_LENGTH])
        message = message[MAX_MESSAGE_LENGTH:]
    parts.append(message)
    return parts

# Функция для сериализации данных
def serialize_data(value):
    # Если это объект типа date, то форматируем его в строку
    if isinstance(value, date):
        return value.strftime('%d-%m-%Y')
    # Если это None, возвращаем пустую строку
    elif value is None:
        return ""
    # Для других типов данных возвращаем их строковое представление
    return str(value)

async def send_individual_results(update, result, selected_columns):
    if result:
        for row in result:
            contact_info = "Информация о контакте:\n"
            
            # Логируем содержимое строки для отладки
            logging.info(f"Контактные данные: {row}")

            for col in selected_columns:
                # Проверяем, что значение для данного столбца есть
                value = row.get(col, 'Не указано')
                contact_info += f"{col}: {value}\n"
            contact_info += "\n-----------------------\n"

            await update.message.reply_text(contact_info)
    else:
        await update.message.reply_text("Контакт не найден в базе данных.")

async def show_settings_menu(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'settings'

    # Кнопка "Изменить информацию поиска" и "Назад"
    keyboard = [
        [InlineKeyboardButton("Изменить информацию поиска", callback_data='edit_search_info')],
        [InlineKeyboardButton("Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        "Настройки:",
        reply_markup=reply_markup
    )

async def edit_search_info(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'edit_search_info'

    # Создаем кнопки для каждого столбца
    keyboard = []
    selected_columns = context.user_data.get('selected_columns', set())

    for col in COLUMNS:
        # Если столбец выбран, помечаем его
        label = f"✅ {col}" if col in selected_columns else col
        keyboard.append([InlineKeyboardButton(label, callback_data=f'toggle_column_{col}')])

    # Добавляем кнопку "Сохранить" и "Назад"
    keyboard.append([InlineKeyboardButton("Сохранить", callback_data='save_columns')])
    keyboard.append([InlineKeyboardButton("Назад", callback_data='back_to_settings')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        "Выберите, какую информацию показывать в результатах поиска:",
        reply_markup=reply_markup
    )    

async def start(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'start'

    # Создаем кнопки "Начать поиск" и "Настройки"
    keyboard = [
        [InlineKeyboardButton("Начать поиск", callback_data='start_search')],
        [InlineKeyboardButton("Настройки", callback_data='settings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.message:
        await update.message.reply_text(
            "Добро пожаловать! Пожалуйста, выберите действие:",
            reply_markup=reply_markup
        )
    elif update.callback_query:
        await update.callback_query.edit_message_text(
            "Добро пожаловать! Пожалуйста, выберите действие:",
            reply_markup=reply_markup
        )

async def show_search_menu(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'search_menu'

    # Получаем текущий выбранный тип поиска, если он есть
    selected_search_type = context.user_data.get('search_type')

    # Кнопки для выбора типа поиска с добавлением/удалением галочки
    keyboard = []
    for search_type in search_conditions.keys():
        label = search_type
        if selected_search_type == search_type:
            label = f"✅ {search_type}"  # Добавляем галочку
        keyboard.append([InlineKeyboardButton(label, callback_data=search_type)])

    # Добавляем кнопку "Назад"
    keyboard.append([InlineKeyboardButton("Назад", callback_data='back_to_start')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        "Выберите, по какому параметру выполнить поиск:",
        reply_markup=reply_markup
    )

async def button(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    # Обработка выбора кнопок
    if query.data == 'start_search':
        await show_search_menu(update, context)
    elif query.data == 'settings':
        await show_settings_menu(update, context)
    elif query.data == 'back_to_start':
        await start(update, context)
    elif query.data == 'edit_search_info':
        await edit_search_info(update, context)
    elif query.data.startswith('toggle_column_'):
        # Переключаем выбор столбца
        col_name = query.data.split('toggle_column_')[1]
        selected_columns = context.user_data.get('selected_columns', set())

        if col_name in selected_columns:
            selected_columns.remove(col_name)
        else:
            selected_columns.add(col_name)

        # Обновляем выбранные столбцы в контексте
        context.user_data['selected_columns'] = selected_columns
        await edit_search_info(update, context)  # Обновляем сообщение
    elif query.data == 'save_columns':
        await show_settings_menu(update, context)  # Возвращаемся в меню настроек
    elif query.data == 'back_to_settings':
        await show_settings_menu(update, context)
    
    # Добавляем логику для установки типа поиска
    elif query.data in search_conditions.keys():
        context.user_data['search_type'] = query.data  # Устанавливаем тип поиска

        # Переходим к выбору параметра для поиска, обновляя галочку на кнопке
        await show_search_menu(update, context)  # Обновляем меню с галочкой

# Функция для обработки текста (после выбора кнопки)
async def handle_text(update: Update, context: CallbackContext):
    search_type = context.user_data.get('search_type')
    if not search_type:
        await update.message.reply_text("Сначала выберите тип поиска с помощью кнопок.")
        return

    # Получаем текст запроса
    query = update.message.text
    selected_columns = context.user_data.get('selected_columns', COLUMNS)  # Все столбцы по умолчанию

    # Выполняем реальный поиск в базе данных
    result = search_contact_info(query, search_type)

    # Если были найдены результаты, отправляем их
    if isinstance(result, list):  # Проверяем, что результат — это список
        await send_individual_results(update, result, selected_columns)
    else:
        # Если возникла ошибка или не найдено, отправляем сообщение об ошибке
        await update.message.reply_text(result)

# Основная функция для запуска бота
def main():
    # Указываем токен вашего бота
    bot_token = '7965334033:AAG51HpKRBK8RmzYBCgqg-Mj7PMPijNL1WM'

    # Создаем приложение Telegram
    application = Application.builder().token(bot_token).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Запускаем бота
    application.run_polling()

if __name__ == '__main__':
    main()