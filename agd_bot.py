import logging
import psycopg2
import psycopg2.extras 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, filters
from datetime import date

# максимальная длина сообщения для Telegram (ограничение самого телеграма)
MAX_MESSAGE_LENGTH = 4096

# подключение к БД
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'agd_db',
    'user': 'postgres',
    'password': '1488'
}

# название кнопок для фильтра в настройках поиска
COLUMNS = [
    "ФИО", "Док ИС", "Направление", "Роль", "Уровень", "Специализация", "Gmail",
    "Контактная почта", "Git email", "GitHub ник", "Портфолио 1", "Портфолио 2", 
    "Телеграм", "Discord", "Страна", "Город", "День рождения", "CV", "Отклик", 
    "Метка", "Steam", "Телефон", "VK", "LinkedIn", "Комментарий", "Проекты"
]

# ключи кнопок фильтра для поиску по БД (колонки в БД называются иначе)
COLUMN_TO_DB = {
    "ФИО": "name",
    "Док ИС": "doc_is",
    "Направление": "direction",
    "Роль": "job_position_name",
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
    "Отклик": "referral_source",
    "Метка": "label",
    "Steam": "steam",
    "Телефон": "phone",
    "VK": "vk",
    "LinkedIn": "linkedin",
    "Комментарий": "comment",
    "Проекты": "projects"
}

# выбор SQL запроса к БД в меню для локального поиска (поиск по основной БД или по связанным)
search_conditions = {
    "ФИО": {"column": "name", "join": False},
    "Роль": {"column": "job_position_name", "join": True},
    "Отдел": {"column": "direction", "join": False},
    "Телеграм": {"column": "telegram", "join": False},
    "Проект": {"column": "project", "join": True} #true переводит нажатие кнопки в скрипт для поиска по привязанным таблицам через JOIN запрос
}

# обработка взаимодействия с кнопками
async def button(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    # Проверка текущего уровня меню и обработка кнопок
    if query.data == 'start_search':
        context.user_data['menu_level'] = 'search_menu'  # Обновляем уровень меню
        await show_search_menu(update, context)
    elif query.data == 'settings':
        context.user_data['menu_level'] = 'settings_menu'  # Обновляем уровень меню
        await show_settings_menu(update, context)
    elif query.data == 'back_to_start':
        context.user_data['menu_level'] = 'start'  # Возвращаемся в главное меню
        await start(update, context)
    elif query.data == 'show_project_selection_menu':
        context.user_data['menu_level'] = 'show_project_selection_menu'
        await show_project_selection_menu(update, context)
    elif query.data == 'edit_search_info':
        context.user_data['menu_level'] = 'edit_search_info'  # Уровень меню редактирования поиска
        await edit_search_info(update, context)
    elif query.data.startswith('toggle_column_'):
        col_name = query.data.split('toggle_column_')[1]
        selected_columns = context.user_data.get('selected_columns', set())

        # Добавление или удаление выбранного столбца
        if col_name in selected_columns:
            selected_columns.remove(col_name)
        else:
            selected_columns.add(col_name)
        context.user_data['selected_columns'] = selected_columns
        await edit_search_info(update, context)  # Обновляем сообщение
    elif query.data == 'select_all':
        context.user_data['selected_columns'] = set(COLUMNS)
        await edit_search_info(update, context)
    elif query.data == 'clear_all':
        context.user_data['selected_columns'] = set()
        await edit_search_info(update, context)
    elif query.data == 'save_columns':
        context.user_data['menu_level'] = 'settings_menu'  # Возвращаемся в меню настроек
        await show_settings_menu(update, context)
    elif query.data == 'back_to_settings':
        context.user_data['menu_level'] = 'settings_menu'  # Обновляем уровень для настроек
        await show_settings_menu(update, context)
    
    # Логика для выбора типа поиска
    elif query.data in search_conditions.keys():
        context.user_data['search_type'] = query.data  # Устанавливаем тип поиска
        await show_search_menu(update, context)  # Обновляем меню с галочкой

# команда старт
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

# меню настроек
async def show_settings_menu(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'settings'

    # Кнопка "Фильтр выдачи" и "Назад"
    keyboard = [
        [InlineKeyboardButton("Фильтр выдачи", callback_data='edit_search_info')],
        [InlineKeyboardButton("Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        "Настройки:",
        reply_markup=reply_markup
    )

# выбор выдачи информации и ее сохранение (например, выдавать только ФИО, тг, прокеты сотрудника)
async def edit_search_info(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'edit_search_info'

    # Получаем текущие выбранные колонки из контекста
    selected_columns = context.user_data.get('selected_columns', set())

    # Создаем клавиатуру
    keyboard = []
    row = []

    # Добавляем кнопки "Выбрать все" и "Сброс" в начало клавиатуры
    keyboard.append([
        InlineKeyboardButton("Выбрать все", callback_data='select_all'),
        InlineKeyboardButton("Сброс", callback_data='clear_all')
    ])

    # Создаем кнопки для каждого столбца
    for idx, col in enumerate(COLUMNS):
        # Отмечаем выбранные колонки галочкой
        label = f"✅ {col}" if col in selected_columns else col
        row.append(InlineKeyboardButton(label, callback_data=f'toggle_column_{col}'))

        # Добавляем кнопки по четыре в ряд
        if (idx + 1) % 4 == 0:
            keyboard.append(row)
            row = []

    # Добавляем оставшиеся кнопки, если их меньше четырех
    if row:
        keyboard.append(row)

    # Добавляем кнопку "Сохранить" и "Назад" на отдельные строки
    keyboard.append([InlineKeyboardButton("Сохранить", callback_data='save_columns')])
    keyboard.append([InlineKeyboardButton("Назад", callback_data='back_to_settings')])

    # Создаем разметку для клавиатуры
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Отправляем обновленное сообщение с клавиатурой
    await update.callback_query.edit_message_text(
        "Выберите, какую информацию показывать в результатах поиска и нажмите Сохранить:",
        reply_markup=reply_markup
    )

# выбор локального поиска, например поиск по ФИО, тг или проекту
async def show_search_menu(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'search_menu'

    # Получаем текущий выбранный тип поиска, если он есть
    selected_search_type = context.user_data.get('search_type')

    # Если тип поиска "Проект", то переходим к отображению кнопок с проектами

    # Кнопки для выбора типа поиска с добавлением/удалением галочки
    keyboard = []
    for search_type in search_conditions.keys():
        label = search_type
        if selected_search_type == search_type:
            label = f"✅ {search_type}"  # Добавляем галочку для выбранного типа поиска
        # Если ключ "Проект", проверяем, добавляем ли его с галочкой или без
        if search_type == "Проект":
            keyboard.append([InlineKeyboardButton(label, callback_data='show_project_selection_menu')])
        else:
            keyboard.append([InlineKeyboardButton(label, callback_data=search_type)])

    # Добавляем кнопку "Назад"
    keyboard.append([InlineKeyboardButton("Назад", callback_data='back_to_start')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    new_text = "Выберите, по какому параметру выполнить поиск:"

    # Получаем текущее сообщение и разметку клавиатуры
    current_message_text = update.callback_query.message.text
    current_markup_keyboard = update.callback_query.message.reply_markup.inline_keyboard if update.callback_query.message.reply_markup else []

    # Преобразуем текущую и новую клавиатуры в обычные списки для корректного сравнения
    new_keyboard_layout = [[button.text for button in row] for row in keyboard]
    current_keyboard_layout = [[button.text for button in row] for row in current_markup_keyboard]

    # Проверяем, отличается ли текст или разметка от текущих
    if current_message_text != new_text or current_keyboard_layout != new_keyboard_layout:
        await update.callback_query.edit_message_text(
            text=new_text,
            reply_markup=reply_markup
        )

# получаем название всех проектов
def get_projects_from_db():
    # Здесь замените на ваши параметры подключения к базе данных
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Получаем все проекты из таблицы projects
    cursor.execute("SELECT project_name FROM projects")
    projects = cursor.fetchall()

    cursor.close()
    connection.close()

    # Возвращаем список названий проектов
    return [project['project_name'] for project in projects]

# создаем меню с проектами для выбора
async def show_project_selection_menu(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'show_project_selection_menu'
    # Получаем список проектов из базы данных
    projects = get_projects_from_db()

    # Создаем клавиатуру с кнопками для каждого проекта
    keyboard = [
        [InlineKeyboardButton(project, callback_data=f"project_{project}")]
        for project in projects
    ]

    # Добавляем кнопку "Назад"
    keyboard.append([InlineKeyboardButton("Назад", callback_data='start_search')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    new_text = "Выберите проект для поиска:"

    # Проверка, отличается ли текст или разметка от текущих
    current_message = update.callback_query.message
    if current_message.text != new_text or current_message.reply_markup != reply_markup:
        await update.callback_query.edit_message_text(
            text=new_text,
            reply_markup=reply_markup
        )


# обработка ввода текста (после выбора типа локального поиска)
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

# поиск информации в БД по запросу
def search_contact_info(query: str, search_type: str):
    try:
        logging.info(f"Начинаем поиск по запросу: {query} для типа поиска: {search_type}")

        # Подключаемся к базе данных
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Проверка типа поиска и настройка join
        if search_type not in search_conditions:
            logging.error(f"Неверный тип поиска: {search_type}")
            return "Неверный тип поиска."

        search_info = search_conditions[search_type]

        if search_info["join"]:
            # Если 'join' == True, ищем через JOIN запрос
            if search_type == "Проект":
                query_string = """
                    SELECT c.contact_id, c.name, jp.job_position_name
                    FROM contacts c
                    LEFT JOIN contact_projects cp ON c.contact_id = cp.contact_id
                    LEFT JOIN projects p ON cp.project_id = p.project_id
                    LEFT JOIN contact_job_position cjp ON c.contact_id = cjp.contact_id
                    LEFT JOIN job_position jp ON cjp.job_position_name = jp.job_position_name
                    WHERE p.project_name ILIKE %s
                """
                like_query = f"%{query.strip()}%"

            elif search_type == "Роль":
                query_string = """
                    SELECT c.contact_id, c.name, jp.job_position_name
                    FROM contacts c
                    LEFT JOIN contact_job_position cjp ON c.contact_id = cjp.contact_id
                    LEFT JOIN job_position jp ON cjp.job_position_name = jp.job_position_name
                    WHERE jp.job_position_name ILIKE %s
                """
                like_query = f"%{query.strip()}%"

        else:
            # Если 'join' == False, обычный запрос по столбцу
            column_name = search_info["column"]
            query_string = f"SELECT contact_id FROM contacts WHERE {column_name} ILIKE %s"
            like_query = f"%{query.strip()}%"

        logging.info(f"Исполняем запрос: {query_string} с параметрами: {like_query}")

        # Выполняем запрос для поиска contact_id по столбцу
        cursor.execute(query_string, (like_query,))
        contact_ids = cursor.fetchall()

        logging.info(f"Полученные contact_id из базы: {contact_ids}")

        if contact_ids:
            formatted_result = []

            for contact_row in contact_ids:
                # Преобразуем contact_row в контакт
                contact_id = contact_row['contact_id']

                # Теперь получаем всю информацию о контакте по найденному contact_id
                cursor.execute("""
                    SELECT *
                    FROM contacts c
                    WHERE c.contact_id = %s
                """, (contact_id,))
                contact_data = cursor.fetchone()

                if contact_data:
                    # Преобразуем контакт в словарь
                    contact = dict(contact_data)

                    # Получаем проекты для контакта
                    cursor.execute("""
                        SELECT p.project_name
                        FROM projects p
                        JOIN contact_projects cp ON p.project_id = cp.project_id
                        WHERE cp.contact_id = %s
                    """, (contact_id,))
                    projects = cursor.fetchall()

                    # Получаем роли для контакта
                    logging.info(f"Выполняем запрос на роли для контакта {contact_id}")
                    cursor.execute("""
                        SELECT jp.job_position_name
                        FROM job_position jp
                        JOIN contact_job_position cjp ON jp.job_position_name = cjp.job_position_name
                        WHERE cjp.contact_id = %s
                    """, (contact_id,))
                    roles = cursor.fetchall()

                    # Логируем результат запроса на роль
                    logging.info(f"Роли для контакта {contact_id}: {roles}")

                    # Объединяем проекты в строку
                    project_names = [project['project_name'] for project in projects]
                    contact['projects'] = ', '.join(project_names) if project_names else 'Нет проектов'

                    # Объединяем роли в строку
                    role_names = [role['job_position_name'] for role in roles]
                    contact['job_position_name'] = ', '.join(role_names) if role_names else 'Нет ролей'

                    # Форматируем контакт с добавленными проектами и ролями
                    formatted_result.append(format_contact_data(contact))

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
    
# форматируем данные, заменяя None/Null... на "Не указано"
def format_contact_data(contact):
    formatted_contact = {
        'ФИО': contact.get('name', 'Не указано'),
        'Док ИС': contact.get('doc_is', 'Не указано'),
        'Направление': contact.get('direction', 'Не указано'),
        'Роль': contact.get('job_position_name', 'Не указано'),
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
        'Отклик': contact.get('referral_source', 'Не указано'),
        'Метка': contact.get('label', 'Не указано'),
        'Steam': contact.get('steam', 'Не указано'),
        'Телефон': contact.get('phone', 'Не указано'),
        'VK': contact.get('vk', 'Не указано'),
        'LinkedIn': contact.get('linkedin', 'Не указано'),
        'Комментарий': contact.get('comment', 'Не указано'),
        'Проекты': contact.get('projects', 'Не указано'),
    }
    return formatted_contact

# сериализация данных (передача данных для бота должна быть определенного формата)
def serialize_data(value):
    # Если это объект типа date, то форматируем его в строку
    if isinstance(value, date):
        return value.strftime('%d-%m-%Y')
    # Если это None, возвращаем пустую строку
    elif value is None:
        return ""
    # Для других типов данных возвращаем их строковое представление
    return str(value)

# выдача информации о сотрудниках с учетом выдачи выбранной информации в настройках
async def send_individual_results(update, result, selected_columns):
    if result:
        all_contact_info = ""  # Здесь будем собирать информацию о всех сотрудниках

        # Собираем информацию о каждом сотруднике
        for row in result:
            contact_info = ""

            # Логируем содержимое строки для отладки
            logging.info(f"Контактные данные: {row}")

            # Проходим по COLUMNS и выводим только те столбцы, которые были выбраны
            for col in COLUMNS:  # Используем COLUMNS для соблюдения порядка
                if col in selected_columns:
                    # Проверяем, что значение для данного столбца есть
                    value = row.get(col, 'Не указано')
                    contact_info += f"{col}: {value}\n"

            contact_info += "-----------------------\n"
            
            # Добавляем информацию о текущем сотруднике к общей строке
            all_contact_info += contact_info

        # Теперь делим весь собранный текст на части, учитывая информацию о сотрудниках
        message_parts = split_message(all_contact_info)

        # Отправляем каждую часть сообщения
        for part in message_parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text("Контакты не найдены в базе данных.")

# разделение выдаваемой информации на N сообщений в случае превышаения макс. кол-ва символов
# информация о сотруднике не разделяется на N сообщений, полная информация о сотруднике всегда будет в одном сообщении целиком
def split_message(message: str):
    # Разбиваем сообщение на части, если оно слишком длинное, но при этом не разделяем информацию о сотрудниках.
    parts = []
    while len(message) > MAX_MESSAGE_LENGTH:
        # Найдем ближайшую точку, чтобы не разделить информацию о сотруднике
        split_point = message.rfind("\n-----------------------\n", 0, MAX_MESSAGE_LENGTH)
        if split_point == -1:  # Если не нашли, просто разрезаем по длине
            split_point = MAX_MESSAGE_LENGTH
        parts.append(message[:split_point])
        message = message[split_point:]
    parts.append(message)
    return parts

# запуск бота
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
