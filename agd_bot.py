import logging
import psycopg2
import psycopg2.extras 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, filters

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

# ключи кнопок фильтра для поиску по БД (колонки в БД называются иначе)
COLUMN_TO_DB = {
    "ФИО": "name",
    "Док_ИС": "doc_is",
    "Направление": "direction",
    "Роль": "job_position_name",
    "Уровень": "level_name",
    "Специализация": "specialization",
    "Gmail": "gmail",
    "Конт.почта": "contact_email",
    "Git_Email": "git_email",
    "GitHub_Ник": "nick_github",
    "Портфолио1": "portfolio_1",
    "Портфолио2": "portfolio_2",
    "ТГ": "telegram",
    "Discord": "discord",
    "Страна": "country",
    "Город": "city",
    "ДР": "birthday",
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

# название кнопок для фильтра в настройках поиска (берем назания на русском из COLUMN_TO_DB)
COLUMNS = [key.split()[0] for key in COLUMN_TO_DB.keys()]

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
    print(query.data)

    actions = {
        'start_search': (show_search_menu, 'search_menu'),
        'settings': (show_settings_menu, 'settings_menu'),
        'back_to_start': (start, 'start'),
        'show_project_selection_menu': (show_project_selection_menu, 'show_project_selection_menu'),
        'edit_search_info': (edit_search_info, 'edit_search_info'),
        'save_columns': (show_settings_menu, 'settings_menu'),
        'back_to_settings': (show_settings_menu, 'settings_menu')
    }

    if query.data in actions:
        func, menu_level = actions[query.data]
        context.user_data['menu_level'] = menu_level

        if query.data == 'show_project_selection_menu':
            try:
                projects = get_projects_from_db()
                context.user_data['projects'] = projects
                await func(update, context, projects)
            except Exception as e:
                await query.answer(text="Ошибка при загрузке проектов.")
                print(f"Ошибка: {e}")
        else:
            await func(update, context)

    elif query.data.startswith('toggle_column_'):
        col_name = query.data.split('toggle_column_')[1]
        selected_columns = context.user_data.setdefault('selected_columns', set())

        if col_name in selected_columns:
            selected_columns.discard(col_name)
        else:
            selected_columns.add(col_name)

        await edit_search_info(update, context)

    elif query.data == 'select_all':
        context.user_data['selected_columns'] = set(COLUMNS)
        await edit_search_info(update, context)

    elif query.data == 'clear_all':
        context.user_data['selected_columns'] = set()
        await edit_search_info(update, context)

    elif query.data in search_conditions:
        context.user_data['search_type'] = query.data
        await show_search_menu(update, context)

    elif query.data in context.user_data.get('projects', []):
        search_type = 'Проект'
        await handle_text(update, context, search_type)

# команда старт
async def start(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'start'
    projects = get_projects_from_db()
    context.user_data['projects'] = projects
    # Создаем кнопки "Начать поиск" и "Настройки"
    keyboard = [
        [InlineKeyboardButton("🔍 Начать поиск", callback_data='start_search')],
        [InlineKeyboardButton("⚙️ Настройки", callback_data='settings')]
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

    # Кнопка "Фильтр выдачи" и "⬅️ Назад"
    keyboard = [
        [InlineKeyboardButton("Фильтр выдачи", callback_data='edit_search_info')],
        [InlineKeyboardButton("⬅️ Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        "⚙️ Настройки:",
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
        InlineKeyboardButton("✔️ Выбрать все", callback_data='select_all'),
        InlineKeyboardButton("❌ Сброс", callback_data='clear_all')
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

    # Добавляем кнопку "Сохранить" и "⬅️ Назад" на отдельные строки
    keyboard.append([InlineKeyboardButton("💾 Сохранить", callback_data='save_columns')])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data='back_to_settings')])

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

    # Кнопки для выбора типа поиска с добавлением/удалением галочки
    keyboard = []
    for search_type in search_conditions.keys():
        label = search_type
        if selected_search_type == search_type:
            label = f"✅ {search_type}"  # Добавляем галочку для выбранного типа поиска
        # Если ключ "Проект", создаем кнопку для перехода в выпадющий список проектов
        if search_type == "Проект":
            keyboard.append([InlineKeyboardButton(label, callback_data='show_project_selection_menu')])
        else:
            keyboard.append([InlineKeyboardButton(label, callback_data=search_type)])

    # Добавляем кнопку "⬅️ Назад"
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data='back_to_start')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    new_text = "Выберите, по какому параметру выполнить поиск:"

    # Получаем текущее сообщение и разметку клавиатуры
    current_message_text = update.callback_query.message.text
    current_markup_keyboard = update.callback_query.message.reply_markup.inline_keyboard if update.callback_query.message.reply_markup else []

    # Преобразуем текущую и новую клавиатуры в списки для сравнения, учитывая и текст, и callback_data
    new_keyboard_layout = [[(button.text, button.callback_data) for button in row] for row in keyboard]
    current_keyboard_layout = [[(button.text, button.callback_data) for button in row] for row in current_markup_keyboard]

    # Проверяем, отличается ли текст или разметка от текущих
    if current_message_text != new_text or current_keyboard_layout != new_keyboard_layout:
        await update.callback_query.edit_message_text(
            text=new_text,
            reply_markup=reply_markup
        )

    # Обработка выбора типа поиска
    for search_type in search_conditions.keys():
        if selected_search_type == search_type:
            # Сохранение выбранного типа поиска в контекст
            context.user_data['search_type'] = search_type

# получаем название всех проектов из БД
def get_projects_from_db():
    try:
        # Подключение к базе данных
        with psycopg2.connect(**db_params) as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Получаем все проекты из таблицы projects
                cursor.execute("SELECT project_name FROM projects")
                projects = cursor.fetchall()

                # Возвращаем список названий проектов
                return [project['project_name'] for project in projects]

    except psycopg2.Error as e:
        # Логирование ошибки или вывод сообщения
        print(f"Ошибка при работе с базой данных: {e}")
        return []

    # Если есть необходимость, можно закрыть соединение вручную, но использование with гарантирует его закрытие.

# создаем меню с проектами для выбора
async def show_project_selection_menu(update: Update, context: CallbackContext, projects: list):
    context.user_data['menu_level'] = 'show_project_selection_menu'

    # Создаем клавиатуру с кнопками для каждого проекта
    keyboard = [
        [InlineKeyboardButton(project, callback_data=project)]  # Каждая кнопка с именем проекта
        for project in projects
    ]

    # Добавляем кнопку "⬅️ Назад"
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data='start_search')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    new_text = "Выберите проект для поиска:"

    # Получаем текущее сообщение и проверяем, нужно ли его обновить
    current_message = update.callback_query.message
    current_keyboard = current_message.reply_markup.inline_keyboard if current_message.reply_markup else []

    # Преобразуем текущую и новую клавиатуру в простые списки для сравнения (сравниваем только текст кнопок и callback_data)
    new_keyboard_layout = [[button.text for button in row] for row in keyboard]
    current_keyboard_layout = [[button.text for button in row] for row in current_keyboard]

    if current_message.text != new_text or new_keyboard_layout != current_keyboard_layout:
        # Обновляем сообщение с новыми данными
        await update.callback_query.edit_message_text(
            text=new_text,
            reply_markup=reply_markup
        )

# обработка ввода текста (после выбора типа локального поиска)
async def handle_text(update: Update, context: CallbackContext, search_type: str = None):
    # Если search_type не передан, пытаемся взять его из context.user_data
    search_type = search_type or context.user_data.get('search_type')

    if not search_type:
        await update.message.reply_text("Сначала выберите тип поиска с помощью кнопок.")
        return

    # Если search_type это "Проект", то мы берем query из данных кнопки (query.data)
    if search_type == "Проект":
        query = update.callback_query.data  # query.data содержит название проекта
    else:
        query = update.message.text  # Если это не "Проект", то текст все-таки из сообщения пользователя

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
        
        # Подключаемся к базе данных с использованием контекстного менеджера
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:

                search_info = search_conditions.get(search_type)
                if not search_info:
                    logging.error(f"Неизвестный тип поиска: {search_type}")
                    return f"Неизвестный тип поиска: {search_type}"

                if search_info['join']:
                    # Поиск через JOIN
                    query_string, like_query = build_join_query(search_type, query)
                else:
                    # Обычный запрос по столбцу
                    query_string, like_query = build_simple_query(search_info['column'], query)

                logging.info(f"Исполняем запрос: {query_string} с параметрами: {like_query}")
                
                # Выполняем запрос
                cursor.execute(query_string, (like_query,))
                contact_ids = cursor.fetchall()

                logging.info(f"Полученные contact_id из базы: {contact_ids}")
                
                if contact_ids:
                    formatted_result = []

                    for contact_row in contact_ids:
                        contact_id = contact_row['contact_id']
                        contact_data = get_contact_data(cursor, contact_id)

                        if contact_data:
                            # Форматируем контакт с добавленными проектами и ролями
                            formatted_result.append(contact_data)

                    logging.info(f"Форматированные данные: {formatted_result}") 
                    return formatted_result
                else:
                    logging.info("Контакты не найдены.")
                    return "Контакты не найдены."
    
    except Exception as e:
        logging.error(f"Ошибка при поиске данных: {e}")
        return f"Ошибка при поиске данных: {e}"
# вспомогательная для def search_contact_info
def build_join_query(search_type, query):
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
    elif search_type == "Роль":
        query_string = """
            SELECT c.contact_id, c.name, jp.job_position_name
            FROM contacts c
            LEFT JOIN contact_job_position cjp ON c.contact_id = cjp.contact_id
            LEFT JOIN job_position jp ON cjp.job_position_name = jp.job_position_name
            WHERE jp.job_position_name ILIKE %s
        """
    like_query = f"%{query.strip()}%"
    return query_string, like_query
# вспомогательная для def search_contact_info
def build_simple_query(column_name, query):
    query_string = f"SELECT contact_id FROM contacts WHERE {column_name} ILIKE %s"
    like_query = f"%{query.strip()}%"
    return query_string, like_query
# вспомогательная для def search_contact_info
def get_contact_data(cursor, contact_id):
    # Получаем полную информацию о контакте
    cursor.execute("SELECT * FROM contacts c WHERE c.contact_id = %s", (contact_id,))
    contact_data = cursor.fetchone()

    if contact_data:
        contact = dict(contact_data)

        # Получаем проекты
        cursor.execute("""
            SELECT p.project_name
            FROM projects p
            JOIN contact_projects cp ON p.project_id = cp.project_id
            WHERE cp.contact_id = %s
        """, (contact_id,))
        projects = cursor.fetchall()

        # Получаем роли
        cursor.execute("""
            SELECT jp.job_position_name
            FROM job_position jp
            JOIN contact_job_position cjp ON jp.job_position_name = cjp.job_position_name
            WHERE cjp.contact_id = %s
        """, (contact_id,))
        roles = cursor.fetchall()

        contact['projects'] = ', '.join([project['project_name'] for project in projects]) if projects else 'Нет проектов'
        contact['job_position_name'] = ', '.join([role['job_position_name'] for role in roles]) if roles else 'Нет ролей'

        # Возвращаем форматированные данные контакта
        return format_contact_data(contact)

    return None
    
# форматируем данные, заменяя None/Null... на "Не указано"
def format_contact_data(contact):
    formatted_contact = {}
    
    for display_name, db_column in COLUMN_TO_DB.items():
        # Получаем данные из contact по столбцу db_column и сохраняем их в formatted_contact
        formatted_contact[display_name] = contact.get(db_column, 'Не указано')
    return formatted_contact

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
            # В случае кнопки используем callback_query
            if update.callback_query:
                await update.callback_query.message.reply_text(part)
            else:
                await update.message.reply_text(part)
    else:
        # Если нет результатов
        if update.callback_query:
            await update.callback_query.message.reply_text("Контакты не найдены в базе данных.")
        else:
            await update.message.reply_text("Контакты не найдены в базе данных.")

# разделение выдаваемой информации на N сообщений в случае превышаения макс. кол-ва символов
# информация о сотруднике не разделяется на N сообщений, полная информация о сотруднике всегда будет в одном сообщении целиком
def split_message(message: str):
    # Разбиваем сообщение на части, если оно слишком длинное, но при этом не разделяем информацию о сотрудниках.
    parts = []
    while len(message) > MAX_MESSAGE_LENGTH:
        # Найдем ближайшую пунктирную отметку, чтобы не разделить информацию о сотруднике
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
