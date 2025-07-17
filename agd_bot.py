import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, filters
import mysql.connector
import asyncio

logging.basicConfig(
    level=logging.INFO,  # Уровень логирования
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# максимальная длина сообщения для Telegram (ограничение самого телеграма)
MAX_MESSAGE_LENGTH = 4096

# подключение к БД
db_params = {
    'user': '',
    'password': '',
    'host': '',  # или другой хост, если удаленный
    'database': ''
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
    "Роль": {"column": "job_position_name", "join": True}, #true переводит нажатие кнопки в скрипт для поиска по привязанным таблицам через JOIN запрос
    "Направление": {"column": "direction", "join": False},
    "Телеграм": {"column": "telegram", "join": False},
    "Проект": {"column": "project", "join": True} #true переводит нажатие кнопки в скрипт для поиска по привязанным таблицам через JOIN запрос
}

async def set_commands(update, context):
    bot = context.bot

    # Устанавливаем команды, которые будут отображаться в меню
    await bot.set_my_commands([
        ("start", "Начать"),
        ("help", "Инструкция")
    ])
    await update.message.reply_text("Команды успешно зарегистрированы!")

# обработка взаимодействия с кнопками
async def button(update: Update, context: CallbackContext):
    query = update.callback_query
    logger.info("Received callback query with data: %s", query.data)

    await query.answer()

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
        logger.info("Action matched: %s, setting menu_level to %s", query.data, menu_level)

        if query.data == 'show_project_selection_menu':
            try:
                logger.info("Loading projects from database")
                projects = get_projects_from_db()
                context.user_data['projects'] = projects
                await func(update, context, projects)
                logger.info("Projects loaded successfully: %s", projects)
            except Exception as e:
                logger.error("Error loading projects: %s", e, exc_info=True)
                await query.answer(text="Ошибка при загрузке проектов.")
        else:
            await func(update, context)
            logger.info("Executed function: %s", func.__name__)

    elif query.data.startswith('toggle_column_'):
        col_name = query.data.split('toggle_column_')[1]
        selected_columns = context.user_data.setdefault('selected_columns', set())
        logger.info("Toggling column: %s", col_name)

        if col_name in selected_columns:
            selected_columns.discard(col_name)
            logger.info("Column %s removed from selected_columns", col_name)
        else:
            selected_columns.add(col_name)
            logger.info("Column %s added to selected_columns", col_name)

        await edit_search_info(update, context)

    elif query.data == 'select_all':
        logger.info("Selecting all columns")
        context.user_data['selected_columns'] = set(COLUMNS)
        await edit_search_info(update, context)

    elif query.data == 'clear_all':
        logger.info("Clearing all selected columns")
        context.user_data['selected_columns'] = set()
        await edit_search_info(update, context)

    elif query.data in search_conditions:
        logger.info("Setting search_type to: %s", query.data)
        context.user_data['search_type'] = query.data
        await show_search_menu(update, context)

    elif query.data in context.user_data.get('projects', []):
        search_type = 'Проект'
        logger.info("Selected project: %s, search_type set to %s", query.data, search_type)
        await handle_start(update, context, search_type)

# команды
async def start(update: Update, context: CallbackContext):
    logger.info("Function `start` called with user: %s", update.effective_user.username)

    # Устанавливаем начальные значения в user_data
    context.user_data['menu_level'] = 'start'
    context.user_data['start'] = True
    context.user_data['help'] = False
    logger.debug("User data initialized: %s", context.user_data)

    # Создаем кнопки "Начать поиск" и "Настройки"
    keyboard = [
        [InlineKeyboardButton("🔍 Начать поиск", callback_data='start_search')],
        [InlineKeyboardButton("⚙️ Настройки", callback_data='settings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    logger.debug("Reply markup created with buttons: %s", keyboard)

    try:
        if update.message:
            logger.info("Sending welcome message to user via `update.message`")
            await update.message.reply_text(
                "Добро пожаловать! Пожалуйста, выберите действие:",
                reply_markup=reply_markup
            )
        elif update.callback_query:
            logger.info("Editing message text via `update.callback_query`")
            await update.callback_query.edit_message_text(
                "Добро пожаловать! Пожалуйста, выберите действие:",
                reply_markup=reply_markup
            )
        else:
            logger.warning("Neither `update.message` nor `update.callback_query` present in the update")
    except Exception as e:
        logger.error("Error in `start` function: %s", e, exc_info=True)
async def help(update: Update, context: CallbackContext):
    logger.info("Function `help` called with user: %s", update.effective_user.username)

    # Устанавливаем начальные значения в user_data
    context.user_data['menu_level'] = 'help'
    context.user_data['help'] = True
    context.user_data['start'] = False
    logger.debug("User data updated: %s", context.user_data)

    try:
        if update.message:
            logger.info("Sending help message to user via `update.message`")
            await update.message.reply_text(
                "👋 <b>Привет</b>, я AGD - бот.\n\n"
                "Здесь ты сможешь найти информацию о сотрудниках 👨‍💻\n\n"
                "Для начала поиска выбери команду <b>/start</b>\n\n"
                "После этого нажми на ⚙️ <b>Настройки</b> -> <b>Фильтр выдачи</b>.\n"
                "Там будет возможность выбрать, какую информацию хочешь получить.\n\n"
                "Можно выбрать <b>все</b> столбцы или <b>несколько</b>.\n"
                "Обязательно нажми <b>Сохранить!</b> 💾\n\n"
                "Затем вернить и нажми 🔍 <b>Начать поиск</b>.\n\n"
                "Сдесь выбираешь, через что хочешь найти сотрудника.\n\n"
                "Например, если выбрать <b>ФИО</b> и ввести <b>Никита</b>, то выдаст <b>всех Никит</b>.\n\n"
                "Нажав на <b>Проект</b>, появится список проектов для поиска, что позволяет не прописывать название проекта вручную.",
                parse_mode='HTML'
            )
        else:
            logger.warning("No `update.message` found in the update")
    except Exception as e:
        logger.error("Error in `help` function: %s", e, exc_info=True)

# меню настроек
async def show_settings_menu(update: Update, context: CallbackContext):
    logger.info("Function `show_settings_menu` called with user: %s", update.effective_user.username)

    # Устанавливаем уровень меню в user_data
    context.user_data['menu_level'] = 'settings'
    logger.debug("User data updated: %s", context.user_data)

    # Создаем кнопки для меню настроек
    keyboard = [
        [InlineKeyboardButton("Фильтр выдачи", callback_data='edit_search_info')],
        [InlineKeyboardButton("⬅️ Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    logger.debug("Reply markup created with buttons: %s", keyboard)

    try:
        await update.callback_query.edit_message_text(
            "⚙️ Настройки:",
            reply_markup=reply_markup
        )
        logger.info("Settings menu displayed successfully")
    except Exception as e:
        logger.error("Error displaying settings menu: %s", e, exc_info=True)

# настраиваем фильтр выдачи
async def edit_search_info(update: Update, context: CallbackContext):
    context.user_data['menu_level'] = 'edit_search_info'
    logger.info(f"edit_search_info called for user {update.effective_user.username}")

    # Получаем текущие выбранные колонки из контекста
    selected_columns = context.user_data.get('selected_columns', set())
    logger.info(f"Selected columns: {selected_columns}")

    # Создаем клавиатуру
    keyboard = []
    row = []

    # Добавляем кнопки "Выбрать все" и "Сброс" в начало клавиатуры
    keyboard.append([
        InlineKeyboardButton("✔️ Выбрать все", callback_data='select_all'),
        InlineKeyboardButton("❌ Сброс", callback_data='clear_all')
    ])
    logger.info("Added 'Выбрать все' and 'Сброс' buttons to keyboard")

    # Создаем кнопки для каждого столбца
    for idx, col in enumerate(COLUMNS):
        # Отмечаем выбранные колонки галочкой
        label = f"✅ {col}" if col in selected_columns else col
        row.append(InlineKeyboardButton(label, callback_data=f'toggle_column_{col}'))

        # Добавляем кнопки по четыре в ряд
        if (idx + 1) % 4 == 0:
            keyboard.append(row)
            logger.info(f"Added row of buttons for columns {row}")
            row = []

    # Добавляем оставшиеся кнопки, если их меньше четырех
    if row:
        keyboard.append(row)
        logger.info(f"Added final row of buttons for columns {row}")

    # Добавляем кнопку "Сохранить" и "⬅️ Назад" на отдельные строки
    keyboard.append([InlineKeyboardButton("💾 Сохранить", callback_data='save_columns')])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data='back_to_settings')])

    logger.info("Added 'Сохранить' and '⬅️ Назад' buttons to keyboard")

    # Создаем разметку для клавиатуры
    reply_markup = InlineKeyboardMarkup(keyboard)
    logger.info("Created reply_markup for the keyboard")

    # Отправляем обновленное сообщение с клавиатурой
    try:
        await update.callback_query.edit_message_text(
            "Выберите, какую информацию показывать в результатах поиска и нажмите Сохранить:",
            reply_markup=reply_markup
        )
        logger.info("Successfully updated message with new keyboard")
    except Exception as e:
        logger.error(f"Error updating message: {e}")

# меню выбора локального поиска, например поиск по ФИО, тг или проекту
async def show_search_menu(update: Update, context: CallbackContext):
    logger.info("Function `show_search_menu` called with user: %s", update.effective_user.username)

    # Устанавливаем уровень меню в user_data
    context.user_data['menu_level'] = 'search_menu'
    logger.debug("User data updated: %s", context.user_data)

    # Получаем текущий выбранный тип поиска
    selected_search_type = context.user_data.get('search_type')
    logger.debug("Selected search type: %s", selected_search_type)

    # Создаем кнопки для выбора типа поиска
    keyboard = []
    for search_type in search_conditions.keys():
        label = search_type
        if selected_search_type == search_type:
            label = f"✅ {search_type}"  # Добавляем галочку для выбранного типа поиска

        if search_type == "Проект":
            keyboard.append([InlineKeyboardButton(label, callback_data='show_project_selection_menu')])
        else:
            keyboard.append([InlineKeyboardButton(label, callback_data=search_type)])

    # Добавляем кнопку "⬅️ Назад"
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data='back_to_start')])

    reply_markup = InlineKeyboardMarkup(keyboard)
    new_text = "Выберите, по какому параметру выполнить поиск:"
    logger.debug("New keyboard layout prepared: %s", keyboard)

    try:
        # Получаем текущее сообщение и разметку клавиатуры
        current_message_text = update.callback_query.message.text
        current_markup_keyboard = update.callback_query.message.reply_markup.inline_keyboard if update.callback_query.message.reply_markup else []

        # Преобразуем текущую и новую клавиатуры в списки для сравнения
        new_keyboard_layout = [[(button.text, button.callback_data) for button in row] for row in keyboard]
        current_keyboard_layout = [[(button.text, button.callback_data) for button in row] for row in current_markup_keyboard]

        # Проверяем, отличается ли текст или разметка от текущих
        if current_message_text != new_text or current_keyboard_layout != new_keyboard_layout:
            await update.callback_query.edit_message_text(
                text=new_text,
                reply_markup=reply_markup
            )
            logger.info("Search menu updated with new text and layout")
        else:
            logger.info("No changes detected in search menu; update skipped")
    except Exception as e:
        logger.error("Error updating search menu: %s", e, exc_info=True)

    # Обработка выбора типа поиска
    for search_type in search_conditions.keys():
        if selected_search_type == search_type:
            context.user_data['search_type'] = search_type
            logger.debug("Search type saved to user data: %s", search_type)

# получаем название всех проектов из БД
def get_projects_from_db():
    logger.info("Function `get_projects_from_db` called")

    try:
        # Подключение к базе данных
        with mysql.connector.connect(**db_params) as connection:
            with connection.cursor(dictionary=True) as cursor:
                # Получаем все проекты из таблицы projects
                cursor.execute("SELECT project_name FROM projects")
                projects = cursor.fetchall()
                logger.info("Projects successfully fetched from database")

                # Возвращаем список названий проектов
                project_list = [project['project_name'] for project in projects]
                logger.debug("Fetched projects: %s", project_list)
                return project_list

    except mysql.connector.Error as e:
        # Логирование ошибки
        logger.error("Database error in `get_projects_from_db`: %s", e, exc_info=True)
        return []

    except Exception as e:
        # Логирование любой другой ошибки
        logger.error("Unexpected error in `get_projects_from_db`: %s", e, exc_info=True)
        return []

# создаем выпадающее меню с проектами для выбора
async def show_project_selection_menu(update: Update, context: CallbackContext, projects: list):
    logger.info("Function `show_project_selection_menu` called with user: %s", update.effective_user.username)

    # Устанавливаем уровень меню в user_data
    context.user_data['menu_level'] = 'show_project_selection_menu'
    logger.debug("User data updated: %s", context.user_data)

    # Создаем клавиатуру с кнопками для каждого проекта
    keyboard = [
        [InlineKeyboardButton(project, callback_data=project)]  # Каждая кнопка с именем проекта
        for project in projects
    ]

    # Добавляем кнопку "⬅️ Назад"
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data='start_search')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    new_text = "Выберите проект для поиска:"
    logger.debug("Keyboard created with projects: %s", projects)

    try:
        # Получаем текущее сообщение и проверяем, нужно ли его обновить
        current_message = update.callback_query.message
        current_keyboard = current_message.reply_markup.inline_keyboard if current_message.reply_markup else []

        # Преобразуем текущую и новую клавиатуру в простые списки для сравнения
        new_keyboard_layout = [[button.text for button in row] for row in keyboard]
        current_keyboard_layout = [[button.text for button in row] for row in current_keyboard]

        # Проверяем, отличается ли текст или клавиатура
        if current_message.text != new_text or new_keyboard_layout != current_keyboard_layout:
            await update.callback_query.edit_message_text(
                text=new_text,
                reply_markup=reply_markup
            )
            logger.info("Project selection menu updated with new text and layout")
        else:
            logger.info("No changes detected in project selection menu; update skipped")
    except Exception as e:
        logger.error("Error updating project selection menu: %s", e, exc_info=True)

# обработка ввода текста и поиска по выпадающим спискам (после выбора типа локального поиска от команды /start)
async def handle_start(update: Update, context: CallbackContext, search_type: str = None):
    logger.info("Function `handle_start` called with user: %s", update.effective_user.username)

    try:
        # Проверяем, активирован ли стартовый режим
        if context.user_data.get('start', False):
            logger.debug("Start mode is active for user: %s", update.effective_user.username)

            # Если search_type не передан, пытаемся взять его из context.user_data
            search_type = search_type or context.user_data.get('search_type')
            logger.debug("Search type determined: %s", search_type)

            if not search_type:
                await update.message.reply_text("Сначала выберите тип поиска с помощью кнопок.")
                logger.warning("Search type is not selected for user: %s", update.effective_user.username)
                return

            # Определяем источник данных в зависимости от search_type
            if search_type == "Проект":
                query = update.callback_query.data  # query.data содержит название проекта
                logger.debug("Query determined from callback data: %s", query)
            else:
                query = update.message.text.lower()  # Текст из сообщения пользователя
                logger.debug("Query determined from user message: %s", query)

            # Получаем выбранные столбцы или используем все по умолчанию
            selected_columns = context.user_data.get('selected_columns', COLUMNS)
            logger.debug("Selected columns: %s", selected_columns)

            # Выполняем поиск в базе данных
            result = search_contact_info(query, search_type)
            logger.info("Search performed with query: %s, type: %s", query, search_type)

            # Отправляем результаты поиска
            if isinstance(result, list):
                await send_individual_results(update, result, selected_columns)
                logger.info("Results sent to user: %s", update.effective_user.username)
            else:
                await update.message.reply_text(result)
                logger.warning("No results found for query: %s, type: %s", query, search_type)
        else:
            logger.warning("Start mode is inactive for user: %s", update.effective_user.username)
    except Exception as e:
        logger.error("Error in `handle_start` for user: %s, error: %s", update.effective_user.username, e, exc_info=True)

# поиск информации в БД по запросу
def search_contact_info(query: str, search_type: str):
    logger.info(f"Начинаем поиск по запросу: {query} для типа поиска: {search_type}")
    
    try:
        # Подключаемся к базе данных с использованием контекстного менеджера
        with mysql.connector.connect(**db_params) as conn:
            with conn.cursor(dictionary=True) as cursor:
                
                search_info = search_conditions.get(search_type)
                if not search_info:
                    logger.error(f"Неизвестный тип поиска: {search_type}")
                    return f"Неизвестный тип поиска: {search_type}"

                # Строим запрос в зависимости от типа поиска
                if search_info['join']:
                    query_string, like_query = build_join_query(search_type, query)
                    logger.debug("Строим JOIN запрос: %s с параметрами: %s", query_string, like_query)
                else:
                    query_string, like_query = build_simple_query(search_info['column'], query)
                    logger.debug("Строим простой запрос: %s с параметрами: %s", query_string, like_query)

                # Исполняем запрос
                logger.info("Исполняем запрос: %s с параметрами: %s", query_string, like_query)
                cursor.execute(query_string, (like_query,))
                contact_ids = cursor.fetchall()

                # Логируем полученные данные из базы
                logger.info(f"Полученные contact_id из базы: {contact_ids}")

                if contact_ids:
                    # Собираем все contact_id для запроса
                    contact_ids_list = [contact['contact_id'] for contact in contact_ids]

                    # Получаем данные для всех контактов за один запрос
                    formatted_result = get_contacts_data(cursor, contact_ids_list)

                    if formatted_result:
                        logger.info(f"Форматированные данные: {formatted_result}")
                        return formatted_result
                    else:
                        logger.info("Контакты не найдены.")
                        return "Контакты не найдены."
                else:
                    logger.info("Контакты не найдены.")
                    return "Контакты не найдены."

    except mysql.connector.Error as db_error:
        logger.error(f"Ошибка при подключении или запросе в базу данных: {db_error}")
        return f"Ошибка при поиске данных: {db_error}"
    except Exception as e:
        logger.error(f"Общая ошибка при поиске данных: {e}", exc_info=True)
        return f"Ошибка при поиске данных: {e}"

# join запрос для связующих таблиц при поиске по связующей таблице (например, поиск по Проекту)
def build_join_query(search_type, query):
    logger.info(f"Строим JOIN запрос для типа поиска: {search_type} с параметром: {query}")
    
    if search_type == "Проект":
        query_string = """
            SELECT c.contact_id, c.name, jp.job_position_name
            FROM contacts c
            LEFT JOIN contact_projects cp ON c.contact_id = cp.contact_id
            LEFT JOIN projects p ON cp.project_id = p.project_id
            LEFT JOIN contact_job_position cjp ON c.contact_id = cjp.contact_id
            LEFT JOIN job_position jp ON cjp.job_position_name = jp.job_position_name
            WHERE LOWER(p.project_name) LIKE %s
        """
        logger.debug(f"Запрос для типа 'Проект': {query_string}")
        
    elif search_type == "Роль":
        query_string = """
            SELECT c.contact_id, c.name, jp.job_position_name
            FROM contacts c
            LEFT JOIN contact_job_position cjp ON c.contact_id = cjp.contact_id
            LEFT JOIN job_position jp ON cjp.job_position_name = jp.job_position_name
            WHERE LOWER(jp.job_position_name) LIKE %s
        """
        logger.debug(f"Запрос для типа 'Роль': {query_string}")
    
    else:
        logger.error(f"Неизвестный тип поиска: {search_type}")
        raise ValueError(f"Неизвестный тип поиска: {search_type}")

    # Формируем строку с параметром для LIKE-запроса
    like_query = f"%{query.strip().lower()}%"
    logger.debug(f"LIKE параметр для запроса: {like_query}")

    return query_string, like_query
# простой запрос при поиске (например, поиск по ФИО)
def build_simple_query(column_name, query):
    logger.info(f"Строим простой запрос для столбца: {column_name} с параметром: {query}")

    # Строим строку запроса для поиска по столбцу
    query_string = f"SELECT contact_id FROM contacts WHERE LOWER({column_name}) LIKE LOWER(%s)"
    
    # Формируем строку с параметром для LIKE-запроса
    like_query = f"%{query.strip().lower()}%"
    
    logger.debug(f"Запрос: {query_string}")
    logger.debug(f"LIKE параметр для запроса: {like_query}")

    return query_string, like_query

# получение информации о контакте
def get_contacts_data(cursor, contact_ids):
    logger.info(f"Получаем данные для контактов с ID: {contact_ids}")

    try:
        # Строим строку с условиями IN для запроса
        format_strings = ','.join(['%s'] * len(contact_ids))
        
        # Получаем полную информацию о контактах
        cursor.execute(f"SELECT * FROM contacts c WHERE c.contact_id IN ({format_strings})", tuple(contact_ids))
        contacts_data = cursor.fetchall()

        if not contacts_data:
            logger.warning("Контакты не найдены.")
            return []

        # Собираем все contact_ids для быстрого поиска в словаре
        contact_data_dict = {contact['contact_id']: contact for contact in contacts_data}
        
        # Получаем проекты для всех контактов за один запрос
        cursor.execute(f"""
            SELECT p.project_name, cp.contact_id
            FROM projects p
            JOIN contact_projects cp ON p.project_id = cp.project_id
            WHERE cp.contact_id IN ({format_strings})
        """, tuple(contact_ids))
        projects = cursor.fetchall()

        # Группируем проекты по контактам
        contact_projects = {}
        for project in projects:
            contact_projects.setdefault(project['contact_id'], []).append(project['project_name'])
        
        # Получаем роли для всех контактов за один запрос
        cursor.execute(f"""
            SELECT jp.job_position_name, cjp.contact_id
            FROM job_position jp
            JOIN contact_job_position cjp ON jp.job_position_name = cjp.job_position_name
            WHERE cjp.contact_id IN ({format_strings})
        """, tuple(contact_ids))
        roles = cursor.fetchall()

        # Группируем роли по контактам
        contact_roles = {}
        for role in roles:
            contact_roles.setdefault(role['contact_id'], []).append(role['job_position_name'])

        # Форматируем все контакты с проектами и ролями
        formatted_contacts = []
        for contact_id, contact in contact_data_dict.items():
            contact['projects'] = ', '.join(contact_projects.get(contact_id, [])) or 'Нет проектов'
            contact['job_position_name'] = ', '.join(contact_roles.get(contact_id, [])) or 'Нет ролей'
            formatted_contact = format_contact_data(contact)
            formatted_contacts.append(formatted_contact)

        logger.debug(f"Отформатированные данные для контактов: {formatted_contacts}")
        return formatted_contacts

    except Exception as e:
        logger.error(f"Ошибка при получении данных для контактов: {e}")
        return []

# форматируем данные, заменяя None/Null на "Не указано" для красоты
def format_contact_data(contact):
    formatted_contact = {}

    # Проверяем и логируем каждое отображение из COLUMN_TO_DB
    for display_name, db_column in COLUMN_TO_DB.items():
        # Получаем данные, используя как отображение, так и "чистое" имя столбца
        value = contact.get(db_column, 'Не указано')
        
        # Добавляем отформатированные данные в итоговый словарь
        formatted_contact[display_name] = value

        # Подробный лог для отладки
        logger.info(f"Отображаем '{display_name}' как '{db_column}': значение = '{value}'")

    logger.debug(f"Отформатированные данные контакта: {formatted_contact}")
    return formatted_contact

# выдача информации о сотрудниках с учетом выдачи выбранной информации в настройках
async def send_individual_results(update, result, selected_columns):
    if result:
        all_contact_info = ""  # Здесь будем собирать информацию о всех сотрудниках

        for row in result:
            contact_info = ""
            logging.info(f"Контактные данные (обработанные): {row}")

            for col in COLUMNS:
                # Проверяем, какие столбцы приходят, чтобы отладить корректность передачи данных
                logging.info(f"Текущий столбец: {col}")
                
                if col in selected_columns:
                    value = row.get(col, 'Не указано')
                    contact_info += f"{col}: {value}\n"
                    logging.info(f"Значение для столбца '{col}': {value}")

            contact_info += "-----------------------\n"
            all_contact_info += contact_info

        # Разбиваем весь собранный текст на части и выводим
        message_parts = split_message(all_contact_info)

        for part in message_parts:
            logging.info(f"Отправка сообщения: {part[:50]}...")  # Логируем первые 50 символов сообщения для отладки

            if update.callback_query:
                await update.callback_query.message.reply_text(part)
            else:
                await update.message.reply_text(part)
    else:
        no_contact_msg = "Контакты не найдены в базе данных."
        logging.warning(f"Контакты не найдены: {no_contact_msg}")
        
        await (update.callback_query.message.reply_text(no_contact_msg) if update.callback_query else update.message.reply_text(no_contact_msg))

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
            logging.warning(f"Разделение по длине, т.к. пунктирная отметка не найдена: длина сообщения = {len(message)}")

        # Добавляем первую часть сообщения
        parts.append(message[:split_point])
        message = message[split_point:]

        # Логирование промежуточных шагов для отслеживания разделения
        logging.info(f"Разделили сообщение на {len(parts)} частей. Оставшаяся длина: {len(message)}")

    # Добавляем последнюю часть
    parts.append(message)
    logging.info(f"Сообщение полностью разделено на {len(parts)} частей.")
    
    return parts

# запуск бота
def main():
    # Указываем токен вашего бота
    bot_token = ''

    # Создаем приложение Telegram
    application = Application.builder().token(bot_token).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start))

    application.add_handler(CommandHandler("setcommands", set_commands))

    # Запускаем бота
    application.run_polling()

if __name__ == '__main__':
    main()
