import mysql.connector
import logging
from google_auth_oauthlib.flow import InstalledAppFlow  # Для OAuth 2.0
from googleapiclient.discovery import build
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Конфигурация Google Sheets API
CLIENT_SECRET_FILE = 'client_secret_942918625771-ph4vdshpi35sk5p8odesjbmj5odkkdjc.apps.googleusercontent.com.json'
API_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '1PWkcDqLb76QxhdNB-70CTHCODvR4U3kk1pLYsBuCeqA'
SHEET_NAME = 'БД'  # Укажите точное название листа, откуда хотите брать данные

# Конфигурация MySQL
mysql_config = {
    'user': 'root',  # Укажите вашего пользователя
    'password': '1234',  # Укажите пароль от базы
    'host': 'localhost',  # Если база данных находится на другом сервере, укажите его IP
    'database': 'agd_db',  # Название вашей базы данных
    'charset': 'utf8mb4'  # Убедитесь, что используете корректную кодировку
}

# Соответствие между столбцами Google Sheets и базой данных
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

# Функция авторизации через OAuth 2.0
def authenticate_oauth():
    """
    Функция для авторизации через OAuth 2.0.
    """
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=8080)  # Запуск локального сервера для авторизации
    return creds

# подгоняем дату под нужный формат
def convert_date_format(birthday):
    try:
        # Преобразуем строку с датой в объект datetime
        birthday_date = datetime.strptime(birthday, "%d.%m.%Y")  # Ожидаемый формат ввода: 'DD.MM.YYYY'
        # Преобразуем объект datetime в строку формата 'YYYY-MM-DD', только дата без времени
        formatted_date = birthday_date.strftime("%Y-%m-%d")
        return formatted_date
    except Exception as e:
        logging.error(f"Ошибка преобразования даты: {birthday} - {e}")
        return None

# Подключение к Google Sheets
def get_google_sheet_data():
    """
    Авторизация через сервисный аккаунт или OAuth 2.0 и извлечение данных из Google Sheets.
    """
    creds = authenticate_oauth()  # Можно заменить на `Credentials.from_service_account_file` для сервисного аккаунта
    service = build(API_NAME, API_VERSION, credentials=creds)

    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=SHEET_NAME).execute()
    values = result.get('values', [])
    
    if not values:
        logging.error("Данные из Google Sheets не получены.")
        return []
    
    headers = values[0]  # Первая строка — это заголовки
    data = values[1:]  # Остальные строки — это данные
    logging.info(f"Получены данные из Google Sheets: {len(data)} строк.")
    return headers, data

# Сихронизации google sheets и БД
def update_database(headers, data):
    """
    Синхронизация данных из Google Sheets с базой данных MySQL.
    """
    # Устанавливаем соединение с базой данных
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()

    try:
        # Начинаем транзакцию
        connection.start_transaction()

        # Создаем сопоставление заголовков Google Sheets и колонок БД
        header_to_db = {header: COLUMN_TO_DB.get(header) for header in headers if header in COLUMN_TO_DB}

        # Список обработанных contact_id
        processed_contact_ids = set()

        for row in data:
            row_data = dict(zip(headers, row))

            # Получаем значения ФИО и Телеграм для идентификации записи
            name = row_data.get("ФИО")  
            telegram = row_data.get("ТГ")
            
            if not name or not telegram:
                logging.warning(f"Пропущена строка из-за отсутствия ФИО или Телеграм: {row_data}")
                continue

            # Проверяем, существует ли уже запись в базе данных
            cursor.execute("""
                SELECT contact_id FROM contacts WHERE name = %s AND telegram = %s
            """, (name, telegram))
            contact = cursor.fetchone()

            if contact:
                contact_id = contact[0]
                processed_contact_ids.add(contact_id)  # Добавляем контакт в обработанные
                update_fields, values = build_update_fields(row_data, header_to_db, contact_id)
                # Обработка пустых значений: если значение пустое, меняем на NULL
                for i in range(len(update_fields)):
                    if values[i] is None or values[i] == '':
                        update_fields[i] = f"{update_fields[i]} = NULL"
                        values[i] = None  # Устанавливаем значение в None, чтобы обновить на NULL

                if update_fields:
                    update_query = f"UPDATE contacts SET {', '.join(update_fields)} WHERE contact_id = %s"
                    cursor.execute(update_query, values)
                    logging.info(f"Обновлена запись для {name} ({telegram}).")
            else:
                contact_id = insert_contact(cursor, row_data, header_to_db)
                processed_contact_ids.add(contact_id)  # Добавляем контакт в обработанные
                logging.info(f"Добавлена новая запись для {name} ({telegram}).")

            # Обработка проектов и роли
            handle_projects(cursor, row_data.get("Проекты"), contact_id)
            handle_role(cursor, row_data.get("Роль"), contact_id)

        # Теперь находим и удаляем все контакты, которых нет в обработанных
        cursor.execute("SELECT contact_id FROM contacts")
        all_contacts = cursor.fetchall()

        for contact in all_contacts:
            contact_id = contact[0]
            if contact_id not in processed_contact_ids:
                # Удаляем записи из связанных таблиц
                delete_related_data(cursor, contact_id)
                # Удаляем контакт
                cursor.execute("DELETE FROM contacts WHERE contact_id = %s", (contact_id,))
                logging.info(f"Удален контакт с id {contact_id} и его связанные данные.")

        # Подтверждаем все изменения в базе данных
        connection.commit()

    except Exception as e:
        logging.error(f"Ошибка обновления базы данных: {e}")
        # В случае ошибки откатываем транзакцию
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

# Удаление контакта, если он удалет из гугл таблиц
def delete_related_data(cursor, contact_id):
    """
    Удаляем все данные, связанные с данным контактным ID из других таблиц.
    """
    try:
        # Удаляем связь с проектами
        cursor.execute("DELETE FROM contact_projects WHERE contact_id = %s", (contact_id,))
        # Удаляем связь с ролями
        cursor.execute("DELETE FROM contact_job_position WHERE contact_id = %s", (contact_id,))
        logging.info(f"Удалены все связанные данные для contact_id {contact_id}.")
    except Exception as e:
        logging.error(f"Ошибка при удалении связанных данных для contact_id {contact_id}: {e}")

# Строит список полей для обновления в SQL-запросе
def build_update_fields(row_data, header_to_db, contact_id):
    update_fields = []
    values = []

    for key, db_column in header_to_db.items():
        if key not in ['Проекты', 'Роль']:  # Исключаем обработку проектов и роли
            value = row_data.get(key)
            value = process_value(value, key)

            # Если значение None или пустая строка, заменяем на None
            if value == '' or value is None:
                value = None  # Преобразуем в None, чтобы в SQL было NULL

            # Добавляем значение в values, даже если это None (будет обработано как NULL)
            update_fields.append(f"{db_column} = %s")
            values.append(value)

    values.append(contact_id)  # Добавляем ID контакта в конец
    return update_fields, values

# Вставляет новый контакт в базу данных
def insert_contact(cursor, row_data, header_to_db):
    columns = []
    placeholders = []
    values = []

    for key, db_column in header_to_db.items():
        if key not in ['Проекты', 'Роль']:  # Исключаем обработку проектов и роли
            value = row_data.get(key)
            value = process_value(value, key)
            if value is not None:
                columns.append(db_column)
                placeholders.append("%s")
                values.append(value)

    # Не включаем contact_id, если оно автоинкрементное
    insert_query = f"""
        INSERT INTO contacts ({', '.join(columns)}) VALUES ({', '.join(placeholders)})
    """
    cursor.execute(insert_query, values)
    return cursor.lastrowid  # Возвращаем id вставленной записи

def process_value(value, key):
    if value == '' or value is None:
        return None
    if key == "ДР":
        return convert_date_format(value) if value else None
    return value

# отдельно собираем и добавляем проекты в связующую таблицу
def handle_projects(cursor, projects, contact_id):
    if projects:
        project_list = projects.splitlines()
        for project_name in project_list:
            project_name = project_name.strip()
            if project_name:
                try:
                    cursor.execute("SELECT project_id FROM projects WHERE project_name = %s", (project_name,))
                    project = cursor.fetchone()  # Читаем результат

                    if project:  # Если проект найден
                        project_id = project[0]
                        # Проверяем, не существует ли уже связи между контактом и проектом
                        cursor.execute("""
                            SELECT 1 FROM contact_projects WHERE contact_id = %s AND project_id = %s
                        """, (contact_id, project_id))
                        existing_relation = cursor.fetchone()  # Читаем результат
                        if not existing_relation:  # Если связи нет, то вставляем
                            cursor.execute("""
                                INSERT INTO contact_projects (contact_id, project_id) 
                                VALUES (%s, %s)
                            """, (contact_id, project_id))
                            logging.info(f"Добавлена связь с проектом '{project_name}' для contact_id {contact_id}.")
                        else:
                            logging.info(f"Связь с проектом '{project_name}' уже существует для contact_id {contact_id}.")
                    else:
                        logging.warning(f"Проект '{project_name}' не найден в базе данных.")
                except Exception as e:
                    logging.error(f"Ошибка при обработке проекта '{project_name}' для contact_id {contact_id}: {e}")
                finally:
                    cursor.nextset()  # Завершаем обработку набора результатов
# отдельно собираем и добавляем роли в связующую таблицу
def handle_role(cursor, role, contact_id):
    if role:
        try:
            cursor.execute("SELECT job_position_name FROM job_position WHERE job_position_name = %s", (role,))
            job_position = cursor.fetchone()  # Читаем результат

            if job_position:  # Если роль найдена
                # Проверяем, не существует ли уже связи между контактом и ролью
                cursor.execute("""
                    SELECT 1 FROM contact_job_position WHERE contact_id = %s AND job_position_name = %s
                """, (contact_id, role))
                existing_relation = cursor.fetchone()  # Читаем результат
                if not existing_relation:  # Если связи нет, то вставляем
                    cursor.execute("""
                        INSERT INTO contact_job_position (contact_id, job_position_name) 
                        VALUES (%s, %s)
                    """, (contact_id, role))
                    logging.info(f"Добавлена связь с ролью '{role}' для contact_id {contact_id}.")
                else:
                    logging.info(f"Связь с ролью '{role}' уже существует для contact_id {contact_id}.")
            else:
                logging.warning(f"Роль '{role}' не найдена в базе данных.")
        except Exception as e:
            logging.error(f"Ошибка при обработке роли '{role}' для contact_id {contact_id}: {e}")
        finally:
            cursor.nextset()  # Завершаем обработку набора результатов

if __name__ == "__main__":
    try:
        headers, data = get_google_sheet_data()  # Получаем данные из Google Sheets
        if headers and data:
            update_database(headers, data)  # Обновляем базу данных
        else:
            print("Нет данных для обработки.")
    except Exception as e:
        logging.error(f"Ошибка при запуске программы: {e}")
