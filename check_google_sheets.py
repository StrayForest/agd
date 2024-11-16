import os
import mysql.connector
import logging
from google_auth_oauthlib.flow import InstalledAppFlow  # Для OAuth 2.0
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from time import sleep
from datetime import datetime

# Конфигурация MySQL
mysql_config = {
    'user': 'root',  # Укажите вашего пользователя
    'password': '1234',  # Укажите пароль от базы
    'host': 'localhost',  # Если база данных находится на другом сервере, укажите его IP
    'database': 'agd_db',  # Название вашей базы данных
    'charset': 'utf8mb4'  # Убедитесь, что используете корректную кодировку
}

# Конфигурация Google Sheets API
CLIENT_SECRET_FILE = 'client_secret_942918625771-ph4vdshpi35sk5p8odesjbmj5odkkdjc.apps.googleusercontent.com.json'
API_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = '1PWkcDqLb76QxhdNB-70CTHCODvR4U3kk1pLYsBuCeqA'
SHEET_NAME = 'БД'  # Укажите точное название листа, откуда хотите брать данные

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

# Логирование
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def convert_date_format(birthday):
    try:
        # Преобразуем строку с датой в объект datetime
        birthday_date = datetime.strptime(birthday, "%d.%m.%Y")  # Ожидаемый формат ввода: 'DD.MM.YYYY'
        print(f'Преобразованная дата: {birthday_date}')  # Выводим объект datetime для проверки
        # Преобразуем объект datetime в строку формата 'YYYY-MM-DD', только дата без времени
        formatted_date = birthday_date.strftime("%Y-%m-%d")
        print(f'Отформатированная дата: {formatted_date}')  # Выводим отформатированную дату для проверки
        return formatted_date
    except Exception as e:
        logging.error(f"Ошибка преобразования даты: {birthday} - {e}")
        return None

# Функция авторизации через OAuth 2.0
def authenticate_oauth():
    """
    Функция для авторизации через OAuth 2.0.
    """
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
    creds = flow.run_local_server(port=8080)  # Запуск локального сервера для авторизации
    return creds

# Функция подключения к Google Sheets
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

# Функция для обновления базы данных
def update_database(headers, data):
    """
    Синхронизация данных из Google Sheets с базой данных MySQL.
    """
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()

    try:
        # Создаем сопоставление заголовков Google Sheets и колонок БД
        header_to_db = {header: COLUMN_TO_DB.get(header) for header in headers if header in COLUMN_TO_DB}

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
                update_fields, values = build_update_fields(row_data, header_to_db, contact_id)
                
                if update_fields:
                    update_query = f"UPDATE contacts SET {', '.join(update_fields)} WHERE contact_id = %s"
                    cursor.execute(update_query, values)
                    logging.info(f"Обновлена запись для {name} ({telegram}).")

                # Если все поля пустые, можно удалить контакт
                if all(value is None or value == '' for value in row_data.values()):
                    cursor.execute("DELETE FROM contacts WHERE contact_id = %s", (contact_id,))
                    logging.info(f"Запись для {name} ({telegram}) удалена, так как все значения пустые.")

            else:
                contact_id = insert_contact(cursor, row_data, header_to_db)
                logging.info(f"Добавлена новая запись для {name} ({telegram}).")

            # Обработка проектов
            handle_projects(cursor, row_data.get("Проекты"), contact_id)
            # Обработка роли
            handle_role(cursor, row_data.get("Роль"), contact_id)

        connection.commit()

    except Exception as e:
        logging.error(f"Ошибка обновления базы данных: {e}")
    finally:
        cursor.close()
        connection.close()

def build_update_fields(row_data, header_to_db, contact_id):
    update_fields = []
    values = []

    # Проходим по всем полям и собираем их для обновления
    for header, db_column in header_to_db.items():
        value = row_data.get(header)

        # Если значение пустое, передаем NULL
        if value == '' or value is None:
            update_fields.append(f"{db_column} = %s")
            values.append(None)  # Это значение будет NULL в базе данных
        else:
            update_fields.append(f"{db_column} = %s")
            values.append(value)

    # Обязательно добавляем contact_id в параметры для WHERE-условия
    update_fields.append("contact_id = %s")
    values.append(contact_id)

    return update_fields, values


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
