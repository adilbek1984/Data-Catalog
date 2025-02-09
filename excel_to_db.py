import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Float, BigInteger, DateTime, Date, Time, text
from sqlalchemy.exc import ProgrammingError

# Загружаем Excel файл
file_path = "C:/Users/adilb/OneDrive/Документы/Работа/Дата каталог/OpenMetadata/source/buro_propuskov.xlsx"
df = pd.read_excel(file_path, header=1)

# Убираем лишние пробелы в названиях столбцов и переименовываем их
df.columns = df.columns.str.strip()
df = df.rename(columns={
    "bd_name": "database_name",
    "schema_name": "schema_name",
    "table_name": "table_name",
    "column_name": "column_name",
    "column_type": "data_type",
    "column_description": "description"
})

# Убираем пустые строки и заполняем пропуски
df = df.dropna(how='all')
df[['database_name', 'schema_name', 'table_name']] = df[['database_name', 'schema_name', 'table_name']].ffill()

# Функция для сопоставления типов данных
def map_data_type(data_type):
    type_mapping = {
        "varchar": String,
        "integer": Integer,
        "text": String,
        "boolean": Boolean,
        "float": Float,
        "bigint": BigInteger,
        "datetime": DateTime,
        "date": Date,
        "time": Time
    }
    return type_mapping.get(data_type.lower(), String)

# Подключение к PostgreSQLr
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')
connection = engine.connect()

# Функция для создания базы данных, если она не существует
def create_database(database_name):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres', isolation_level='AUTOCOMMIT')
    with engine.connect() as connection:
        db_exists = connection.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname='{database_name}'")
        ).fetchone()
        if not db_exists:
            connection.execute(text(f'CREATE DATABASE "{database_name}"'))
            print(f"База данных '{database_name}' успешно создана.")
        else:
            print(f"База данных '{database_name}' уже существует")

# Функция для предоставления привилегий пользователю
def grant_privileges(database_name, schema_name, user_name):
    db_engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/{database_name}')
    db_connection = db_engine.connect()
    try:
        # Предоставляем привилегии на создание таблиц и изменения таблиц в конкретной схеме
        db_connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema_name}" TO {user_name}'))
        db_connection.execute(text(f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema_name}" TO {user_name}'))
        print(f"Привилегии успешно предоставлены пользователю '{user_name}' в базе данных '{database_name}', схеме '{schema_name}'.")
    except Exception as e:
        print(f"Ошибка при предоставлении привилегий: {e}")

# Создание баз данных, схем и таблиц
for database in df['database_name'].unique():
    create_database(database)

    db_engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/{database}')
    metadata = MetaData()

    with db_engine.begin() as db_connection:
        # После создания базы данных предоставляем привилегии
        for schema in df[df['database_name'] == database]['schema_name'].unique():
            # Создание схемы перед предоставлением прав
            db_connection = db_engine.connect()
            db_connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            grant_privileges(database, schema, 'postgres')

            # Создание таблиц
            for table in df[(df['database_name'] == database) & (df['schema_name'] == schema)]['table_name'].unique():
                columns_data = df[(df['database_name'] == database) & (df['schema_name'] == schema) & (df['table_name'] == table)]
                columns = [
                    Column(row['column_name'], map_data_type(row['data_type']))
                    for _, row in columns_data.iterrows()
                ]

                table_obj = Table(table, metadata, *columns, schema=schema)

                try:
                    # Создание таблиц
                    metadata.create_all(db_engine)
                    print(f"Таблица '{schema}.{table}' успешно создана в базе '{database}'.")

                    # Добавление комментариев отдельно
                    with db_engine.connect() as conn:
                        for _, row in columns_data.iterrows():
                            if pd.notna(row['description']):
                                try:
                                    description = row['description'].replace("'", "''")
                                    comment_query = f'COMMENT ON COLUMN "{schema}"."{table}"."{row["column_name"]}" IS \'{description}\''
                                    conn.execute(text(comment_query))
                                    print(
                                        f"Комментарий '{row['description']}' успешно добавлен для колонки '{row['column_name']}' в таблице '{schema}.{table}'.")
                                except Exception as e:
                                    print(f"Ошибка при добавлении комментария к колонке '{row['column_name']}' в таблице '{schema}.{table}': {e}")
                except Exception as e:
                    print(f"Ошибка при создании таблицы '{schema}.{table}': {e}")

