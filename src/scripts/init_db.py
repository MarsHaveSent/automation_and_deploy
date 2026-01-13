"""
Скрипт для инициализации базы данных PostgreSQL.
Выполняет DDL скрипты из папки sql/
"""
import sys
from pathlib import Path
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Добавляем корневую папку в путь Python
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DB_CONFIG, check_required_env_vars

def create_database():
    """Создание базы данных если она не существует"""
    # Подключаемся к серверу PostgreSQL без указания базы данных
    conn_config = DB_CONFIG.copy()
    database_name = conn_config.pop('database')
    
    try:
        # Подключаемся к postgres БД для создания новой БД
        conn_config['database'] = 'postgres'
        conn = psycopg2.connect(**conn_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Проверяем существование базы данных
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
        exists = cursor.fetchone()
        
        if not exists:
            print(f"Создаем базу данных: {database_name}")
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"База данных {database_name} создана")
        else:
            print(f"База данных {database_name} уже существует")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при создании базы данных: {e}")
        sys.exit(1)

def execute_sql_file(filepath, connection):
    """Выполнение SQL файла"""
    with open(filepath, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    cursor = connection.cursor()
    try:
        cursor.execute(sql_content)
        connection.commit()
        print(f"Выполнен файл: {filepath.name}")
    except Exception as e:
        connection.rollback()
        print(f"Ошибка при выполнении {filepath.name}: {e}")
        raise
    finally:
        cursor.close()

def main():
    """Основная функция инициализации БД"""
    print("Инициализация базы данных PostgreSQL")
    
    try:
        # Проверяем переменные окружения
        check_required_env_vars()
        
        # Создаем базу данных если её нет
        create_database()
        
        # Подключаемся к созданной базе данных
        conn = psycopg2.connect(**DB_CONFIG)
        print("Подключение к базе данных установлено")
        
        # Выполняем SQL файлы по порядку
        sql_dir = Path(__file__).parent.parent / "sql"
        sql_files = [
            sql_dir / "create_tables.sql",
            # Здесь можно добавить другие SQL файлы
            # sql_dir / "insert_initial_data.sql",
        ]
        
        for sql_file in sql_files:
            if sql_file.exists():
                execute_sql_file(sql_file, conn)
            else:
                print(f"Файл не найден: {sql_file}")
        
        print("\nИнициализация базы данных завершена!")
        print("Созданы таблицы:")
        print("  - receipts (чеки)")
        print("  - processed_files (обработанные файлы)")
        
        # Закрываем соединение
        conn.close()
        
    except Exception as e:
        print(f"\nОшибка при инициализации БД: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()