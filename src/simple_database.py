"""
упрощенный модуль для работы с БД PostgreSQL
"""
import psycopg2
import logging

from config import DB_CONFIG

logger = logging.getLogger(__name__)

class SimpleDB:
    def __init__(self):
        self.params = DB_CONFIG
    
    def connect(self):
        """Подключение к БД"""
        return psycopg2.connect(**self.params)
    
    def save_file_data(self, filename: str, data: list) -> bool:
        """
        Сохранение данных из CSV файла
        """
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # Удаляем старые данные этого файла (если нужно перезаписать)
            cursor.execute("DELETE FROM receipts WHERE file_name = %s", (filename,))
            
            # Вставляем данные
            for row in data:
                cursor.execute("""
                    INSERT INTO receipts 
                    (doc_id, store_id, cash_id, item, category, 
                     quantity, unit_price, discount_amount, 
                     receipt_date, file_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['doc_id'],
                    row['store_id'],
                    row['cash_id'],
                    row['item'],
                    row['category'],
                    row['amount'],
                    row['price'],
                    row['discount'],
                    row['receipt_date'],
                    filename
                ))
            
            # Отмечаем файл как обработанный
            cursor.execute("""
                INSERT INTO processed_files (file_name, records_count, status)
                VALUES (%s, %s, 'success')
                ON CONFLICT (file_name) DO UPDATE 
                SET processed_at = CURRENT_TIMESTAMP,
                    status = 'success',
                    records_count = EXCLUDED.records_count
            """, (filename, len(data)))
            
            conn.commit()
            logger.info(f"Файл {filename} загружен: {len(data)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки {filename}: {e}")
            if conn:
                conn.rollback()
            
            # Записываем ошибку
            try:
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO processed_files 
                        (file_name, status, error_message)
                        VALUES (%s, 'error', %s)
                        ON CONFLICT (file_name) DO UPDATE 
                        SET processed_at = CURRENT_TIMESTAMP,
                            status = 'error',
                            error_message = EXCLUDED.error_message
                    """, (filename, str(e)[:200]))  # обрезаем длинное сообщение
                    conn.commit()
            except Exception as inner_e:
                logger.error(f"Не удалось записать ошибку: {inner_e}")
            
            return False
        finally:
            if conn:
                conn.close()
    
    def is_file_processed(self, filename: str) -> bool:
        """Проверка, был ли файл уже обработан"""
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM processed_files WHERE file_name = %s AND status = 'success'",
                (filename,)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Ошибка проверки файла: {e}")
            return False
        finally:
            if conn:
                conn.close()