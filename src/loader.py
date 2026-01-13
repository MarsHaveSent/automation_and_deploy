"""
Модуль загрузки данных из CSV файлов в базу данных
"""
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import logging
from typing import List, Optional
import argparse
import sys

from config import DATA_DIR, MAX_RETRIES
from simple_database import SimpleDB

logger = logging.getLogger(__name__)

class DataLoader:
    """Класс для загрузки данных в БД"""
    
    def __init__(self):
        self.db = SimpleDB()
        self.processed_count = 0
        self.error_count = 0
        self.total_records = 0
    
    def extract_store_cash_from_filename(self, filename: str) -> Optional[tuple]:
        """Извлечение номера магазина и кассы из имени файла"""
        match = re.match(r'(\d+)_(\d+)\.csv$', filename.name)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None
    
    def validate_csv_file(self, filepath: Path) -> bool:
        """Валидация CSV файла"""
        try:
            # Проверяем расширение
            if filepath.suffix.lower() != '.csv':
                return False
            
            # Проверяем имя файла (формат store_cash.csv)
            if not self.extract_store_cash_from_filename(filepath):
                logger.warning(f"Неверный формат имени файла: {filepath.name}")
                return False
            
            # Проверяем что файл не пустой
            if filepath.stat().st_size == 0:
                logger.warning(f"Файл пустой: {filepath}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка валидации файла {filepath}: {e}")
            return False
    
    def read_and_prepare_data(self, filepath: Path) -> Optional[pd.DataFrame]:
        """Чтение и подготовка данных из CSV файла"""
        try:
            # Извлекаем информацию из имени файла
            store_id, cash_id = self.extract_store_cash_from_filename(filepath)
            
            # Извлекаем дату из пути
            date_str = filepath.parent.name
            try:
                receipt_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                logger.error(f"Неверный формат даты в пути: {date_str}")
                return None
            
            # Читаем CSV файл
            df = pd.read_csv(filepath, encoding='utf-8')
            
            # Проверяем обязательные колонки
            required_columns = ['doc_id', 'item', 'category', 'amount', 'price', 'discount']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Отсутствуют обязательные колонки в {filepath}: {missing_columns}")
                return None
            
            # Очистка данных
            df = df.dropna(subset=['doc_id', 'item'])  # Удаляем строки без doc_id или item
            df = df.fillna({'discount': 0})  # Заполняем пропуски в скидке
            
            # Преобразование типов
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(1).astype(int)
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
            df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0)
            
            # Удаляем некорректные строки
            df = df[(df['amount'] > 0) & (df['price'] >= 0) & (df['discount'] >= 0)]
            
            # Добавляем метаданные
            df['store_id'] = store_id
            df['cash_id'] = cash_id
            df['receipt_date'] = receipt_date
            
            # Округление денежных значений
            df['price'] = df['price'].round(2)
            df['discount'] = df['discount'].round(2)
            
            logger.debug(f"Прочитано {len(df)} записей из {filepath.name}")
            return df
            
        except Exception as e:
            logger.error(f"Ошибка чтения файла {filepath}: {e}")
            return None
    
    def process_file(self, filepath: Path) -> bool:
        """Обработка одного CSV файла"""
        filename = filepath.name
        
        # Пропускаем если файл уже обработан
        if self.db.is_file_processed(filename):
            logger.info(f"Файл уже обработан: {filename}")
            return True
        
        # Валидация файла
        if not self.validate_csv_file(filepath):
            return False
        
        # Чтение данных
        df = self.read_and_prepare_data(filepath)
        if df is None or df.empty:
            return False
        
        # Преобразуем DataFrame в список словарей
        data = df.to_dict('records')
        
        # Загружаем данные в БД
        success = self.db.save_file_data(filename, data)
        
        if success:
            self.processed_count += 1
            self.total_records += len(df)
            logger.info(f"Успешно обработан файл: {filename} ({len(df)} записей)")
        else:
            self.error_count += 1
            logger.error(f"Ошибка обработки файла: {filename}")
        
        return success
    
    def find_csv_files(self, base_dir: Path = None) -> List[Path]:
        """Поиск CSV файлов в папке и подпапках"""
        if base_dir is None:
            base_dir = DATA_DIR
        
        csv_files = []
        
        # Ищем во всех подпапках
        for csv_file in base_dir.rglob("*.csv"):
            # Проверяем что файл находится в папке с датой (формат YYYY-MM-DD)
            try:
                date_str = csv_file.parent.name
                datetime.strptime(date_str, "%Y-%m-%d")
                csv_files.append(csv_file)
            except ValueError:
                # Пропускаем файлы не в папках с датами
                logger.warning(f"Файл вне папки с датой: {csv_file}")
                continue
        
        # Сортируем по дате (из пути) и имени файла
        csv_files.sort(key=lambda x: (x.parent.name, x.name))
        
        return csv_files
    
    def process_all_files(self, specific_date: str = None) -> dict:
        """Обработка всех CSV файлов"""
        logger.info("Начало обработки файлов...")
        
        start_time = datetime.now()
        
        # Получаем список файлов
        csv_files = self.find_csv_files()
        
        if not csv_files:
            logger.warning("CSV файлы не найдены")
            return {
                'processed': 0,
                'errors': 0,
                'total_records': 0,
                'duration': 0
            }
        
        logger.info(f"Найдено {len(csv_files)} CSV файлов для обработки")
        
        # Фильтруем по дате если указано
        if specific_date:
            csv_files = [f for f in csv_files if f.parent.name == specific_date]
            logger.info(f"Фильтр по дате {specific_date}: {len(csv_files)} файлов")
        
        # Обрабатываем файлы
        for csv_file in csv_files:
            self.process_file(csv_file)
        
        # Статистика
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "="*50)
        logger.info("ОБРАБОТКА ЗАВЕРШЕНА")
        logger.info(f"Обработано файлов: {self.processed_count}")
        logger.info(f"Ошибок: {self.error_count}")
        logger.info(f"Всего записей: {self.total_records}")
        logger.info(f"Время выполнения: {duration:.2f} сек")
        logger.info(f"Скорость: {self.total_records/duration:.1f} записей/сек" if duration > 0 else "Скорость: N/A")
        logger.info("="*50)
        
        return {
            'processed': self.processed_count,
            'errors': self.error_count,
            'total_records': self.total_records,
            'duration': duration
        }

def main():
    parser = argparse.ArgumentParser(description='Загрузчик данных в БД PostgreSQL')
    parser.add_argument('--date', type=str, help='Дата для обработки (YYYY-MM-DD)')
    parser.add_argument('--file', type=str, help='Конкретный файл для обработки')
    parser.add_argument('--verbose', '-v', action='store_true', help='Подробный вывод')
    parser.add_argument('--force', action='store_true', help='Перезапись существующих данных')
    
    args = parser.parse_args()
    
    # Настройка логирования
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/loader.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Создаем и запускаем загрузчик
    loader = DataLoader()
    
    if args.file:
        # Обработка конкретного файла
        filepath = Path(args.file)
        if filepath.exists():
            loader.process_file(filepath)
        else:
            logger.error(f"Файл не найден: {args.file}")
    else:
        # Обработка всех файлов
        loader.process_all_files(args.date)

if __name__ == "__main__":
    main()