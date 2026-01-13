import pandas as pd
from datetime import datetime, timedelta
import random
import string
from config import DATA_DIR, NUM_STORES, CATEGORIES, PRODUCTS_BY_CATEGORY
from config import DISCOUNT_PROBABILITY, DISCOUNT_RANGE, GENERATION_SETTINGS
import config as config
import logging
import sys
import argparse

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/generator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self, num_stores=NUM_STORES):
        self.num_stores = num_stores
        self.stores = {}
        self.initialize_stores()
    
    def initialize_stores(self):
        """Инициализация магазинов с случайным количеством касс"""
        for store_id in range(1, self.num_stores + 1):
            num_cash_registers = random.randint(
                config.MIN_CASH_REGISTERS_PER_STORE,
                config.MAX_CASH_REGISTERS_PER_STORE
            )
            self.stores[store_id] = {
                'num_cash_registers': num_cash_registers,
                'cash_registers': list(range(1, num_cash_registers + 1))
            }
        logger.info(f"Инициализировано {self.num_stores} магазинов")
    
    def generate_doc_id(self):
        """Генерация уникального ID чека"""
        timestamp = datetime.now().strftime('%y%m%d%H%M%S')
        random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        return f"{timestamp}_{random_part}"
    
    def select_item_from_category(self, category):
        """Выбор случайного товара из категории"""
        items = PRODUCTS_BY_CATEGORY[category]
        item_data = random.choice(items)
        return item_data
    
    def calculate_discount(self, price):
        """Расчет скидки для товара"""
        if random.random() < DISCOUNT_PROBABILITY:
            discount_percent = random.uniform(DISCOUNT_RANGE[0], DISCOUNT_RANGE[1])
            discount_amount = round(price * discount_percent, 2)
            return discount_amount
        return 0.0
    
    def generate_receipt(self, store_id, cash_id, receipt_date):
        """Генерация одного чека"""
        doc_id = self.generate_doc_id()
        num_items = random.randint(
            config.ITEMS_PER_RECEIPT_MIN,
            config.ITEMS_PER_RECEIPT_MAX
        )
        
        receipt_items = []
        for _ in range(num_items):
            # Выбираем случайную категорию
            category = random.choice(CATEGORIES)
            
            # Выбираем случайный товар из категории
            item_data = self.select_item_from_category(category)
            item_name = item_data["item"]
            min_price, max_price = item_data["price_range"]
            price = random.uniform(min_price, max_price)
            
            amount = random.randint(1, 5)
            discount = self.calculate_discount(price)
            
            receipt_items.append({
                'doc_id': doc_id,
                'item': item_name,
                'category': category,
                'amount': amount,
                'price': round(price, 2),
                'discount': discount,
                'store_id': store_id,
                'cash_id': cash_id,
                'receipt_date': receipt_date
            })
        
        return receipt_items
    
    def generate_cash_data(self, store_id, cash_id, date, num_receipts=None):
        """Генерация данных для одной кассы за день"""
        if num_receipts is None:
            num_receipts = random.randint(30, config.RECEIPTS_PER_CASH_PER_DAY)
        
        all_items = []
        for _ in range(num_receipts):
            receipt_items = self.generate_receipt(store_id, cash_id, date)
            all_items.extend(receipt_items)
        
        return pd.DataFrame(all_items)
    
    def generate_daily_files(self, target_date=None, force=False):
        """
        Генерация файлов за определенный день
        
        Args:
            target_date: Дата для генерации (по умолчанию - сегодня)
            force: Принудительная генерация даже в воскресенье
        """
        if target_date is None:
            target_date = datetime.now()
        elif isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d")
        
        # Проверка на воскресенье
        if target_date.weekday() == 6 and not force:
            logger.info(f"Воскресенье {target_date.date()} - генерация отключена")
            return 0
        
        # Проверка на праздники
        if target_date.strftime("%Y-%m-%d") in GENERATION_SETTINGS['holidays'] and not force:
            logger.info(f"Праздничный день {target_date.date()} - генерация отключена")
            return 0
        
        date_str = target_date.strftime("%Y-%m-%d")
        day_dir = DATA_DIR / date_str
        day_dir.mkdir(exist_ok=True, parents=True)
        
        generated_files = 0
        total_items = 0
        
        # Генерация для каждого магазина
        for store_id, store_info in self.stores.items():
            for cash_id in store_info['cash_registers']:
                # Случайное количество чеков для кассы
                num_receipts = random.randint(20, config.RECEIPTS_PER_CASH_PER_DAY)
                
                # Генерация данных
                df = self.generate_cash_data(store_id, cash_id, target_date.date(), num_receipts)
                
                # Сохранение в CSV
                filename = day_dir / f"{store_id}_{cash_id}.csv"
                df.to_csv(filename, index=False, encoding='utf-8')
                
                generated_files += 1
                total_items += len(df)
                
                logger.info(f"Сгенерирован файл: {filename} "
                          f"(чеков: {num_receipts}, товаров: {len(df)})")
        
        logger.info(f"Всего сгенерировано: {generated_files} файлов, {total_items} записей")
        return generated_files
    
    def generate_date_range(self, start_date, end_date):
        """Генерация данных за диапазон дат"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        total_files = 0
        current_date = start
        
        while current_date <= end:
            logger.info(f"Генерация данных за {current_date.date()}")
            files_generated = self.generate_daily_files(current_date)
            total_files += files_generated
            
            current_date += timedelta(days=1)
        
        logger.info(f"Генерация завершена. Всего файлов: {total_files}")
        return total_files

def main():
    parser = argparse.ArgumentParser(description='Генератор данных о продажах')
    parser.add_argument('--date', type=str, help='Дата для генерации (YYYY-MM-DD)')
    parser.add_argument('--stores', type=int, default=NUM_STORES, 
                       help=f'Количество магазинов (по умолчанию: {NUM_STORES})')
    parser.add_argument('--start-date', type=str, help='Начальная дата для генерации диапазона')
    parser.add_argument('--end-date', type=str, help='Конечная дата для генерации диапазона')
    parser.add_argument('--force', action='store_true', help='Генерировать даже в выходные')
    parser.add_argument('--verbose', '-v', action='store_true', help='Подробный вывод')
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    generator = DataGenerator(num_stores=args.stores)
    
    if args.start_date and args.end_date:
        # Генерация диапазона дат
        generator.generate_date_range(args.start_date, args.end_date)
    elif args.date:
        # Генерация за конкретную дату
        generator.generate_daily_files(args.date, args.force)
    else:
        # Генерация за сегодня
        generator.generate_daily_files(force=args.force)

if __name__ == "__main__":
    main()