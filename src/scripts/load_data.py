"""
Скрипт для запуска загрузки данных в БД
"""
import sys
from pathlib import Path

# Добавляем корневую папку в путь Python
sys.path.insert(0, str(Path(__file__).parent.parent))

from loader import main

if __name__ == "__main__":
    main()