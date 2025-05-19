import os
import pandas as pd
from typing import Dict, List, Optional, Union

class DatasetProcessor:
  
    def __init__(self, file_path: str, expected_columns: Optional[List[str]] = None, expected_dtypes: Optional[Dict[str, str]] = None, date_column: str = "Дата оплаты"):
        self.file_path = file_path
        self.expected_columns = expected_columns
        self.expected_dtypes = expected_dtypes
        self.date_column = date_column
        self.df = None
        self.duplicates_removed = 0

    def __invert__(self) -> 'DatasetProcessor':
       
        if self.df is None:
            raise ValueError("Данные не загружены. Сначала выполните validate_and_load().")
            
        before = len(self.df)
        self.df = self.df.drop_duplicates()
        self.duplicates_removed = before - len(self.df)
        return self
    
    def show_removed_count(self) -> None:
        """Выводит количество удалённых дубликатов."""
        print(f"Удалено дубликатов: {self.duplicates_removed}")
    
    def get_data(self) -> pd.DataFrame:
        
        if self.df is not None:
            return self.df
        raise ValueError(
            "Данные не были загружены. Сначала выполните метод validate_and_load()."
        )
        
    def validate_and_load(self) -> bool:
       
        try:
            self._check_file_exists()
            self._load_dataset()
            
            if self.expected_columns:
                self._check_columns()
            if self.expected_dtypes:
                self._check_dtypes()
                
            # Преобразование даты
            self._convert_date_column()
                
            print("Проверка и загрузка датасета успешно завершены!")
            return True
            
        except Exception as e:
            print(f"Ошибка при валидации датасета: {str(e)}")
            return False
    
    def _check_file_exists(self) -> None:
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(
                f"Файл не найден по указанному пути: {self.file_path}"
            )
            
        if not os.path.isfile(self.file_path):
            raise IsADirectoryError(
                f"Указанный путь ведет к директории, а не к файлу: {self.file_path}"
            )
    
    def _load_dataset(self) -> None:
        
        try:
            if self.file_path.endswith('.csv'):
                self.df = pd.read_csv(self.file_path)
            elif self.file_path.endswith(('.xls', '.xlsx')):
                self.df = pd.read_excel(self.file_path)
            elif self.file_path.endswith('.json'):
                self.df = pd.read_json(self.file_path)
            else:
                raise ValueError(
                    f"Неподдерживаемый формат файла: {self.file_path}. "
                    "Поддерживаются CSV, Excel и JSON."
                )
                
        except Exception as e:
            raise Exception(
                f"Ошибка при чтении файла {self.file_path}: {str(e)}"
            )
    
    def _check_columns(self) -> None:
        missing_columns = set(self.expected_columns) - set(self.df.columns)
        extra_columns = set(self.df.columns) - set(self.expected_columns)
        
        errors = []
        if missing_columns:
            errors.append(
                f"Отсутствуют ожидаемые колонки: {', '.join(missing_columns)}"
            )
        if extra_columns:
            errors.append(
                f"Обнаружены лишние колонки: {', '.join(extra_columns)}"
            )
            
        if errors:
            raise ValueError(
                "Несоответствие структуры датасета:\n" + "\n".join(errors)
            )
    
    def _check_dtypes(self) -> None:
        
        type_errors = []
        
        for column, expected_type in self.expected_dtypes.items():
            if column not in self.df.columns:
                continue
                
            actual_type = str(self.df[column].dtype)
            if actual_type != expected_type:
                type_errors.append(
                    f"Колонка '{column}': ожидался тип {expected_type}, "
                    f"но получен {actual_type}"
                )
                
        if type_errors:
            raise TypeError(
                "Несоответствие типов данных:\n" + "\n".join(type_errors)
            )
    
    def _convert_date_column(self) -> None:
        
        if self.date_column in self.df.columns:
            self.df[self.date_column] = pd.to_datetime(
                self.df[self.date_column], 
                errors="coerce"
            )
    
    def split_by_date(self, split_date: Union[str, pd.Timestamp] = "2014-01-01") -> None:
        
        if self.df is None:
            raise ValueError("Данные не загружены. Сначала выполните validate_and_load().")
            
        if self.date_column not in self.df.columns:
            raise ValueError(f"Колонка с датой '{self.date_column}' не найдена в датасете.")
            
        split_date = pd.to_datetime(split_date)
        
        # Фильтруем строки с датой до split_date
        before_date = self.df[self.df[self.date_column] < split_date]
        
        # Фильтруем строки с датой начиная с split_date
        from_date = self.df[self.df[self.date_column] >= split_date]

        # Сохраняем оба датафрейма в отдельные CSV-файлы
        before_date.to_csv("data_before_cutoff.csv", index=False)
        from_date.to_csv("data_from_cutoff.csv", index=False)

        print("Файлы сохранены:")
        print("  data_before_cutoff.csv")
        print("  data_from_cutoff.csv")
    
    