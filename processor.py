import pandas as pd  

class DatasetProcessor:
    def __init__(self, file_path):
       
        self.df = pd.read_csv(file_path)

        # Преобразуем столбец "Дата оплаты" в формат даты.
        # errors="coerce" — если в ячейке неверный формат, будет записано NaT (Not a Time)
        self.df["Дата оплаты"] = pd.to_datetime(self.df["Дата оплаты"], errors="coerce")
        
        # Счётчик удалённых дубликатов
        self.duplicates_removed = 0

    def split_by_date(self):
        # Фильтруем строки с датой оплаты до 2014 года 
        before_2014 = self.df[self.df["Дата оплаты"] < pd.Timestamp("2014-01-01")]
        
        # Фильтруем строки с датой оплаты с 2014 года и позже
        from_2014 = self.df[self.df["Дата оплаты"] >= pd.Timestamp("2014-01-01")]

        # Сохраняем оба датафрейма в отдельные CSV-файлы
        before_2014.to_csv("data_before_2014.csv", index=False)
        from_2014.to_csv("data_from_2014.csv", index=False)

        # Выводим 
        print("Файлы сохранены:")
        print("  data_before_2014.csv")
        print("  data_from_2014.csv")

    def __invert__(self):
        # Переопределяем унарный оператор ~ для удаления дубликатов
        before = len(self.df)  # Количество строк до удаления
        self.df = self.df.drop_duplicates()  # Удаление дубликатов
        after = len(self.df)  # Количество строк после удаления
        self.duplicates_removed = before - after  # Сохраняем, сколько удалено
        return self  # Возвращаем объект 

    def show_removed_count(self):
        # Выводим количество удалённых дубликатов
        print(f"Удалено дубликатов: {self.duplicates_removed}")
