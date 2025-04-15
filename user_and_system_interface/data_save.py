import os
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment

class DataSave:
    """Сохраняет данные формата JSON в различные форматы"""

    def __init__(self, base_dir):
        """init"""
        self.base_dir = base_dir
        self.headers = [
            "Дата", "Подразделение", "Операция", "Культура",
            "За день, га", "Начала операции", "Вал за день, ц", "Вал с начала, ц"
        ]

        os.makedirs(base_dir, exist_ok=True)


    def convert_iso_to_custom_format(self, iso_string):
        """Преобразует ISO-дату в формат: Минута_Час_День_Месяц_Год"""
        dt = datetime.strptime(iso_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%M_%H_%d_%m_%Y")


    def _get_next_message_number(self, sender):
        """Подсчитывает количество файлов от отправителя"""
        return sum(sender in filename for filename in os.listdir(self.base_dir)) + 1


    def save_to_txt(self, data):
        """Сохраняет данные в .txt файл"""

        sender = data["from"]
        timestamp = data["timestamp"]
        content = data["content"]

        message_num = self._get_next_message_number(sender)
        timestamp_str = self.convert_iso_to_custom_format(timestamp)
        file_name = f"{sender}_{message_num}_{timestamp_str}.txt"
        #file_path = os.path.join(self.base_dir, file_name)
        file_path = self.base_dir + "/" + file_name
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content + '\n')


    def append_to_excel(self, excel_path, data_list):
        """Открывает существующий Excel-файл и добавляет в него строки из JSON-данных."""

        wb = load_workbook(excel_path)
        ws = wb.active  # Можно уточнить имя листа, если нужно

        # Определяем, с какой строки начинать (после последней непустой)
        start_row = ws.max_row + 1

        yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        for row_index, entry in enumerate(data_list, start=start_row):
            # Добавляем данные в ячейки
            ws.cell(row=row_index, column=1, value=entry.get("date"))
            ws.cell(row=row_index, column=2, value=entry.get("department"))
            ws.cell(row=row_index, column=3, value=entry.get("operation"))
            ws.cell(row=row_index, column=4, value=entry.get("crop"))
            ws.cell(row=row_index, column=5, value=entry.get("area_per_day"))
            ws.cell(row=row_index, column=6, value=entry.get("operation_start"))
            ws.cell(row=row_index, column=7, value=entry.get("yield_per_day"))
            ws.cell(row=row_index, column=8, value=entry.get("yield_total"))

            # Проверяем значения и закрашиваем ячейки, если они равны "none"
            for col_index in range(1, 9):  # Обходим все 8 колонок
                cell_value = ws.cell(row=row_index, column=col_index).value
                if cell_value == "none":
                    ws.cell(row=row_index, column=col_index).fill = yellow_fill

        wb.save(excel_path)
