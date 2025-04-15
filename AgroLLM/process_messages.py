import pandas as pd
import openai
from openai import OpenAI
import os
from typing import List, Dict, Optional
import json
import getpass
import random
import logging
from datetime import datetime

from AgroLLM import LLM_config


class LLMProcess:
    """Класс для обработки сырых агросообщений с помощью LLM модели"""


    def __init__(self):
        """Инициализация параметров и конфигураций"""

        # Название модели, указывается в конфиге
        self.model_name = LLM_config.model_name
        self.api_key = LLM_config.api_key

        # Размер тестовой выборки
        self.TEST_SIZE = LLM_config.TEST_SIZE

        # Определяем директорию текущего скрипта
        self.SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.DATA_DIR = os.path.join(self.SCRIPT_DIR, "data")  # Путь к данным
        self.LOGS_DIR = os.path.join(self.SCRIPT_DIR, "logs")  # Путь к логам

        # Настройка логирования
        # logging.basicConfig(
        #     level=logging.INFO,
        #     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        #     handlers=[
        #         logging.FileHandler(
        #             os.path.join(self.LOGS_DIR, f'processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        #         logging.StreamHandler()
        #     ]
        # )
        # self.logger = logging.getLogger(__name__)


    def read_instruction_file(self):
        """Чтение файла с инструкциями (JSON)"""

        try:
            instruction_path = os.path.join(self.DATA_DIR, "instruction.json")

            with open(instruction_path, 'r', encoding='utf-8') as f:
                instruction_data = json.load(f)
            return instruction_data

        except Exception as e:
            #self.logger.error(f"Ошибка при чтении файла инструкции: {e}")
            raise


    def create_system_prompt(self, instruction_data: Dict) -> str:
        """Создание системного промпта на основе инструкции"""
        
        examples = instruction_data.get("примеры_обработки", [])
        extra_info_text = ""

        # Добавление таблицы принадлежности отделений к ПУ
        if "справочники" in instruction_data and "принадлежность_подразделений" in instruction_data["справочники"]:
            extra_info_text += "\n=== Принадлежность отделений и ПУ ===\n"
            dept_data = instruction_data["справочники"]["принадлежность_подразделений"]
            dept_df = pd.DataFrame(dept_data)
            extra_info_text += dept_df.to_string(index=False)
            extra_info_text += "\n"

        # Добавление справочника операций
        if "справочники" in instruction_data and "операции" in instruction_data["справочники"]:
            extra_info_text += "\n=== Операции ===\n"
            operations_data = instruction_data["справочники"]["операции"]
            operations_df = pd.DataFrame(operations_data)
            extra_info_text += operations_df.to_string(index=False)
            extra_info_text += "\n"

        # Добавление списка культур
        if "справочники" in instruction_data and "культуры" in instruction_data["справочники"]:
            extra_info_text += "\n=== Культуры ===\n"
            crops = instruction_data["справочники"]["культуры"]
            extra_info_text += "\n".join([f"- {crop}" for crop in crops])
            extra_info_text += "\n"

        # Форматирование примеров
        examples_text = ""
        for i, example in enumerate(examples[:3], 1):  # Use first 3 examples
            message = example.get("Сообщение", "")
            data = example.get("Данные", [])

            examples_text += f"\nExample {i}:\n```\n{message}\n```\n\n"

            if data:
                examples_text += "Should be parsed into:\n"
                for item in data:
                    examples_text += f"- {item}\n"
                examples_text += "\n"

        system_prompt = f"""You are an expert in parsing agricultural messages. Your task is to extract structured information from messages.

    Reference information for parsing:
    {extra_info_text}

    IMPORTANT: When filling the "Подразделение" field, use the table "Принадлежность отделений и ПУ" from the reference information above to determine the correct department name. For example, if you see "Отд 11", look up which "ПУ" it belongs to in the reference table and use that as the "Подразделение" value.

    IMPORTANT: The department name can be specified with the prefix "ПУ" (e.g., "ПУ Восход") or without it (just "Восход"). In both cases, it is the same department. If the message specifies a department name without the "ПУ" prefix, still use it as the full department name.

    IMPORTANT: When you see "Многолетние травы" in the message, ALWAYS use the full name "Многолетние травы текущего года" in the "Культура" field.

    IMPORTANT: For "Вал" values (both "Вал за день, ц" and "Вал с начала, ц"), be aware that decimal points are often omitted in messages. For example, if you see "37400", it likely means "374.00". If a value seems unusually large (more than 10000), consider that it might be missing a decimal point. In such cases, add a decimal point before the last two digits.

    IMPORTANT: For the "Операция" field, you MUST use EXACTLY the operation name from the "Операции" table in the reference information. Do not use variations or abbreviations. For example, if the message says "Посев", but the correct operation in the table is "Сев", you should use "Сев". If you're unsure which operation to choose, carefully analyze the message and select the most appropriate one from the table.

    IMPORTANT: For numeric fields (like "За день, га", "С начала операции, га", "Вал за день, ц", "Вал с начала, ц"), if you cannot extract a value, use an empty string ("") instead of null or "N/A". Never return null values for these fields.

    Examples of message processing:
    {examples_text}

    I will provide you with messages. Each message might contain one or multiple operations.
    Please analyze each message and extract the following information for each operation:
    1. Дата (Date) - if specified for the whole message, use it for all operations
    2. Подразделение (Department/Unit) - Use the "Принадлежность отделений и ПУ" table to determine the correct ПУ name for each department
    3. Операция (Operation type) - the main agricultural operation being performed
    4. Культура (Crop) - if specified once for multiple operations, use it for all related operations
    5. За день, га (Area per day, hectares) - first number in pairs like "41/501"
    6. С начала операции, га (Total area since operation start, hectares) - second number in pairs like "41/501"
    7. Вал за день, ц (Yield per day, centners)
    8. Вал с начала, ц (Total yield since operation start, centners)

    Format your response as a JSON array, where each element represents one operation:
    [
        {{
            "Дата": "",
            "Подразделение": "ПУ \\"Север\\"",
            "Операция": "Сев",
            "Культура": "Сахарная свекла",
            "За день, га": "85",
            "С начала операции, га": "210",
            "Вал за день, ц": "",
            "Вал с начала, ц": "",
        }},
        // more operations if present in the message
    ]

    Important notes:
    1. If you can't extract some values, leave them empty (empty string). Never return null or "N/A".
    2. For each operation, include ALL relevant context in the "Исходное сообщение" field, even if it's shared between multiple operations.
    3. Make sure to properly escape newlines (\\n) and quotes (\") in the "Исходное сообщение" field.
    4. Pay attention to abbreviated crop names:
       - "св" or "с. св" = "Сахарная свекла"
       - "подс" = "Подсолнечник"
       - "оз п" = "Озимая пшеница"
       - "кук" = "Кукуруза"
       - "мн тр" or "многол трав" = "Многолетние травы текущего года"
    5. Common operation abbreviations:
       - "пах" = "Пахота"
       - "диск" = "Дискование"
       - "культ" = "Культивация"
       - "предп культ" = "Предпосевная культивация"
       - "сев" = "Сев"
    6. When an operation has a total for "ПоПу" (or "По пу") and then individual department values, create separate entries for each department.
    7. ALWAYS use the "Принадлежность отделений и ПУ" table to determine the correct ПУ name for each department number.
    8. When you see a value like "152га" or "97га", extract both numbers for "За день, га" and "С начала операции, га" fields. For example, from "152га" extract "152" for both fields.
    9. When you see a percentage like "100%" or "50%", this indicates the completion percentage of the operation, but you should still extract the area values.

    ВАЖНО: При обработке сообщений с детализацией по отделениям (например, "Отд 11-307/307", "Отд 12-671/671" и т.д.):

    10. Если в сообщении указано общее значение для подразделения (ПУ) и затем детализация по отделениям, 
        ВСЕГДА используйте ТОЛЬКО общее значение для подразделения и игнорируйте детализацию по отделениям.
        
    11. Пример: "2-я подкормка озимых, ПУ "Юг" - 1850/2700 (в т.ч Амазон-1150/1450, Пневмоход-700/1250)
        Отд11- 307/307 (амазон 307/307) 
        Отд 12- 671/671( амазон 318/318; пневмоход 353/353)"
        
        В этом случае нужно создать ТОЛЬКО ОДНУ запись для ПУ "Юг" со значениями 1850/2700, 
        а НЕ создавать отдельные записи для каждого отделения.
        
    12. Если в сообщении указано только детализация по отделениям без общего значения для подразделения,
        суммируйте значения всех отделений и создайте ОДНУ запись для соответствующего подразделения.
        
    13. Пример: "Внесение мин удобрений под оз пшеницу 2025 г ПУ Юг 165/7800
        Отд 17-165/1550"
        
        В этом случае нужно создать ОДНУ запись для ПУ "Юг" со значениями 165/7800,
        а НЕ создавать отдельную запись для Отд 17.
        
    14. Помните: ваша задача - создать записи на уровне подразделения (ПУ), а не на уровне отделений.
        Детализация по отделениям предоставляется только для информационных целей и не должна
        приводить к созданию отдельных записей в результатах."""
        
        return system_prompt


    def process_messages_batch(self, messages: List[str], system_prompt: str, client: OpenAI) -> List[Dict]:
        """Обработка батча сообщений с помощью API"""

        # Объединяем сообщения в один текст с разделителями
        batch_text = "\n\n=== NEXT MESSAGE ===\n\n".join(messages)

        messages_for_api = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": batch_text}
        ]

        try:
            completion = client.chat.completions.create(
                model=self.model_name,
                messages=messages_for_api,
                temperature=0.1,
                extra_headers={"X-Title": "Agro Message Parser"}
            )

            response = completion.choices[0].message.content

            try:
                # Пробуем распарсить ответ как JSON
                parsed_results = json.loads(response)

                if not isinstance(parsed_results, list):
                    parsed_results = [parsed_results]

                # Заменяем None и "N/A" на пустую строку и нормализуем числовые значения
                for result in parsed_results:
                    for key in result:
                        if result[key] is None or result[key] == "N/A":
                            result[key] = ""
                        elif key in ["За день, га", "С начала операции, га", "Вал за день, ц", "Вал с начала, ц"]:
                            # Нормализуем числовые значения
                            result[key] = self._normalize_numeric_value(result[key])

                return parsed_results

            except json.JSONDecodeError as e:
                #self.logger.error(f"Ошибка при разборе JSON: {e}")

                try:
                    import re
                    json_match = re.search(r'\[(.*)\]', response.replace('\n', ''))
                    if json_match:
                        fixed_json = json_match.group(0)
                        return json.loads(fixed_json)
                except:
                    pass

                # Возвращаем пустой результат при ошибке
                return [{
                    "Дата": "",
                    "Подразделение": "",
                    "Операция": "",
                    "Культура": "",
                    "За день, га": "",
                    "С начала операции, га": "",
                    "Вал за день, ц": "",
                    "Вал с начала, ц": "",
                    "Исходное сообщение": messages[0] if messages else ""
                }]

        except Exception as e:
            #self.logger.error(f"Ошибка при запросе к API: {e}")
            return [{
                "Дата": "",
                "Подразделение": "",
                "Операция": "",
                "Культура": "",
                "За день, га": "",
                "С начала операции, га": "",
                "Вал за день, ц": "",
                "Вал с начала, ц": "",
                "Исходное сообщение": messages if messages else ""
            }]
            
    def _normalize_numeric_value(self, value):
        """Нормализует числовые значения для корректного сравнения.
        
        Args:
            value: Значение для нормализации
            
        Returns:
            str: Нормализованное значение
        """
        if value is None or value == "":
            return ""
        
        # Преобразуем в строку, если это не строка
        if not isinstance(value, str):
            value = str(value)
        
        # Заменяем запятые на точки для десятичных чисел
        value = value.replace(',', '.')
        
        # Удаляем лишние пробелы
        value = value.strip()
        
        # Проверяем, является ли значение числом
        try:
            # Пробуем преобразовать в float
            float_value = float(value)
            
            # Проверяем, не является ли значение слишком большим (возможно, пропущена десятичная точка)
            if float_value > 1000000:  # Если значение больше миллиона
                # Проверяем, не является ли это значением с пропущенной десятичной точкой
                # Например, 1259680 может быть 12596.80
                if '.' not in value and len(value) > 6:
                    # Добавляем десятичную точку перед последними двумя цифрами
                    value = value[:-2] + '.' + value[-2:]
                    # Преобразуем обратно в float и форматируем
                    float_value = float(value)
            
            # Форматируем число с двумя знаками после запятой
            return f"{float_value:.2f}"
        except ValueError:
            # Если не удалось преобразовать в число, возвращаем исходное значение
            return value

    def process_agro_messages(self, messages: List[str], save_to_excel: bool = False,
                              output_path: Optional[str] = None) -> pd.DataFrame:
        """Обработка списка агросообщений, сохранение результата в DataFrame (и опционально — Excel)"""

        # Получаем ключ API и инициализируем клиент
        api_key = self.api_key
        client = OpenAI(
            api_key=api_key,
            base_url="https://api.vsegpt.ru/v1",
        )

        # Читаем файл с инструкцией
        instruction_data = self.read_instruction_file()

        # Создаем системный промпт
        system_prompt = self.create_system_prompt(instruction_data)

        #self.logger.info(f"Всего сообщений для обработки: {len(messages)}")


        # Обработка сообщений по батчам

        batch_results = self.process_messages_batch(messages, system_prompt, client)


        # Создаем финальный DataFrame
        columns = [
            "Дата",
            "Подразделение",
            "Операция",
            "Культура",
            "За день, га",
            "С начала операции, га",
            "Вал за день, ц",
            "Вал с начала, ц",
            "Исходное сообщение"
        ]

        results_df = pd.DataFrame(batch_results, columns=columns)

        #self.logger.info(f"Обработка завершена! Всего найдено операций: {len(batch_results)}")

        return batch_results


    def process_messages(self, append_to_excel, message, exel_path, date):
        """Основной запуск для тестирования скрипта"""
        try:
            # # Загружаем сообщения из Excel
            # messages_df = pd.read_excel(
            #     os.path.join(self.DATA_DIR, "messages.xlsx"),
            #     engine='openpyxl'
            # )


            # Обрабатываем сообщения
            results_df = self.process_agro_messages(
                messages=message,
                save_to_excel=True
            )

            append_to_excel(filepath=exel_path, message_dict=results_df[0], date_value=date)
            return results_df
        except Exception as e:
            #self.logger.error(f"Ошибка в основной функции: {e}")
            raise