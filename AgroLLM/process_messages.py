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
import sys

# Убираем циклический импорт и используем абсолютный путь
from AgroLLM.LLM_config import chat_model_name, TEST_SIZE, api_key


class LLMProcess:
    """Класс для обработки сырых агросообщений с помощью LLM модели"""


    def __init__(self):
        """Инициализация параметров и конфигураций"""

        # Название модели, указывается в конфиге
        self.model_name = chat_model_name
        self.api_key = api_key

        # Размер тестовой выборки
        self.TEST_SIZE = TEST_SIZE

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
        for i, example in enumerate(examples, 1):
            message = example.get("Сообщение", "")
            data = example.get("Данные", [])

            examples_text += f"\nПример {i}:\n```\n{message}\n```\n\n"

            if data:
                examples_text += "Обработанные данные:\n"
                for item in data:
                    examples_text += f"- {item}\n"
                examples_text += "\n"

        system_prompt = f"""Ты - эксперт по обработке агросообщений. Твоя задача - извлекать структурированную информацию из сообщений.

АЛГОРИТМ ОБРАБОТКИ ДАТЫ (САМОЕ ВАЖНОЕ - ВЫПОЛНЯТЬ В ПЕРВУЮ ОЧЕРЕДЬ):
1. Перед любой другой обработкой, ВСЕГДА проверяй первую строку сообщения.
2. Если эта строка содержит ТОЛЬКО числа с точкой между ними (например, "11.09", "12.10", "6.05") - ЭТО ДАТА в формате ДД.ММ.
3. Если строка выглядит как "11.09", "12.10", "13.08" и т.д. - это дата в формате ДД.ММ без года.
4. При обнаружении даты в таком формате, ОБЯЗАТЕЛЬНО добавь ее в поле "Дата" в формате "дд.мм.гггг".
5. НИКОГДА не оставляй поле даты пустым, если в сообщении есть строки вида "11.09", "6.05", "24.12" и т.п.

ПРИМЕРЫ ВЫДЕЛЕНИЯ ДАТЫ:
- Если первая строка сообщения: "11.09" → Дата = "11.09.2025"
- Если первая строка сообщения: "6.05" → Дата = "06.05.2025"
- Если первая строка сообщения: "13.08" → Дата = "13.08.2025"

Справочная информация:
{extra_info_text}

ВАЖНО: Примеры приведены только для демонстрации формата обработки. Не используй никакие данные из примеров при обработке реальных сообщений. Извлекай информацию ТОЛЬКО из текста самого сообщения:

1. Дата - ОБЯЗАТЕЛЬНО извлекать, если она указана в первой строке сообщения в формате ЧЧ.ЧЧ
2. Подразделение - берется из сообщения и строго по справочнику 
3. Операция - берется из сообщения и строго по справочнику
4. Культура - берется из сообщения и строго по справочнику
5. Числовые значения - только из текста сообщения

ДОПОЛНИТЕЛЬНО ПРО ДАТЫ:
- Дата ПОЧТИ ВСЕГДА указывается в первой строке сообщения
- Дата может быть в форматах: дд.мм, д.мм, дд.м или д.м (например: 11.09, 6.05, 13.8, 1.2)
- Другие форматы даты: дд.мм.гггг, дд.мм.ггг. (например: 30.03.25г., 29.06.2025)
- Иногда после даты может быть слово "день" (например: 24.12 день)
- Если год не указан явно, используй 2025
- Первая строка сообщения, содержащая только числа с точкой - это ВСЕГДА дата

ЖЕСТКИЕ ПРАВИЛА:
- Если ты вообще не видишь какой-либо информации в сообщении, то в соответствующем поле должна быть пустая строка.
- Никогда не подставляй даты или другие данные из примеров
- Используй только ту информацию, которая есть в текущем обрабатываемом сообщении

Основные правила:
1. Название подразделения может быть с префиксом "ПУ" или без него
2. "Многолетние травы" = "Многолетние травы текущего года"
3. В значениях "Вал" часто пропущена десятичная точка перед последними двумя цифрами (37480 = 374.8 или 264491 = 2644.91)
4. Используй точные названия операций из справочника
5. Пустые значения = пустая строка (""), не null и не "N/A"

Примеры обработки:
{examples_text}

Формат ответа - JSON-массив:
[
    {{
        "Дата": "дд.мм.гггг",
        "Подразделение": "ПУ \\"Север\\"",
        "Операция": "Сев",
        "Культура": "Сахарная свекла",
        "За день, га": "85",
        "С начала операции, га": "210",
        "Вал за день, ц": "xxx.xx",
        "Вал с начала, ц": "xxxx.xx"
    }}
]

Примеры сокращений (могут быть и другие):
- "св" или "с. св" = "Сахарная свекла"
- "подс" = "Подсолнечник"
- "оз п" = "Озимая пшеница"
- "кук" = "Кукуруза"
- "мн тр" или "многол трав" = "Многолетние травы текущего года"
- "пах" = "Пахота"
- "диск" = "Дискование"
- "культ" = "Культивация"
- "предп культ" = "Предпосевная культивация"
- "сев" = "Сев"

Обработка детализации по отделениям:
1. Если есть общее значение для ПУ и детализация по отделениям - используй только общее значение
2. Если есть только детализация по отделениям - суммируй значения и создай одну запись для ПУ
3. Не создавай отдельные записи для отделений

Особенности культур:
2-е диск сах св под пш - тут культура пшеница озимая товарная, а не сахарная свекла. Культура - это то, под которое проводится операция.

ВАЖНО: даже если ты видишь очень похожее сообщение как в примерах, но в нем нет какой-то информации, которая есть в примере, тебе не нужно дополнять этой информацией результат. Смотри только на само сообщение для обработки."""
    
        return system_prompt


    def process_messages_batch(self, messages: List[str], system_prompt: str, client: OpenAI) -> List[Dict]:
        """Обработка батча сообщений с помощью API"""

        # Объединяем сообщения в один текст с разделителями
        #batch_text = "\n\n=== NEXT MESSAGE ===\n\n".join(messages)

        messages_for_api = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": messages}
        ]

        try:
            completion = client.chat.completions.create(
                model=self.model_name,
                messages=messages_for_api,
                temperature=0.1,
                extra_headers={"X-Title": "Agro Message Parser"}
            )

            response = completion.choices[0].message.content

            print(response)

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
        print(messages)
        batch_results = self.process_messages_batch(messages, system_prompt, client)

        #print(batch_results)

        # result_df = pd.DataFrame(batch_results)

        # result_df.to_excel("test_results/16_04_xlsx", index=False)

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
            # Обрабатываем сообщения
            results = self.process_agro_messages(
                messages=message,
                save_to_excel=True
            )

            # Сохраняем все операции из сообщения
            for operation in results:
                append_to_excel(filepath=exel_path, message_dict=operation, date_value=date)

            return results
        except Exception as e:
            #self.logger.error(f"Ошибка в основной функции: {e}")
            raise