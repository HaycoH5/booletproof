# AgroLLM/process_images.py

import os
import base64
import json
from typing import List, Dict
from openai import OpenAI
from app.AgroLLM.src.LLM_config import api_key, vision_model_name
from datetime import datetime, timezone
import pandas as pd

__all__ = ["ImageProcessor"]

# Ваш системный промт
_SYSTEM_PROMPT = """
Ты – эксперт по распознаванию табличных данных на фотографиях сельхоз‑учёта.
Твоя задача:
1. Найти на картинке таблицу.
2. Переписать названия колонок и все числовые значения.
3. Вернуть только JSON, только русские заголовки и цифры.
""".strip()



class ImageProcessor:
    """OCR/vision‑процессор изображений с помощью Gemini‑2 или другой vision‑модели."""

    def __init__(self) -> None:
        self.model_name = vision_model_name
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://api.vsegpt.ru/v1",
        )
        self.instruction_data = self._read_instruction_file()

    def _read_instruction_file(self) -> Dict:
        """Чтение файла с инструкциями (JSON)"""
        try:
            instruction_path = os.path.join(os.path.dirname(__file__), "..", "data", "instruction.json")
            with open(instruction_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка при чтении файла инструкции: {e}")
            return {}

    def _create_system_prompt(self) -> str:
        """Создание системного промпта на основе инструкции"""
        extra_info_text = ""

        # Добавление таблицы принадлежности отделений к ПУ
        if "справочники" in self.instruction_data and "принадлежность_подразделений" in self.instruction_data["справочники"]:
            extra_info_text += "\n=== Принадлежность отделений и ПУ ===\n"
            dept_data = self.instruction_data["справочники"]["принадлежность_подразделений"]
            dept_df = pd.DataFrame(dept_data)
            extra_info_text += dept_df.to_string(index=False)
            extra_info_text += "\n"

        # Добавление справочника операций
        if "справочники" in self.instruction_data and "операции" in self.instruction_data["справочники"]:
            extra_info_text += "\n=== Операции ===\n"
            operations_data = self.instruction_data["справочники"]["операции"]
            operations_df = pd.DataFrame(operations_data)
            extra_info_text += operations_df.to_string(index=False)
            extra_info_text += "\n"

        # Добавление списка культур
        if "справочники" in self.instruction_data and "культуры" in self.instruction_data["справочники"]:
            extra_info_text += "\n=== Культуры ===\n"
            crops = self.instruction_data["справочники"]["культуры"]
            extra_info_text += "\n".join([f"- {crop}" for crop in crops])
            extra_info_text += "\n"

        system_prompt = f"""Ты - эксперт по обработке агросообщений. Твоя задача - извлекать структурированную информацию из табличных данных.

Формат входных данных - JSON с таблицей, которая содержит:
- Наименование тех операции
- Данные по отделениям (отд7, отд10, отд20, отдз)
- Итоги по ПУ

Справочная информация:
{extra_info_text}

ПРАВИЛА ОБРАБОТКИ:
1. Каждая строка таблицы должна быть преобразована в отдельную запись
2. Подразделение определяется по отделению (см. справочник принадлежности)
3. Операция берется из поля "Наименование тех операции" и должна строго соответствовать справочнику операций
4. Культура определяется из названия операции и должна строго соответствовать справочнику культур
5. Значения "За день" и "С начала" берутся из соответствующих итоговых полей
6. Поля "Вал" оставляем пустыми, так как в таблице нет этих данных
7. Дата должна быть в формате "дд.мм.гггг" (если год не указан, используй текущий)

ОСОБЕННОСТИ ОБРАБОТКИ:
1. Если в названии операции есть сокращения:
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

2. Если в таблице есть общее значение для ПУ и детализация по отделениям - используй только общее значение
3. Если есть только детализация по отделениям - суммируй значения и создай одну запись для ПУ
4. Не создавай отдельные записи для отделений

Формат ответа - JSON-массив:
[
    {{
        "Дата": "дд.мм.гггг",
        "Подразделение": "название ПУ",
        "Операция": "название операции",
        "Культура": "название культуры",
        "За день, га": "число",
        "С начала операции, га": "число",
        "Вал за день, ц": "",
        "Вал с начала, ц": ""
    }}
]"""

        return system_prompt

    def process_images(self, image_paths: List[str], prompt: str) -> List[Dict]:
        """
        :param image_paths: список путей к файлам
        :param prompt: текст‑инструкция (user_prompt), который отправляем к модели
        :return: список dict‑ов с распознанным/сгенерированным текстом
        """
        results: List[Dict] = []
        for path in image_paths:
            if not os.path.isfile(path):
                print(f"⚠️ Файл не найден: {path}")
                continue
            try:
                answer = self._describe_single(path, prompt)
                results.append({"file": os.path.basename(path), "answer": answer})
            except Exception as err:
                print(f"Ошибка при обработке {path}: {err}")
        return results

    def _describe_single(self, image_path: str, user_prompt: str) -> str:
        """Отправляет одно изображение + инструкции в модель и возвращает ответ."""
        data_url = self._encode_to_data_url(image_path)

        messages = [
            # 1) системный промт
            {"role": "system", "content": _SYSTEM_PROMPT},
            # 2) пользовательская инструкция
            {"role": "user", "content": user_prompt.strip()},
            # 3) само изображение
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": data_url}}
                ],
            },
        ]

        resp = self.client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=0.0,
            max_tokens=2048,
            extra_headers={"X-Title": "AGRO OCR"},
        )
        return resp.choices[0].message.content.strip()

    @staticmethod
    def _encode_to_data_url(image_path: str) -> str:
        """Конвертирует файл в data‑URL (base64)."""
        mime = "jpeg" if image_path.lower().endswith((".jpg", ".jpeg")) else "png"
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/{mime};base64,{b64}"

    def process_ocr_results(self, ocr_results: List[Dict]) -> List[Dict]:
        """Обработка данных из OCR результатов изображений
        
        Args:
            ocr_results: Список словарей с результатами OCR, где каждый словарь содержит:
                        - file: имя файла
                        - answer: JSON строка с данными
        
        Returns:
            List[Dict]: Список обработанных записей в стандартном формате
        """
        try:
            # Создаем системный промпт для обработки табличных данных
            system_prompt = self._create_system_prompt()

            processed_results = []
            
            for result in ocr_results:
                try:

                    # Проверяем, что ответ не пустой
                    if not result['answer'].strip():
                        print(f"Пустой ответ OCR для файла {result.get('file', 'unknown')}")
                        continue

                    # Парсим JSON из строки answer
                    try:
                        data = json.loads(result['answer'].replace('```json\n', '').replace('\n```', ''))
                    except json.JSONDecodeError as e:
                        print(f"Ошибка парсинга JSON: {str(e)}")
                        print(f"Содержимое ответа OCR: {result['answer']}")
                        continue
                    
                    # Формируем сообщение для API
                    message = json.dumps(data, ensure_ascii=False)
                    
                    messages_for_api = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": message}
                    ]

                    # Отправляем запрос к API
                    completion = self.client.chat.completions.create(
                        model=self.model_name,
                        messages=messages_for_api,
                        temperature=0.1,
                        extra_headers={"X-Title": "Agro Table Parser"}
                    )

                    response = completion.choices[0].message.content
                    
                    # Выводим ответ API для отладки
                    print("API response:")
                    print(response)
                    print("-" * 50)
                    
                    # Очищаем ответ от маркеров markdown
                    response = response.replace('```json\n', '').replace('\n```', '').strip()
                    
                    # Парсим результат
                    try:
                        parsed_results = json.loads(response)
                        # Заменяем None на пустые строки в результатах
                        for result in parsed_results:
                            for key in result:
                                if result[key] is None:
                                    result[key] = ""
                        processed_results.extend(parsed_results)
                    except json.JSONDecodeError as e:
                        print(f"Ошибка парсинга JSON: {str(e)}")
                        print(f"Содержимое ответа API: {response}")
                        continue
                    
                except Exception as e:
                    print(f"Ошибка при обработке файла {result.get('file', 'unknown')}: {str(e)}")
                    continue

            return processed_results

        except Exception as e:
            print(f"Ошибка в process_ocr_results: {str(e)}")
            return []


# if __name__ == "__main__":
#     # локальный тест
#     processor = ImageProcessor()
#     # замените на путь к вашей тестовой картинке
#     demo = processor.process_images(
#         image_paths=["1.jpg"],
#         prompt="На этой фотографии должна быть таблица. Перепиши..."
#     )
#     print(demo)
