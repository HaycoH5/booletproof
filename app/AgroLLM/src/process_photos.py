# AgroLLM/process_images.py

import os
import base64
from typing import List, Dict
from openai import OpenAI
from app.AgroLLM.src.LLM_config import api_key, vision_model_name
from datetime import datetime, timezone

__all__ = ["ImageProcessor"]

# Ваш системный промт
_SYSTEM_PROMPT = """
Ты – эксперт по распознаванию табличных данных на фотографиях сельхоз‑учёта.
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

    def process_images(self, image_paths: List[str], prompt: str) -> List[Dict]:
        """
        :param image_paths: список путей к файлам
        :param prompt: текст‑инструкция (user_prompt), который отправляем к модели
        :return: список dict‑ов с распознанным/сгенерированным текстом
        """
        results: List[Dict] = []
        for path in image_paths:
            if not os.path.isfile(path):
                print(f"⚠️ Файл не найден: {path}")
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


if __name__ == "__main__":
    # локальный тест
    processor = ImageProcessor()
    # замените на путь к вашей тестовой картинке
    demo = processor.process_images(
        image_paths=["1.jpg"],
        prompt="На этой фотографии должна быть таблица. Перепиши..."
    )
    print(demo)
