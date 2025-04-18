# AgroLLM/message_router.py
"""
Маршрутизатор входящих сообщений AgroLLM.

Использование (пример):
-----------------------------------------------------------------
router = MessageRouter()
router.handle(
    msg_type="text",
    payload={"content": "11.09\nПУ Север..."},
    meta={"excel_path": "...", "date": "..."}
)
-----------------------------------------------------------------
Для обработки изображений достаточно передать msg_type="media"
и путь к файлу (или списку путей) в payload["paths"].
"""
from typing import Dict, Any, List, Union

from AgroLLM.process_messages import LLMProcess
from AgroLLM.process_photos import ImageProcessor


class MessageRouter:
    """Выбирает нужный процессор в зависимости от типа сообщения."""

    def __init__(self) -> None:
        self._text_processor = LLMProcess()
        self._image_processor = ImageProcessor()

    # ------------------------------------------------------------------ #
    # PUBLIC API
    # ------------------------------------------------------------------ #
    def handle(
        self,
        msg_type: str,
        payload: Dict[str, Any],
        meta: Dict[str, Any] | None = None,
    ) -> Union[Dict, List[Dict], None]:
        """
        :param msg_type: "text" | "media"
        :param payload:  для text  — {"content": "..."}  
                         для media — {"paths": ["a.jpg", ...], "prompt": "..."}
        :param meta:     вспомогательные параметры (excel_path, date и т.п.)
        :return:         результат обработки (пока печатаем в консоль — см. код)
        """
        meta = meta or {}

        if msg_type == "text":
            result = self._handle_text(payload, meta)
        elif msg_type == "media":
            result = self._handle_media(payload, meta)
        else:
            raise ValueError(f"Неизвестный тип сообщения: {msg_type}")

        # Пока просто печатаем ответ (дальше можно будет заменить на return)
        print("=== RESULT ===")
        print(result)
        return result

    # ------------------------------------------------------------------ #
    # INTERNALS
    # ------------------------------------------------------------------ #
    def _handle_text(self, payload: Dict[str, Any], meta: Dict[str, Any]):
        text = payload.get("content", "")
        return self._text_processor.process_messages(
            append_to_excel=meta.get("append_to_excel", lambda *a, **kw: None),
            message=text,
            exel_path=meta.get("excel_path", ""),
            date=meta.get("date", ""),
        )

    def _handle_media(self, payload: Dict[str, Any], meta: Dict[str, Any]):
        paths: list[str] = payload.get("paths", [])
        prompt: str = payload.get(
            "prompt",
            "Опиши, что изображено на фото. Ответ только на русском.",
        )
        return self._image_processor.process_images(paths, prompt)
