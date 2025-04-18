from flask import Flask, request, jsonify
from datetime import datetime
from pathlib import Path

from app import config
from app.AgroLLM.src.process_messages import LLMProcess
from app.AgroLLM.src.process_photos import ImageProcessor
from app.user_and_system_interface.src.data_save import DataSave


# Инициализация компонентов
data_save = DataSave(config.BASE_DIR, config.EXEL_TABLE_BASE_NAME)
llm_process_text = LLMProcess()
llm_process_image = ImageProcessor()

app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Обработчик входящих данных с вебхука.
    Сохраняет текстовые сообщения и медиафайлы в директории:
    agro_data/{номер_телефона}/{дата}/
    """
    data = request.get_json()

    if not data or 'from' not in data:
        return jsonify({'error': 'Invalid data: missing "from" field'}), 400

    # Директория для медиафайлов
    media_dir = Path(config.BASE_DIR) / config.MEDIA_DIR
    media_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    results = {}

    # Обработка медиафайлов
    if data.get('type') == 'media' and 'media_data' in data:
        ext = data.get('ext', 'bin')
        timestamp = int(datetime.utcnow().timestamp())
        media_filename = f'media_{timestamp}.{ext}'
        media_path = media_dir / media_filename

        try:
            with open(media_path, 'wb') as f:
                f.write(bytes.fromhex(data['media_data']))
        except ValueError:
            return jsonify({'error': 'Invalid media_data format'}), 400

        # Получаем результаты OCR
        ocr_results = llm_process_image.process_images(
            image_paths=[str(media_path)],
            prompt="На этой фотографии должна быть таблица. Перепиши содержимое таблицы..."
        )

        # Обрабатываем результаты OCR
        results = llm_process_image.process_ocr_results(ocr_results)

    else:
        # Сохраняем текстовые данные
        text_dir = Path(config.BASE_DIR) / config.TEXT_DIR
        text_dir.mkdir(parents=True, exist_ok=True)
        data_save.save_to_txt(data, str(text_dir))

        # Обработка текста через LLM
        results = llm_process_text.process_messages(
            data.get("content", "")
        )

    # Сохраняем результат в Excel
    excel_dir = Path(config.BASE_DIR) / config.EXEL_TABLE_DIR
    excel_dir.mkdir(parents=True, exist_ok=True)
    filename = now.strftime("%M_%H_%d_%m_%Y")

    data_save.append_message_to_table(
        message_dict=results,
        filepath=str(excel_dir),
        date_value=filename
    )

    return jsonify({'status': 'ok'}), 200
