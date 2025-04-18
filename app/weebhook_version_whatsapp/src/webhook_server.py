from flask import Flask, request, jsonify
from datetime import datetime, timezone
from pathlib import Path
import os
from app.AgroLLM.src.process_messages import LLMProcess
from app.user_and_system_interface.src.data_save import DataSave
from app import config

# Инициализация компонентов
data_save = DataSave(config.BASE_DIR, config.EXEL_TABLE_BASE_NAME)
llm_process = LLMProcess()



app = Flask(__name__)


# Базовая директория для хранения данных
BASE_DIR = Path('agro_data')


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

    phone = data['from']
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Создание директорий для пользователя и медиафайлов
    user_dir = BASE_DIR / phone / date_str
    media_dir = user_dir / 'media'
    os.makedirs(media_dir, exist_ok=True)

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

        # Удаление данных медиа и сохранение относительного пути
        data['media_path'] = str(media_path.relative_to(user_dir))
        data.pop('media_data', None)

    # Сохраняем данные
    print(data)
    data_save.save_to_txt(data)

    # Обработка сообщений через LLM
    response = llm_process.process_messages(
        data_save.append_message_to_table,
        data.get("content", ""),
        config.EXEL_TABLE_DIR,
        data.get("timestamp", "")
    )
    print(response)

    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(debug=True)
