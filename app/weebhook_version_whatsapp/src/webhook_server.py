from flask import Flask, request, jsonify
from datetime import datetime, timezone
from pathlib import Path
import os
from app.AgroLLM.src.process_messages import LLMProcess
from app.user_and_system_interface.src.data_save import DataSave
from app import config
from datetime import datetime

# Инициализация компонентов
data_save = DataSave(config.BASE_DIR, config.EXEL_TABLE_BASE_NAME)
llm_process = LLMProcess()
now = datetime.now()


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
    data_save.save_to_txt(data, config.BASE_DIR + "/" + config.TEXT_DIR)

    # Обработка сообщений через LLM
    results = response = llm_process.process_messages(
        data.get("content", "")
    )

    data_save.append_message_to_table(message_dict=results,
                                      filepath=(config.BASE_DIR + "/" + config.EXEL_TABLE_DIR),
                                      date_value=now.strftime("%M_%H_%d_%m_%Y"))
    print(data.get("timestamp"))
    return jsonify({'status': 'ok'})


