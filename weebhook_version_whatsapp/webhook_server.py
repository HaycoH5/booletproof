from flask import Flask, request, jsonify
import os
from datetime import datetime
from pathlib import Path

from AgroLLM.process_messages import LLMProcess
from user_and_system_interface.data_save import DataSave

import config

app = Flask(__name__)

data_save = DataSave(config.base_dir)
LLM_Process = LLMProcess()


# Базовая директория для хранения данных
BASE_DIR = Path('agro_data')


@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Обработчик входящих данных с WhatsApp-подобного вебхука.
    Сохраняет сообщения и, при наличии, медиафайлы в структуре каталогов:
    agro_data/{номер_телефона}/{дата}/
    """
    data = request.get_json()

    # Проверка, что данные есть и содержат номер отправителя
    if not data or 'from' not in data:
        return jsonify({'error': 'Invalid data: missing "from" field'}), 400

    phone = data['from']
    date_str = datetime.utcnow().strftime('%Y-%m-%d')

    # Создание необходимых директорий
    user_dir = BASE_DIR / phone / date_str
    media_dir = user_dir / 'media'
    os.makedirs(media_dir, exist_ok=True)

    # Обработка медиа-файлов, если они присутствуют
    if data.get('type') == 'media' and 'media_data' in data:
        ext = data.get('ext', 'bin')  # расширение по умолчанию
        timestamp = int(datetime.utcnow().timestamp())
        media_filename = f'media_{timestamp}.{ext}'
        media_path = media_dir / media_filename

        try:
            # Декодируем hex-строку в байты и сохраняем файл
            with open(media_path, 'wb') as f:
                f.write(bytes.fromhex(data['media_data']))
        except ValueError:
            return jsonify({'error': 'Invalid media_data format'}), 400

        # Сохраняем относительный путь к файлу в логах
        data['media_path'] = str(media_path.relative_to(user_dir))
        data.pop('media_data')  # Удаляем большие бинарные данные

        data_save.save_to_txt(data)
        LLM_Process.start(data_save.append_to_excel)

if __name__ == '__main__':
    # Запуск приложения в режиме отладки (для разработки)
    app.run(debug=True)
