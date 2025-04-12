from flask import Flask, request, jsonify
import os
import json
from datetime import datetime

app = Flask(__name__)

# Константа для хранения всех данных
BASE_DIR = 'agro_data'


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json

    # Проверка корректности входных данных
    if not data or 'from' not in data:
        return jsonify({'error': 'Invalid data'}), 400

    phone = data['from']
    date_str = datetime.utcnow().strftime('%Y-%m-%d')

    # Формирование структуры директорий
    user_dir = os.path.join(BASE_DIR, phone, date_str)
    media_dir = os.path.join(user_dir, 'media')
    os.makedirs(media_dir, exist_ok=True)

    # Обработка медиафайлов (если есть)
    if data.get('type') == 'media' and 'media_data' in data:
        ext = data.get('ext', 'bin')
        timestamp = int(datetime.utcnow().timestamp())
        media_filename = f'media_{timestamp}.{ext}'
        media_path = os.path.join(media_dir, media_filename)

        # Сохраняем медиафайл
        with open(media_path, 'wb') as media_file:
            media_file.write(bytes.fromhex(data['media_data']))

        # Добавляем путь к файлу в данные и удаляем media_data
        data['media_path'] = os.path.relpath(media_path, user_dir)
        data.pop('media_data')

    # Сохраняем сообщение в JSONL файл
    log_path = os.path.join(user_dir, 'messages.jsonl')
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(data, ensure_ascii=False) + '\n')

    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    app.run(debug=True)
