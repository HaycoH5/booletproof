from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone, timedelta
from pathlib import Path
import os
import requests

from booletproof.AgroLLM.process_messages import LLMProcess
from booletproof.user_and_system_interface.data_save import DataSave

import config

app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()

# Настройки
BASE_DIR = Path('agro_data')
JS_SERVER_URL = 'http://localhost:3000/send'
EXCEL_FOLDER = './'
TARGET_FILE = '1115042025_BulletProof.xlsx'
PHONE_NUMBER = '...'
SEND_AT = (datetime.now() + timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S')

# Инициализация компонентов
data_save = DataSave(config.BASE_DIR, config.EXEL_TABLE_BASE_NAME)
llm_process = LLMProcess()


@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Обработчик входящих данных с вебхука.
    Сохраняет текстовые сообщения и медиафайлы, обрабатывает через LLM.
    """
    data = request.get_json()

    if not data or 'from' not in data:
        return jsonify({'error': 'Invalid data: missing "from" field'}), 400

    phone = data['from']
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    user_dir = BASE_DIR / phone / date_str
    media_dir = user_dir / 'media'
    os.makedirs(media_dir, exist_ok=True)

    # Обработка медиа
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

        data['media_path'] = str(media_path.relative_to(user_dir))
        data.pop('media_data', None)

    # Сохранение и обработка
    print(data)
    data_save.save_to_txt(data)

    response = llm_process.process_messages(
        data_save.append_message_to_table,
        data.get("content", ""),
        config.EXEL_TABLE_DIR,
        data.get("timestamp", "")
    )
    print(response)

    return jsonify({'status': 'ok'})


def send_file_to_whatsapp(phone, file_path, caption=''):
    with open(file_path, 'rb') as f:
        files = {
            'file': (os.path.basename(file_path), f)
        }
        data = {
            'phone': phone,
            'caption': caption
        }
        response = requests.post(JS_SERVER_URL, data=data, files=files)

    print(f"Отправка завершена: {response.status_code}, {response.text}")

    try:
        return response.json()
    except Exception as e:
        print(f"Ошибка при разборе JSON: {e}")
        return {'status': 'error', 'response': response.text, 'code': response.status_code}


def job():
    file_path = os.path.join(EXCEL_FOLDER, TARGET_FILE)
    if os.path.exists(file_path):
        print(f"Отправляем файл {file_path} получателю {PHONE_NUMBER}")
        send_file_to_whatsapp(PHONE_NUMBER, file_path, caption='🧾 Ваш файл готов')
    else:
        print(f"Файл не найден: {file_path}")


def schedule_task():
    run_date = datetime.strptime(SEND_AT, '%Y-%m-%d %H:%M:%S')
    scheduler.add_job(job, 'date', run_date=run_date)
    print(f"Задача запланирована на {SEND_AT}")


if __name__ == '__main__':
    schedule_task()
    app.run(debug=True, use_reloader=False)
