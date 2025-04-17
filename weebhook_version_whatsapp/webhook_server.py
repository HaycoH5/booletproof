# weebhook_version_whatsapp/webhook_server.py

from flask import Flask, request, jsonify
from datetime import datetime, timezone
from pathlib import Path
import os
import sys

# чтобы найти ваш пакет AgroLLM
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ROOT_DIR)

import config
from AgroLLM.message_router import MessageRouter
from user_and_system_interface.data_save import DataSave

app = Flask(__name__)

# точка входа в LLM- и в image‑процессоры
router = MessageRouter()

# хелпер для логирования текстовых сообщений и инициализации Excel
data_save = DataSave(config.BASE_DIR, config.EXEL_TABLE_BASE_NAME)

# **Создаём папку для текстовых логов**, чтобы save_to_txt не падал
text_log_dir = Path(config.BASE_DIR) / 'txt_messages'
text_log_dir.mkdir(parents=True, exist_ok=True)

# корневая папка для входящих агроданных
BASE_DIR = Path('agro_data')


@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    if not data or 'from' not in data:
        return jsonify({'error': 'Invalid data: missing "from" field'}), 400

    phone = data['from']
    date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # создаём папки agro_data/{phone}/{date}/media
    user_dir = BASE_DIR / phone / date_str
    media_dir = user_dir / 'media'
    media_dir.mkdir(parents=True, exist_ok=True)

    result = None

    # ── MEDIA ───────────────────────────────────────────────────────────
    if data.get('type') == 'media' and 'media_data' in data:
        ext = data.get('ext', 'bin')
        ts = int(datetime.now(timezone.utc).timestamp())
        media_filename = f"media_{ts}.{ext}"
        media_path = media_dir / media_filename

        try:
            with open(media_path, 'wb') as f:
                f.write(bytes.fromhex(data['media_data']))
        except ValueError:
            return jsonify({'error': 'Invalid media_data format'}), 400

        # чистим payload и сохраняем относительный путь
        data.pop('media_data', None)
        data['media_path'] = str(media_path.relative_to(user_dir))

        result = router.handle(
            msg_type="media",
            payload={
                "paths": [str(media_path)],
                "prompt": (
                    "На этой фотографии должна быть таблица. "
                    "Перепиши все столбики с их значениями. "
                    "Ответ — только JSON, только русский текст и цифры."
                ),
            },
            meta={
                "append_to_excel": data_save.append_message_to_table,
                "excel_path": config.EXEL_TABLE_DIR,
                "date": data.get("timestamp", "")
            }
        )

    # ── TEXT ────────────────────────────────────────────────────────────
    else:
        # логируем текстовое сообщение (папка уже создана выше)
        data_save.save_to_txt(data)

        result = router.handle(
            msg_type="text",
            payload={"content": data.get("content", "")},
            meta={
                "append_to_excel": data_save.append_message_to_table,
                "excel_path": config.EXEL_TABLE_DIR,
                "date": data.get("timestamp", "")
            }
        )

    return jsonify({'status': 'ok', 'result': result})


if __name__ == '__main__':
    app.run(debug=True)
