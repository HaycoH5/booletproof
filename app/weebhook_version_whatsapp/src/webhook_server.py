from flask import Flask, request, jsonify
from app.AgroLLM.src.process_messages import LLMProcess
from app.user_and_system_interface.src.data_save import DataSave
from app.AgroLLM.src.process_photos import ImageProcessor
from app import config
from datetime import datetime


# Инициализация компонентов
data_save = DataSave(config.BASE_DIR, config.EXEL_TABLE_BASE_NAME)
llm_process_text = LLMProcess()
llm_process_image = ImageProcessor()
now = datetime.now()


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

    # Создание директорий для пользователя и медиафайлов
    media_dir = config.BASE_DIR + "/" + config.MEDIA_DIR


    # Обработка медиафайлов

    if data.get('type') == 'media' and 'media_data' in data:
        ext = data.get('ext', 'bin')
        timestamp = int(datetime.utcnow().timestamp())
        media_filename = f'media_{timestamp}.{ext}'
        media_path = media_dir + "/" + media_filename

        try:
            with open(media_path, 'wb') as f:
                f.write(bytes.fromhex(data['media_data']))
        except ValueError:
            print(11111)
            return jsonify({'error': 'Invalid media_data format'}), 400
        print(222222222)
        data = llm_process_image.process_images(image_paths=[media_path],
                                                      prompt="На этой фотографии должна быть таблица. Перепиши...")

        results = llm_process_text.process_messages(data)

    else:
        # Сохраняем данные
        data_save.save_to_txt(data, config.BASE_DIR + "/" + config.TEXT_DIR)


        # Обработка сообщений через LLM
        results = llm_process_text.process_messages(
            data.get("content", "")
        )

    print(results)
    data_save.append_message_to_table(message_dict=results,
                                      filepath=(config.BASE_DIR + "/" + config.EXEL_TABLE_DIR),
                                      date_value=now.strftime("%M_%H_%d_%m_%Y"))

    return jsonify({'status': 'ok'})


