# 🌾 AgroLLM WhatsApp Webhook

Flask-приложение для приёма сообщений через WhatsApp вебхук, обработки текстов и изображений с помощью LLM и OCR, и сохранения данных в структуру каталогов и Excel-файлы.

## 📦 Структура проекта

```
app/
├── AgroLLM/
│   ├── data/
│   │   └── instruction.json
│   └── src/
│       ├── process_messages.py
│       └── process_photos.py
├── user_and_system_interface/
│   └── src/
│       └── data_save.py
├── weebhook_version_whatsapp/
│   ├── src/
│   │   └── webhook_server.py
│   └── index.js
├── config.py
```

## 🚀 Быстрый старт

1. **Первый запуск (инициализация проекта и директорий):**
   Запустите скрипт:
   ```bash
   init.bat
   ```
   Во время первого запуска вам потребуется отсканировать QR-код с помощью WhatsApp на том аккаунте, который вы хотите использовать с этим приложением.

2. **Обычный запуск после инициализации:**
   ```bash
   setup.bat
   ```

3. **(Опционально) Установка зависимостей вручную:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Убедитесь, что настроен `config.py`:**

   Пример `config.py`:
   ```python
   BASE_DIR = "agro_data"
   TEXT_DIR = "texts"
   MEDIA_DIR = "media"
   EXEL_TABLE_DIR = "excels"
   EXEL_TABLE_BASE_NAME = "messages.xlsx"
   ```

## 🛠 Возможности для расширения

- Поддержка других форматов медиа.
- Расширенный диалог с пользователями через NLP.
- Интеграция с базой данных.