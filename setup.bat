@echo off


echo Запуск main.py...
python main.py
cd /d "%~dp0app\weebhook_version_whatsapp"
node node index.js

pause