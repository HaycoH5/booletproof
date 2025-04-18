@echo off
cd /d "%~dp0app\weebhook_version_whatsapp"

echo Инициализация проекта Node.js...
npm init -y

echo Установка зависимостей...
npm install whatsapp-web.js qrcode-terminal axios express multer

echo Установка завершена!
pause
