const { Client } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const axios = require('axios');

const client = new Client();

// Генерация QR-кода для авторизации
client.on('qr', (qr) => {
    qrcode.generate(qr, { small: true });
});

// Подтверждение успешного подключения клиента
client.on('ready', () => {
    console.log('Клиент подключён!');
});

// Обработка входящих сообщений
client.on('message', async (message) => {
    try {
        const contact = await message.getContact();
        const phone = contact.number || 'unknown';
        const now = new Date().toISOString();

        const logEntry = {
            from: phone,
            timestamp: now
        };

        // Текстовое сообщение
        if (message.type === 'chat') {
            logEntry.type = 'text';
            logEntry.content = message.body;
        }

        // Медиа-сообщение
        if (message.hasMedia) {
            const media = await message.downloadMedia();

            if (media) {
                const ext = media.mimetype.split('/')[1]?.split(';')[0] || '
