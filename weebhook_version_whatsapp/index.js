const { Client } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const axios = require('axios');

const client = new Client();

client.on('qr', (qr) => {
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    console.log('Клиент подключён!');
});

client.on('message', async (message) => {
    const contact = await message.getContact();
    const phone = contact.number || 'unknown';
    const now = new Date().toISOString();

    const logEntry = {
        from: phone,
        timestamp: now
    };

    if (message.type === 'chat') {
        logEntry.type = 'text';
        logEntry.content = message.body;
    }

    if (message.hasMedia) {
        const media = await message.downloadMedia();
        if (media) {
            const ext = media.mimetype.split('/')[1].split(';')[0];
            logEntry.type = 'media';
            logEntry.media_type = media.mimetype;
            logEntry.ext = ext;
            logEntry.media_data = Buffer.from(media.data, 'base64').toString('hex');
        }
    }

    // Отправляем в Python webhook
    axios.post('http://127.0.0.1:5000/webhook', logEntry)
        .then(() => console.log(`📤 Сообщение от ${phone} отправлено`))
        .catch((err) => console.error('Ошибка при отправке:', err.message));
});

client.initialize();
