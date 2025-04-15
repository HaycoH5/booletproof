const express = require('express');
const multer = require('multer');
const qrcode = require('qrcode-terminal');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const axios = require('axios');

const app = express();
const PORT = 3000;

const storage = multer.memoryStorage();
const upload = multer({ storage });

app.use(express.json());

const client = new Client({
    authStrategy: new LocalAuth()
});

client.on('qr', qr => qrcode.generate(qr, { small: true }));
client.on('ready', () => console.log('WhatsApp клиент готов и авторизован'));
client.on('disconnected', reason => console.log('WhatsApp отключился:', reason));

// === 📥 Парсинг входящих сообщений ===
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

    // 🔁 Отправка в Python-сервер
    axios.post('http://localhost:5000/webhook', logEntry)
        .then(() => console.log(`Сообщение от ${phone} отправлено в Flask`))
        .catch((err) => console.error('Ошибка при отправке в Flask:', err.message));
});

// === 📤 Отправка файлов или сообщений ===
app.post('/send', upload.single('file'), async (req, res) => {
    console.log('📩 /send получен');
    const phone = req.body.phone;
    const caption = req.body.caption || '';
    const chatId = `${phone}@c.us`;

    if (!phone) {
        return res.status(400).json({ error: 'Не указан номер получателя' });
    }

    try {
        if (req.file) {
            const base64File = req.file.buffer.toString('base64');
            const media = new MessageMedia(req.file.mimetype, base64File, req.file.originalname);

            console.log('📎 Отправка файла:', req.file.originalname);
            await client.sendMessage(chatId, media, { caption });
            return res.json({ status: 'file sent' });
        } else if (req.body.message) {
            await client.sendMessage(chatId, req.body.message);
            return res.json({ status: 'message sent' });
        } else {
            return res.status(400).json({ error: 'Нет содержимого для отправки' });
        }
    } catch (err) {
        console.error('Ошибка при отправке:', err.message);
        return res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`JS-сервер слушает на http://localhost:${PORT}`);
});

client.initialize();
