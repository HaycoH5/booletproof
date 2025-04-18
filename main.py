from app.weebhook_version_whatsapp.src import webhook_server
from app import config
import os

webhook_server.data_save.create_structure([
    config.BASE_DIR,
    os.path.join(config.BASE_DIR, config.TEXT_DIR),
    os.path.join(config.BASE_DIR, config.EXEL_TABLE_DIR),
])

if __name__ == '__main__':
    webhook_server.app.run(debug=True)