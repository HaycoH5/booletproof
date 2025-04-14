import pandas as pd
import openai
from openai import OpenAI
import os
from typing import List, Dict, Optional
import json
import getpass
import random
import logging
from datetime import datetime

import LLM_config


class LLMProcess:
    """обработка сырых данных LLM моделью"""

    def __init__(self):
        """init"""

        self.model_name = LLM_config.model_name

        self.TEST_SIZE = LLM_config.TEST_SIZE
        self.BATCH_SIZE = LLM_config.BATCH_SIZE

        # Получение пути к директории скрипта
        self.SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.DATA_DIR = os.path.join(self.SCRIPT_DIR, "data")
        self.LOGS_DIR = os.path.join(self.SCRIPT_DIR, "logs")

    def get_api_key(self):
        """Get API key from environment or prompt user"""

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            api_key = getpass.getpass("Введи ваш VseGPT ключ API:")
            os.environ["OPENAI_API_KEY"] = api_key
        return api_key

    def read_instruction_file(self):
        """Read instruction data from JSON file"""

        try:
            instruction_path = os.path.join(DATA_DIR, "instruction.json")
            with open(instruction_path, 'r', encoding='utf-8') as f:
                instruction_data = json.load(f)
            return instruction_data
        except Exception as e:
            logger.error(f"Error reading instruction file: {e}")
            raise
