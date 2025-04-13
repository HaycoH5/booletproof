import pandas as pd
import openai
from openai import OpenAI
import os
from typing import List, Dict
import json
import getpass
import random
import logging
from datetime import datetime


TEST_SIZE = 5
BATCH_SIZE = 1
# Получение пути к директории скрипта
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
LOGS_DIR = os.path.join(SCRIPT_DIR, "logs")

# Создание директорий, если они не существуют
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, f'processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

model_name = 'deepseek/deepseek-chat-0324-alt-structured'

def get_api_key():
    """Get API key from environment or prompt user"""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        api_key = getpass.getpass("Введи ваш VseGPT ключ API:")
        os.environ["OPENAI_API_KEY"] = api_key
    return api_key

def get_sheet_names(file_path: str) -> List[str]:
    """Get sheet names from Excel file"""
    try:
        xl = pd.ExcelFile(file_path, engine='openpyxl')
        return xl.sheet_names
    except Exception as e:
        logger.error(f"Error reading sheet names from {file_path}: {e}")
        return []

def format_table_for_display(df: pd.DataFrame) -> str:
    """Format DataFrame as a readable table string"""
    if df is None or df.empty:
        return "No data available"
    
    # Convert DataFrame to string with aligned columns
    return df.to_string(index=False)

def read_excel_files():
    """Read all necessary Excel files with proper sheet handling"""
    try:
        # Get sheet names first
        messages_sheets = get_sheet_names(os.path.join(DATA_DIR, "messages.xlsx"))
        examples_sheets = get_sheet_names(os.path.join(DATA_DIR, "processed_examples.xlsx"))
        extra_info_sheets = get_sheet_names(os.path.join(DATA_DIR, "extra_information.xlsx"))
        
        logger.info("Available sheets:")
        logger.info(f"Messages: {messages_sheets}")
        logger.info(f"Examples: {examples_sheets}")
        logger.info(f"Extra info: {extra_info_sheets}")
        
        # Read messages
        messages_df = pd.read_excel(
            os.path.join(DATA_DIR, "messages.xlsx"),
            sheet_name=messages_sheets[0],
            engine='openpyxl'
        )
        
        # Read examples
        examples_df = pd.read_excel(
            os.path.join(DATA_DIR, "processed_examples.xlsx"),
            sheet_name=examples_sheets[0],
            engine='openpyxl'
        )
        
        # Read extra information - read all sheets
        extra_info_data = {}
        for sheet_name in extra_info_sheets:
            extra_info_data[sheet_name] = pd.read_excel(
                os.path.join(DATA_DIR, "extra_information.xlsx"),
                sheet_name=sheet_name,
                engine='openpyxl'
            )
        
        # Clean up dataframes - remove empty rows and columns
        messages_df.dropna(how='all', inplace=True)
        messages_df.dropna(axis=1, how='all', inplace=True)
        
        examples_df.dropna(how='all', inplace=True)
        examples_df.dropna(axis=1, how='all', inplace=True)
        
        # Clean each sheet in extra_info
        for sheet_name in extra_info_data:
            extra_info_data[sheet_name].dropna(how='all', inplace=True)
            extra_info_data[sheet_name].dropna(axis=1, how='all', inplace=True)
        
        return messages_df, examples_df, extra_info_data, extra_info_sheets
    
    except Exception as e:
        logger.error(f"Error reading Excel files: {e}")
        raise

def create_system_prompt(examples_df: pd.DataFrame, extra_info_data: Dict[str, pd.DataFrame], sheet_names: List[str]) -> str:
    """Create a system prompt using examples and extra information"""
    # Format reference information
    extra_info_text = ""
    for sheet_name in sheet_names:
        if sheet_name in extra_info_data:
            extra_info_text += f"\n=== {sheet_name} ===\n"
            extra_info_text += format_table_for_display(extra_info_data[sheet_name])
            extra_info_text += "\n"
    
    system_prompt = f"""You are an expert in parsing agricultural messages. Your task is to extract structured information from messages.

Reference information for parsing:
{extra_info_text}

IMPORTANT: When filling the "Подразделение" field, use the table "Принадлежность отделений и ПУ" from the reference information above to determine the correct department name. For example, if you see "Отд 11", look up which "ПУ" it belongs to in the reference table and use that as the "Подразделение" value.

Example 1 - Single operation message:
```
2-е диск сах св под пш
По Пу 25/775
Отд 12 25/475
```

Should be parsed into one row:
| Дата | Подразделение | Операция | Культура | За день, га | С начала операции, га | Вал за день, ц | Вал с начала, ц | Исходное сообщение |
|------|---------------|-----------|-----------|-------------|---------------------|----------------|-----------------|-------------------|
|      | ПУ "Север"   | Дискование 2-е | Пшеница | 25 | 475 |  |  | 2-е диск сах св под пш\nПо Пу 25/775\nОтд 12 25/475 |

Example 2 - Multiple operations in one message:
```
10.03 день
2-я подкормка озимых, ПУ "Юг" - 1749/2559
(в т.ч Амазон-1082/1371
Пневмоход-667/1188)

Отд11- 307/307 (амазон 307/307) 
Отд 12- 671/671( амазон 318/318; пневмоход 353/353) 
Отд 16- 462/1272( амазон 148/437; пневмоход 314/835) 
Отд 17- 309/309( амазон 309/309)
```

Should be parsed into multiple rows:
| Дата | Подразделение | Операция | Культура | За день, га | С начала операции, га | Вал за день, ц | Вал с начала, ц | Исходное сообщение |
|------|---------------|-----------|-----------|-------------|---------------------|----------------|-----------------|-------------------|
| 10.03 | ПУ "Юг" | 2-я подкормка | Озимые | 307 | 307 | | | 10.03 день\n2-я подкормка озимых, ПУ "Юг" - 1749/2559\nОтд11- 307/307 (амазон 307/307) |
| 10.03 | ПУ "Юг" | 2-я подкормка | Озимые | 671 | 671 | | | 10.03 день\n2-я подкормка озимых, ПУ "Юг" - 1749/2559\nОтд 12- 671/671( амазон 318/318; пневмоход 353/353) |
| 10.03 | ПУ "Юг" | 2-я подкормка | Озимые | 462 | 1272 | | | 10.03 день\n2-я подкормка озимых, ПУ "Юг" - 1749/2559\nОтд 16- 462/1272( амазон 148/437; пневмоход 314/835) |
| 10.03 | ПУ "Юг" | 2-я подкормка | Озимые | 309 | 309 | | | 10.03 день\n2-я подкормка озимых, ПУ "Юг" - 1749/2559\nОтд 17- 309/309( амазон 309/309) |

Example 3 - Multiple different operations with different crops:
```
Север
Отд7 пах с св 41/501
Отд20 20/281 по пу 61/793
Отд 3 пах подс 60/231
По пу 231

Диск к. Сил отд 7. 32/352
Пу- 484
Диск под Оз п взубор 20/281
Диск под с. Св отд 10 83/203 пу-1065га
Выравн под кук силос
ПоПу 25/932
Отд 11 25/462
Предп культ под сах св
ПоПу 452/1636
Отд 11 78/252
Отд 12 143/610
Отд 16 121/469 5га вымочки
Отд 17 110/305
Сев сах св
ПоПу 403/982
Отд 11 73/170
Отд 12 139/269
Отд 16 65/296
Отд 17 126/247
```

Should be parsed into multiple rows:
| Дата | Подразделение | Операция | Культура | За день, га | С начала операции, га | Вал за день, ц | Вал с начала, ц | Исходное сообщение |
|------|---------------|-----------|-----------|-------------|---------------------|----------------|-----------------|-------------------|
| | ПУ "Север" | Пахота | Сахарная свекла | 41 | 501 | | | Север\nОтд7 пах с св 41/501 |
| | ПУ "Север" | Пахота | | 20 | 281 | | | Север\nОтд20 20/281 по пу 61/793 |
| | ПУ "Север" | Пахота | Подсолнечник | 60 | 231 | | | Север\nОтд 3 пах подс 60/231 |
| | ПУ "Север" | Дискование | Силос | 32 | 352 | | | Север\nДиск к. Сил отд 7. 32/352\nПу- 484 |
| | ПУ "Север" | Дискование | Озимая пшеница | 20 | 281 | | | Север\nДиск под Оз п взубор 20/281 |
| | ПУ "Север" | Дискование | Сахарная свекла | 83 | 203 | | | Север\nДиск под с. Св отд 10 83/203 пу-1065га |
| | ПУ "Север" | Выравнивание | Кукуруза на силос | 25 | 932 | | | Север\nВыравн под кук силос\nПоПу 25/932\nОтд 11 25/462 |
| | ПУ "Север" | Предпосевная культивация | Сахарная свекла | 78 | 252 | | | Север\nПредп культ под сах св\nПоПу 452/1636\nОтд 11 78/252 |
| | ПУ "Север" | Предпосевная культивация | Сахарная свекла | 143 | 610 | | | Север\nПредп культ под сах св\nПоПу 452/1636\nОтд 12 143/610 |
| | ПУ "Север" | Предпосевная культивация | Сахарная свекла | 121 | 469 | | | Север\nПредп культ под сах св\nПоПу 452/1636\nОтд 16 121/469 5га вымочки |
| | ПУ "Север" | Предпосевная культивация | Сахарная свекла | 110 | 305 | | | Север\nПредп культ под сах св\nПоПу 452/1636\nОтд 17 110/305 |
| | ПУ "Север" | Сев | Сахарная свекла | 73 | 170 | | | Север\nСев сах св\nПоПу 403/982\nОтд 11 73/170 |
| | ПУ "Север" | Сев | Сахарная свекла | 139 | 269 | | | Север\nСев сах св\nПоПу 403/982\nОтд 12 139/269 |
| | ПУ "Север" | Сев | Сахарная свекла | 65 | 296 | | | Север\nСев сах св\nПоПу 403/982\nОтд 16 65/296 |
| | ПУ "Север" | Сев | Сахарная свекла | 126 | 247 | | | Север\nСев сах св\nПоПу 403/982\nОтд 17 126/247 |

I will provide you with messages. Each message might contain one or multiple operations.
Please analyze each message and extract the following information for each operation:
1. Дата (Date) - if specified for the whole message, use it for all operations
2. Подразделение (Department/Unit) - Use the "Принадлежность отделений и ПУ" table to determine the correct ПУ name for each department
3. Операция (Operation type) - the main agricultural operation being performed
4. Культура (Crop) - if specified once for multiple operations, use it for all related operations
5. За день, га (Area per day, hectares) - first number in pairs like "41/501"
6. С начала операции, га (Total area since operation start, hectares) - second number in pairs like "41/501"
7. Вал за день, ц (Yield per day, centners)
8. Вал с начала, ц (Total yield since operation start, centners)
9. Исходное сообщение (Original message) - include ALL relevant parts of the message that describe this operation, including shared information like date, operation type, or crop type

Format your response as a JSON array, where each element represents one operation:
[
    {{
        "Дата": "",
        "Подразделение": "ПУ \\"Север\\"",
        "Операция": "Сев",
        "Культура": "Сахарная свекла",
        "За день, га": "73",
        "С начала операции, га": "170",
        "Вал за день, ц": "",
        "Вал с начала, ц": "",
        "Исходное сообщение": "Север\\nСев сах св\\nПоПу 403/982\\nОтд 11 73/170"
    }},
    // more operations if present in the message
]

Important notes:
1. If you can't extract some values, leave them empty (empty string). Never return null or "N/A".
2. For each operation, include ALL relevant context in the "Исходное сообщение" field, even if it's shared between multiple operations.
3. Make sure to properly escape newlines (\\n) and quotes (\") in the "Исходное сообщение" field.
4. Pay attention to abbreviated crop names:
   - "св" or "с. св" = "Сахарная свекла"
   - "подс" = "Подсолнечник"
   - "оз п" = "Озимая пшеница"
   - "кук" = "Кукуруза"
5. Common operation abbreviations:
   - "пах" = "Пахота"
   - "диск" = "Дискование"
   - "культ" = "Культивация"
   - "предп культ" = "Предпосевная культивация"
   - "сев" = "Сев"
6. When an operation has a total for "ПоПу" (or "По пу") and then individual department values, create separate entries for each department.
7. ALWAYS use the "Принадлежность отделений и ПУ" table to determine the correct ПУ name for each department number."""

    return system_prompt

def process_messages_batch(messages: List[str], system_prompt: str) -> List[Dict]:
    """Process a batch of messages using OpenAI API"""
    # Combine messages into a single text with clear separators
    batch_text = "\n\n=== NEXT MESSAGE ===\n\n".join(messages)
    
    messages_for_api = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": batch_text}
    ]
    
    try:
        completion = client.chat.completions.create(
            model=model_name,
            messages=messages_for_api,
            temperature=0.1,
            extra_headers={"X-Title": "Agro Message Parser"}
        )
        
        response = completion.choices[0].message.content
        logger.info("\nAPI Response:", response)  # Debug print
        
        try:
            # Parse the JSON response
            parsed_results = json.loads(response)
            
            # Ensure we got a list of results
            if not isinstance(parsed_results, list):
                parsed_results = [parsed_results]
            
            # Clean up results - replace None and "N/A" with empty strings
            for result in parsed_results:
                for key in result:
                    if result[key] is None or result[key] == "N/A":
                        result[key] = ""
                
            return parsed_results
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            logger.error("Raw response:", response)
            # Try to extract any valid JSON from the response
            try:
                # Find anything that looks like JSON array
                import re
                json_match = re.search(r'\[(.*)\]', response.replace('\n', ''))
                if json_match:
                    fixed_json = json_match.group(0)
                    return json.loads(fixed_json)
            except:
                pass
            
            # If all fails, return empty result
            return [{
                "Дата": "",
                "Подразделение": "",
                "Операция": "",
                "Культура": "",
                "За день, га": "",
                "С начала операции, га": "",
                "Вал за день, ц": "",
                "Вал с начала, ц": "",
                "Исходное сообщение": messages[0] if messages else ""
            }]
        
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        return [{
            "Дата": "",
            "Подразделение": "",
            "Операция": "",
            "Культура": "",
            "За день, га": "",
            "С начала операции, га": "",
            "Вал за день, ц": "",
            "Вал с начала, ц": "",
            "Исходное сообщение": messages[0] if messages else ""
        }]

def main():
    # Get API key and initialize client
    api_key = get_api_key()
    global client
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.vsegpt.ru/v1",
    )
    
    # Read all necessary files
    messages_df, examples_df, extra_info_data, extra_info_sheets = read_excel_files()
    
    # Create system prompt
    system_prompt = create_system_prompt(examples_df, extra_info_data, extra_info_sheets)
    
    # Print the system prompt for verification
    logger.info("System prompt:")
    logger.info("="*80)
    logger.info(system_prompt)
    logger.info("="*80)
    
    # Get messages from the "Данные для тренировки" column
    all_messages = messages_df['Данные для тренировки'].dropna().tolist()
    
    # Select 10 random messages for testing
    test_size = TEST_SIZE
    test_messages = random.sample(all_messages, min(test_size, len(all_messages)))
    
    logger.info(f"Total messages available: {len(all_messages)}")
    logger.info(f"Selected {len(test_messages)} random messages for testing")
    
    # Print test messages for verification
    logger.info("Test messages:")
    for i, msg in enumerate(test_messages, 1):
        logger.info(f"\n{i}. {'='*40}")
        logger.info(msg)
    
    # Process messages in larger batches since we're using deepseek model
    batch_size = BATCH_SIZE
    all_results = []
    
    # Process test messages in batches
    for i in range(0, len(test_messages), batch_size):
        batch = test_messages[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} of {(len(test_messages) + batch_size - 1)//batch_size}...")
        batch_results = process_messages_batch(batch, system_prompt)
        all_results.extend(batch_results)
        
        logger.info(f"Found {len(batch_results)} operations in this batch")
    
    # Create results DataFrame with specified columns
    columns = [
        "Дата",
        "Подразделение",
        "Операция",
        "Культура",
        "За день, га",
        "С начала операции, га",
        "Вал за день, ц",
        "Вал с начала, ц",
        "Исходное сообщение"
    ]
    
    results_df = pd.DataFrame(all_results, columns=columns)
    
    # Save results
    output_file = os.path.join(LOGS_DIR, f"result_table_{model_name.replace('/', '_')}_test.xlsx")
    results_df.to_excel(output_file, index=False)
    logger.info(f"Processing complete! Found {len(all_results)} operations")
    logger.info(f"Results saved to {output_file}")
    
    # Display first few results
    logger.info("First few results:")
    logger.info(results_df.head().to_string())

if __name__ == "__main__":
    main() 