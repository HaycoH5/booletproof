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


TEST_SIZE = 10
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

def read_instruction_file():
    """Read instruction data from JSON file"""
    try:
        instruction_path = os.path.join(DATA_DIR, "instruction.json")
        with open(instruction_path, 'r', encoding='utf-8') as f:
            instruction_data = json.load(f)
        return instruction_data
    except Exception as e:
        logger.error(f"Error reading instruction file: {e}")
        raise

def create_system_prompt(instruction_data: Dict) -> str:
    """Create a system prompt using instruction data"""
    # Extract examples from instruction data
    examples = instruction_data.get("примеры_обработки", [])
    
    # Format reference information
    extra_info_text = ""
    
    # Add department/unit mappings
    if "справочники" in instruction_data and "принадлежность_подразделений" in instruction_data["справочники"]:
        extra_info_text += "\n=== Принадлежность отделений и ПУ ===\n"
        dept_data = instruction_data["справочники"]["принадлежность_подразделений"]
        dept_df = pd.DataFrame(dept_data)
        extra_info_text += dept_df.to_string(index=False)
        extra_info_text += "\n"
    
    # Add operations list
    if "справочники" in instruction_data and "операции" in instruction_data["справочники"]:
        extra_info_text += "\n=== Операции ===\n"
        operations_data = instruction_data["справочники"]["операции"]
        operations_df = pd.DataFrame(operations_data)
        extra_info_text += operations_df.to_string(index=False)
        extra_info_text += "\n"
    
    # Add crops list
    if "справочники" in instruction_data and "культуры" in instruction_data["справочники"]:
        extra_info_text += "\n=== Культуры ===\n"
        crops = instruction_data["справочники"]["культуры"]
        extra_info_text += "\n".join([f"- {crop}" for crop in crops])
        extra_info_text += "\n"
    
    # Format examples
    examples_text = ""
    for i, example in enumerate(examples[:3], 1):  # Use first 3 examples
        message = example.get("Сообщение", "")
        data = example.get("Данные", [])
        
        examples_text += f"\nExample {i}:\n```\n{message}\n```\n\n"
        
        if data:
            examples_text += "Should be parsed into:\n"
            for item in data:
                examples_text += f"- {item}\n"
            examples_text += "\n"
    
    system_prompt = f"""You are an expert in parsing agricultural messages. Your task is to extract structured information from messages.

Reference information for parsing:
{extra_info_text}

IMPORTANT: When filling the "Подразделение" field, use the table "Принадлежность отделений и ПУ" from the reference information above to determine the correct department name. For example, if you see "Отд 11", look up which "ПУ" it belongs to in the reference table and use that as the "Подразделение" value.

{examples_text}

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

def process_messages_batch(messages: List[str], system_prompt: str, client: OpenAI) -> List[Dict]:
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

def process_agro_messages(messages: List[str], batch_size: int = 1, save_to_excel: bool = False, output_path: Optional[str] = None) -> pd.DataFrame:
    """
    Process agricultural messages and return a DataFrame with structured data.
    
    Args:
        messages: List of messages to process
        batch_size: Number of messages to process in each batch
        save_to_excel: Whether to save results to Excel
        output_path: Path to save Excel file (if save_to_excel is True)
        
    Returns:
        DataFrame with processed results
    """
    # Get API key and initialize client
    api_key = get_api_key()
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.vsegpt.ru/v1",
    )
    
    # Read instruction data
    instruction_data = read_instruction_file()
    
    # Create system prompt
    system_prompt = create_system_prompt(instruction_data)
    
    # Print the system prompt for verification
    logger.info("System prompt:")
    logger.info("="*80)
    logger.info(system_prompt)
    logger.info("="*80)
    
    logger.info(f"Total messages to process: {len(messages)}")
    
    # Process messages in batches
    all_results = []
    
    # Process messages in batches
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} of {(len(messages) + batch_size - 1)//batch_size}...")
        batch_results = process_messages_batch(batch, system_prompt, client)
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
    
    # Save results if requested
    if save_to_excel:
        if output_path is None:
            output_path = os.path.join(LOGS_DIR, f"result_table_{model_name.replace('/', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
        results_df.to_excel(output_path, index=False)
        logger.info(f"Results saved to {output_path}")
    
    logger.info(f"Processing complete! Found {len(all_results)} operations")
    
    return results_df

def main():
    """Main function for testing the script"""
    # Read messages from Excel file
    try:
        messages_df = pd.read_excel(
            os.path.join(DATA_DIR, "messages.xlsx"),
            engine='openpyxl'
        )
        
        # Get messages from the "Данные для тренировки" column
        all_messages = messages_df['Данные для тренировки'].dropna().tolist()
        
        # Select random messages for testing
        test_size = 5  # TEST_SIZE
        test_messages = random.sample(all_messages, min(test_size, len(all_messages)))
        
        # Process messages
        results_df = process_agro_messages(
            messages=test_messages,
            batch_size=1,  # BATCH_SIZE
            save_to_excel=True
        )
        
        # Display first few results
        logger.info("First few results:")
        logger.info(results_df.head().to_string())
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main() 