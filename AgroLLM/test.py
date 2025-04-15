#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Тестирование точности обработки сообщений LLM

Этот скрипт предназначен для тестирования точности обработки сообщений агрономов с помощью LLM.
Мы будем использовать часть примеров из инструкции для обучения модели, а остальные примеры - для тестирования точности.
"""

import json
import random
import pandas as pd
import numpy as np
import os
import sys
from typing import List, Dict, Tuple, Any
from datetime import datetime
import io
import contextlib

# Добавляем родительскую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем класс LLMProcess из process_messages.py
from process_messages import LLMProcess
from openai import OpenAI


def load_instruction_data(file_path: str = None) -> Dict:
    """Загружает данные из файла инструкций.
    
    Args:
        file_path: Путь к файлу с инструкциями
        
    Returns:
        Dict: Данные из файла инструкций
    """
    # Если путь не указан, используем путь относительно текущего файла
    if file_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, 'data', 'instruction.json')
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Обрабатываем значения в данных
    if 'примеры_обработки' in data:
        for example in data['примеры_обработки']:
            if 'Данные' in example:
                for row in example['Данные']:
                    for key in row:
                        # Заменяем None на пустую строку
                        if row[key] is None:
                            row[key] = ""
                        # Преобразуем числовые значения в строки
                        elif isinstance(row[key], (int, float)):
                            row[key] = str(row[key])
    
    return data


def split_examples(instruction_data: Dict, train_size: int = 5, random_seed: int = 42) -> Tuple[List[Dict], List[Dict]]:
    """Разделяет примеры на обучающие и тестовые.
    
    Args:
        instruction_data: Данные из файла инструкций
        train_size: Количество примеров для обучения
        random_seed: Seed для воспроизводимости результатов
        
    Returns:
        Tuple[List[Dict], List[Dict]]: Обучающие и тестовые примеры
    """
    examples = instruction_data['примеры_обработки']
    
    # Устанавливаем seed для воспроизводимости
    random.seed(random_seed)
    
    # Выбираем случайные примеры для обучения
    train_indices = random.sample(range(len(examples)), train_size)
    
    # Разделяем примеры
    train_examples = [examples[i] for i in train_indices]
    test_examples = [examples[i] for i in range(len(examples)) if i not in train_indices]
    
    return train_examples, test_examples


def create_custom_instruction(train_examples: List[Dict], instruction_data: Dict) -> str:
    """Создает инструкцию для LLM на основе обучающих примеров.
    
    Args:
        train_examples: Обучающие примеры
        instruction_data: Данные из файла инструкций
        
    Returns:
        str: Инструкция для LLM
    """
    # Создаем экземпляр класса LLMProcess
    llm_processor = LLMProcess()
    
    # Создаем копию данных инструкции
    custom_instruction_data = instruction_data.copy()
    
    # Заменяем примеры обработки на обучающие примеры
    custom_instruction_data['примеры_обработки'] = train_examples
    
    # Создаем системный промпт
    system_prompt = llm_processor.create_system_prompt(custom_instruction_data)
    
    return system_prompt


def process_test_messages(test_examples: List[Dict], system_prompt: str) -> List[Dict]:
    """Обрабатывает тестовые сообщения с помощью LLM.
    
    Args:
        test_examples: Тестовые примеры
        system_prompt: Инструкция для LLM
        
    Returns:
        List[Dict]: Результаты обработки тестовых сообщений
    """
    # Создаем экземпляр класса LLMProcess
    llm_processor = LLMProcess()
    
    # Получаем API ключ
    api_key = llm_processor.api_key
    
    # Инициализируем клиент OpenAI
    client = OpenAI(
        api_key=api_key,
        base_url="https://api.vsegpt.ru/v1",
    )
    
    # Извлекаем сообщения из тестовых примеров
    test_messages = [example['Сообщение'] for example in test_examples]
    
    # Обрабатываем сообщения
    results = []
    for i, message in enumerate(test_messages):
        print(f"Обработка сообщения {i+1}/{len(test_messages)}...")
        
        # Обрабатываем сообщение
        batch_results = llm_processor.process_messages_batch([message], system_prompt, client)
        
        # Добавляем результаты
        results.append({
            'message': message,
            'expected': test_examples[i]['Данные'],
            'actual': batch_results
        })
    
    return results


def normalize_numeric_value(value):
    """Нормализует числовые значения для корректного сравнения.
    
    Args:
        value: Значение для нормализации
        
    Returns:
        str: Нормализованное значение
    """
    if value is None or value == "":
        return ""
    
    # Преобразуем в строку, если это не строка
    if not isinstance(value, str):
        value = str(value)
    
    # Заменяем запятые на точки для десятичных чисел
    value = value.replace(',', '.')
    
    # Удаляем лишние пробелы
    value = value.strip()
    
    # Для дат игнорируем год
    if ":" in value and "-" in value:  # Похоже на дату
        try:
            # Извлекаем только месяц и день
            parts = value.split(" ")[0].split("-")
            if len(parts) >= 2:
                return f"{parts[1]}-{parts[2]}"  # Возвращаем только месяц-день
        except:
            pass
    
    # Проверяем, является ли значение числом
    try:
        # Пробуем преобразовать в float
        float_value = float(value)
        
        # Проверяем, не является ли значение слишком большим (возможно, пропущена десятичная точка)
        if float_value > 1000000:  # Если значение больше миллиона
            # Проверяем, не является ли это значением с пропущенной десятичной точкой
            # Например, 1259680 может быть 12596.80
            if '.' not in value and len(value) > 6:
                # Добавляем десятичную точку перед последними двумя цифрами
                value = value[:-2] + '.' + value[-2:]
                # Преобразуем обратно в float и форматируем
                float_value = float(value)
        
        # Форматируем число с двумя знаками после запятой
        return f"{float_value:.2f}"
    except ValueError:
        # Если не удалось преобразовать в число, возвращаем исходное значение
        return value


def calculate_accuracy(test_results: List[Dict]) -> Dict[str, float]:
    """Рассчитывает точность обработки сообщений.
    
    Args:
        test_results: Результаты обработки тестовых сообщений
        
    Returns:
        Dict[str, float]: Метрики точности
    """
    # Инициализируем счетчики
    total_cells = 0
    correct_cells = 0
    total_operations = 0
    correct_operations = 0
    
    # Список колонок для проверки
    columns = [
        "Дата",
        "Подразделение",
        "Операция",
        "Культура",
        "За день, га",
        "С начала операции, га",
        "Вал за день, ц",
        "Вал с начала, ц"
    ]
    
    # Счетчики для каждой колонки
    column_correct = {column: 0 for column in columns}
    column_total = {column: 0 for column in columns}
    
    # Обрабатываем каждое сообщение
    for result in test_results:
        expected = result['expected']
        actual = result['actual']
        
        # Если строк больше, чем ожидалось, точность = 0
        if len(actual) > len(expected):
            continue
        
        # Считаем правильные ячейки для каждой строки
        message_correct = 0
        message_total = 0
        
        # Обрабатываем каждую строку
        for i in range(len(expected)):
            # Если строка отсутствует в результатах, считаем все ячейки неправильными
            if i >= len(actual):
                message_total += len(columns)
                continue
            
            # Считаем правильные ячейки
            for column in columns:
                message_total += 1
                column_total[column] += 1
                
                # Проверяем, совпадают ли значения
                expected_value = expected[i].get(column)
                actual_value = actual[i].get(column)
                
                # Нормализуем числовые значения
                expected_value = normalize_numeric_value(expected_value)
                actual_value = normalize_numeric_value(actual_value)
                
                # Если оба значения None или пустые строки, считаем правильным
                if (expected_value is None or expected_value == "") and (actual_value is None or actual_value == ""):
                    message_correct += 1
                    column_correct[column] += 1
                # Если значения совпадают, считаем правильным
                elif expected_value == actual_value:
                    message_correct += 1
                    column_correct[column] += 1
        
        # Добавляем к общим счетчикам
        total_cells += message_total
        correct_cells += message_correct
        
        # Если все ячейки в сообщении правильные, считаем операцию правильной
        if message_total > 0 and message_correct == message_total:
            correct_operations += 1
        
        # Увеличиваем счетчик операций
        total_operations += 1
    
    # Рассчитываем метрики
    accuracy = correct_cells / total_cells if total_cells > 0 else 0
    operation_accuracy = correct_operations / total_operations if total_operations > 0 else 0
    
    # Рассчитываем точность для каждой колонки
    column_accuracy = {column: column_correct[column] / column_total[column] if column_total[column] > 0 else 0 for column in columns}
    
    return {
        'overall_accuracy': accuracy,
        'operation_accuracy': operation_accuracy,
        'column_accuracy': column_accuracy
    }


def analyze_errors(test_results: List[Dict]) -> pd.DataFrame:
    """Анализирует ошибки в обработке сообщений.
    
    Args:
        test_results: Результаты обработки тестовых сообщений
        
    Returns:
        pd.DataFrame: Таблица с ошибками
    """
    # Список колонок для проверки
    columns = [
        "Дата",
        "Подразделение",
        "Операция",
        "Культура",
        "За день, га",
        "С начала операции, га",
        "Вал за день, ц",
        "Вал с начала, ц"
    ]
    
    # Создаем список для хранения ошибок
    errors = []
    
    # Обрабатываем каждое сообщение
    for i, result in enumerate(test_results):
        message = result['message']
        expected = result['expected']
        actual = result['actual']
        
        # Если строк больше, чем ожидалось, добавляем ошибку
        if len(actual) > len(expected):
            errors.append({
                'message_index': i,
                'message': message,
                'error_type': 'extra_rows',
                'expected_rows': len(expected),
                'actual_rows': len(actual),
                'column': 'all',
                'expected_value': None,
                'actual_value': None
            })
            continue
        
        # Обрабатываем каждую строку
        for j in range(len(expected)):
            # Если строка отсутствует в результатах, добавляем ошибку
            if j >= len(actual):
                for column in columns:
                    errors.append({
                        'message_index': i,
                        'message': message,
                        'error_type': 'missing_row',
                        'row_index': j,
                        'column': column,
                        'expected_value': expected[j].get(column),
                        'actual_value': None
                    })
                continue
            
            # Проверяем каждую колонку
            for column in columns:
                expected_value = expected[j].get(column)
                actual_value = actual[j].get(column)
                
                # Преобразуем числовые значения в строки
                if isinstance(expected_value, (int, float)):
                    expected_value = str(expected_value)
                if isinstance(actual_value, (int, float)):
                    actual_value = str(actual_value)
                
                # Проверяем, совпадают ли значения
                values_match = False
                
                # Специальная обработка для None, пустых значений и "N/A"
                if ((expected_value is None or expected_value == "" or expected_value == "N/A") and 
                    (actual_value is None or actual_value == "" or actual_value == "N/A")):
                    values_match = True
                # Для числовых колонок используем нормализацию
                elif column in ["За день, га", "С начала операции, га", "Вал за день, ц", "Вал с начала, ц"]:
                    normalized_expected = normalize_numeric_value(expected_value)
                    normalized_actual = normalize_numeric_value(actual_value)
                    if normalized_expected == normalized_actual:
                        values_match = True
                # Для остальных колонок сравниваем напрямую
                elif expected_value == actual_value:
                    values_match = True
                
                # Если значения не совпадают после всех проверок, добавляем ошибку
                if not values_match:
                    errors.append({
                        'message_index': i,
                        'message': message,
                        'error_type': 'value_mismatch',
                        'row_index': j,
                        'column': column,
                        'expected_value': expected_value,
                        'actual_value': actual_value
                    })
    
    # Создаем DataFrame с ошибками
    errors_df = pd.DataFrame(errors)
    
    return errors_df


def print_error_analysis(test_results: List[Dict], errors_df: pd.DataFrame):
    """Выводит анализ ошибок в консоль в удобном формате.
    
    Args:
        test_results: Результаты обработки тестовых сообщений
        errors_df: DataFrame с ошибками
    """
    if errors_df.empty:
        print("Ошибок не обнаружено!")
        return
    
    # Группируем ошибки по сообщениям
    message_indices = errors_df['message_index'].unique()
    
    print(f"\nОбнаружено {len(errors_df)} ошибок в {len(message_indices)} сообщениях.")
    
    # Обрабатываем каждое сообщение с ошибками
    for message_index in message_indices:
        message_errors = errors_df[errors_df['message_index'] == message_index]
        message = test_results[message_index]['message']
        
        print(f"\n{'='*80}")
        print(f"Сообщение #{message_index+1}:")
        print(f"{'-'*80}")
        print(f"{message}")
        print(f"{'-'*80}")
        
        # Проверяем наличие лишних строк
        extra_rows = message_errors[message_errors['error_type'] == 'extra_rows']
        if not extra_rows.empty:
            print(f"ЛИШНИЕ СТРОКИ: {extra_rows.iloc[0]['actual_rows'] - extra_rows.iloc[0]['expected_rows']} строк больше, чем ожидалось")
            print(f"Ожидалось: {extra_rows.iloc[0]['expected_rows']}, Получено: {extra_rows.iloc[0]['actual_rows']}")
        
        # Проверяем наличие отсутствующих строк
        missing_rows = message_errors[message_errors['error_type'] == 'missing_row']
        if not missing_rows.empty:
            # Группируем по индексу строки
            missing_row_indices = missing_rows['row_index'].unique()
            for row_index in missing_row_indices:
                row_errors = missing_rows[missing_rows['row_index'] == row_index]
                print(f"ОТСУТСТВУЮЩАЯ СТРОКА #{row_index+1}:")
                # Выводим все колонки для этой строки
                for _, error in row_errors.iterrows():
                    print(f"  {error['column']}: {error['expected_value']}")
        
        # Проверяем несовпадения значений
        mismatches = message_errors[message_errors['error_type'] == 'value_mismatch']
        if not mismatches.empty:
            print("НЕСОВПАДЕНИЯ ЗНАЧЕНИЙ:")
            # Группируем по индексу строки
            mismatch_row_indices = mismatches['row_index'].unique()
            for row_index in mismatch_row_indices:
                row_mismatches = mismatches[mismatches['row_index'] == row_index]
                print(f"  Строка #{row_index+1}:")
                for _, error in row_mismatches.iterrows():
                    print(f"    {error['column']}: Ожидалось '{error['expected_value']}', Получено '{error['actual_value']}'")
    
    print(f"\n{'='*80}")
    print("СВОДКА ПО ОШИБКАМ:")
    print(f"{'-'*80}")
    
    # Выводим статистику по типам ошибок
    error_types = errors_df['error_type'].value_counts()
    print("Типы ошибок:")
    for error_type, count in error_types.items():
        print(f"  {error_type}: {count}")
    
    # Выводим статистику по колонкам
    column_errors = errors_df['column'].value_counts()
    print("\nОшибки по колонкам:")
    for column, count in column_errors.items():
        print(f"  {column}: {count}")


def save_results_to_excel(test_results: List[Dict], accuracy_metrics: Dict[str, float], errors_df: pd.DataFrame, output_path: str = None) -> str:
    """Сохраняет результаты тестирования в Excel-файл.
    
    Args:
        test_results: Результаты обработки тестовых сообщений
        accuracy_metrics: Метрики точности
        errors_df: DataFrame с ошибками
        output_path: Путь для сохранения файла
        
    Returns:
        str: Путь к сохраненному файлу
    """
    # Создаем директорию для результатов тестирования, если она не существует
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_results_dir = os.path.join(current_dir, 'test_results')
    os.makedirs(test_results_dir, exist_ok=True)
    
    # Если путь не указан, создаем его в директории результатов тестирования
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(test_results_dir, f'test_results_{timestamp}.xlsx')
    
    # Создаем Excel-файл с несколькими листами
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Создаем DataFrame для результатов
        results_data = []
        
        # Список колонок для проверки
        columns = [
            "Дата",
            "Подразделение",
            "Операция",
            "Культура",
            "За день, га",
            "С начала операции, га",
            "Вал за день, ц",
            "Вал с начала, ц"
        ]
        
        # Обрабатываем каждое сообщение
        for i, result in enumerate(test_results):
            message = result['message']
            expected = result['expected']
            actual = result['actual']
            
            # Определяем максимальное количество строк
            max_rows = max(len(expected), len(actual))
            
            # Обрабатываем каждую строку
            for j in range(max_rows):
                row_data = {
                    'Сообщение': message if j == 0 else '',  # Показываем сообщение только в первой строке
                    'Номер сообщения': i + 1,
                    'Номер строки': j + 1
                }
                
                # Добавляем ожидаемые и фактические значения рядом для каждой колонки
                for column in columns:
                    # Ожидаемое значение
                    if j < len(expected):
                        value = expected[j].get(column, '')
                        # Нормализуем числовые значения
                        row_data[f'Ожидаемое {column}'] = normalize_numeric_value(value)
                    else:
                        row_data[f'Ожидаемое {column}'] = ''
                    
                    # Фактическое значение (сразу после ожидаемого)
                    if j < len(actual):
                        value = actual[j].get(column, '')
                        # Нормализуем числовые значения
                        row_data[f'Фактическое {column}'] = normalize_numeric_value(value)
                    else:
                        row_data[f'Фактическое {column}'] = ''
                    
                    # Добавляем флаг совпадения для этой колонки
                    if j < len(expected) and j < len(actual):
                        expected_value = normalize_numeric_value(expected[j].get(column))
                        actual_value = normalize_numeric_value(actual[j].get(column))
                        if (expected_value is not None and expected_value != "") or (actual_value is not None and actual_value != ""):
                            row_data[f'Совпадение {column}'] = 'Да' if expected_value == actual_value else 'Нет'
                        else:
                            row_data[f'Совпадение {column}'] = 'Да'  # Если оба пустые, считаем совпадающими
                    else:
                        row_data[f'Совпадение {column}'] = 'Нет'
                
                # Добавляем общий флаг совпадения
                if j < len(expected) and j < len(actual):
                    match = True
                    for column in columns:
                        expected_value = normalize_numeric_value(expected[j].get(column))
                        actual_value = normalize_numeric_value(actual[j].get(column))
                        if (expected_value is not None and expected_value != "") or (actual_value is not None and actual_value != ""):
                            if expected_value != actual_value:
                                match = False
                                break
                    row_data['Совпадение'] = 'Да' if match else 'Нет'
                else:
                    row_data['Совпадение'] = 'Нет'
                
                results_data.append(row_data)
        
        # Создаем DataFrame для результатов
        results_df = pd.DataFrame(results_data)
        
        # Переупорядочиваем колонки для удобства просмотра
        column_order = ['Сообщение', 'Номер сообщения', 'Номер строки']
        
        # Добавляем колонки в порядке: ожидаемое, фактическое, совпадение для каждой метрики
        for column in columns:
            column_order.extend([
                f'Ожидаемое {column}',
                f'Фактическое {column}',
                f'Совпадение {column}'
            ])
        
        # Добавляем общее совпадение в конец
        column_order.append('Совпадение')
        
        # Переупорядочиваем колонки
        results_df = results_df[column_order]
        
        # Сохраняем результаты на лист "Результаты"
        results_df.to_excel(writer, sheet_name='Результаты', index=False)
        
        # Создаем DataFrame для метрик точности
        accuracy_data = {
            'Метрика': ['Общая точность', 'Точность по операциям'] + [f'Точность по колонке {column}' for column in columns],
            'Значение': [
                accuracy_metrics['overall_accuracy'],
                accuracy_metrics['operation_accuracy']
            ] + [accuracy_metrics['column_accuracy'][column] for column in columns]
        }
        accuracy_df = pd.DataFrame(accuracy_data)
        
        # Сохраняем метрики на лист "Метрики"
        accuracy_df.to_excel(writer, sheet_name='Метрики', index=False)
        
        # Сохраняем ошибки на лист "Ошибки"
        errors_df.to_excel(writer, sheet_name='Ошибки', index=False)
    
    print(f"Результаты сохранены в файл: {output_path}")
    return output_path


def save_console_output_to_file(console_output: str, output_path: str = None) -> str:
    """Сохраняет вывод консоли в текстовый файл.
    
    Args:
        console_output: Текст, выведенный в консоль
        output_path: Путь для сохранения файла
        
    Returns:
        str: Путь к сохраненному файлу
    """
    # Создаем директорию для результатов тестирования, если она не существует
    current_dir = os.path.dirname(os.path.abspath(__file__))
    test_results_dir = os.path.join(current_dir, 'test_results')
    os.makedirs(test_results_dir, exist_ok=True)
    
    # Если путь не указан, создаем его в директории результатов тестирования
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(test_results_dir, f'console_output_{timestamp}.txt')
    
    # Сохраняем вывод консоли в файл
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(console_output)
    
    print(f"Вывод консоли сохранен в файл: {output_path}")
    return output_path


def main():
    """Основная функция для запуска тестирования."""
    # Создаем буфер для захвата вывода консоли
    console_output_buffer = io.StringIO()
    
    # Захватываем вывод консоли
    with contextlib.redirect_stdout(console_output_buffer):
        print("Загрузка данных из файла инструкций...")
        instruction_data = load_instruction_data()
        print(f"Загружено {len(instruction_data['примеры_обработки'])} примеров обработки сообщений.")
        
        print("\nРазделение примеров на обучающие и тестовые...")
        train_examples, test_examples = split_examples(instruction_data)
        print(f"Обучающих примеров: {len(train_examples)}")
        print(f"Тестовых примеров: {len(test_examples)}")
        
        print("\nСоздание инструкции для LLM...")
        system_prompt = create_custom_instruction(train_examples, instruction_data)
        print("Инструкция для LLM создана.")
        
        print("\nОбработка тестовых сообщений...")
        test_results = process_test_messages(test_examples, system_prompt)
        print(f"Обработано {len(test_results)} тестовых сообщений.")
        
        print("\nРасчет точности...")
        accuracy_metrics = calculate_accuracy(test_results)
        
        # Выводим результаты
        print(f"Общая точность: {accuracy_metrics['overall_accuracy']:.2%}")
        print(f"Точность по операциям: {accuracy_metrics['operation_accuracy']:.2%}")
        print("\nТочность по колонкам:")
        for column, accuracy in accuracy_metrics['column_accuracy'].items():
            print(f"  {column}: {accuracy:.2%}")
        
        print("\nАнализ ошибок...")
        errors_df = analyze_errors(test_results)
        
        # Выводим анализ ошибок в удобном формате
        print_error_analysis(test_results, errors_df)
        
        print("\nСохранение результатов в Excel...")
        excel_path = save_results_to_excel(test_results, accuracy_metrics, errors_df)
        
        print("\nТестирование завершено.")
    
    # Получаем вывод консоли
    console_output = console_output_buffer.getvalue()
    
    # Выводим в консоль
    print(console_output)
    
    # Сохраняем вывод консоли в файл
    save_console_output_to_file(console_output)


if __name__ == "__main__":
    main() 