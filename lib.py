import os
from datetime import date
import json
import pandas as pd

import dbf
from datetime import datetime
from dbfread import DBF


#------- код отвечающий за создание json из dbf
def dbf_to_json(dbf_file_path, json_file_path):
    # Читаем DBF файл
    table = DBF(dbf_file_path, encoding='cp866')  # или другая кодировка

    result = {}

    for record in table:
        # Первый столбец становится ключом
        first_column = list(record.keys())[0]
        key = record[first_column]

        # Обрабатываем все поля записи
        processed_record = {}
        for field_name, value in record.items():
            # Преобразуем даты в формат "dd.mm.yyyy"
            if isinstance(value, (datetime, date)):
                processed_record[field_name] = value.strftime("%d.%m.%Y")
            else:
                processed_record[field_name] = value

        result[str(key)] = processed_record  # Преобразуем ключ в строку

    # Сохраняем в JSON
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return result
# -----------------------------------------------------------------------------------------------------------------


# ------- код отвечающий за объединение json файлов--------------------------
def smart_json_merge(file1, file2, output_file):
    """
    Умное объединение с проверкой структуры и обработкой ошибок
    """
    try:
        # Читаем файлы
        with open(file1, 'r', encoding='utf-8') as f:
            data1 = json.load(f)

        with open(file2, 'r', encoding='utf-8') as f:
            data2 = json.load(f)

        # Проверяем, что это словари (как в вашем случае)
        if not isinstance(data1, dict) or not isinstance(data2, dict):
            raise ValueError("Оба файла должны содержать JSON объекты (словари)")

        # Проверяем структуру первых записей (если есть)
        if data1 and data2:
            sample_key1 = next(iter(data1))
            sample_key2 = next(iter(data2))

            if isinstance(data1[sample_key1], dict) and isinstance(data2[sample_key2], dict):
                keys1 = set(data1[sample_key1].keys())
                keys2 = set(data2[sample_key2].keys())

                if keys1 != keys2:
                    print(f"Предупреждение: структуры записей различаются")
                    print(f"Общие поля: {keys1 & keys2}")
                    print(f"Уникальные в file1: {keys1 - keys2}")
                    print(f"Уникальные в file2: {keys2 - keys1}")

        # Объединяем данные
        merged_data = {}

        # Сначала добавляем все из первого файла
        merged_data.update(data1)

        # Затем добавляем/перезаписываем данными из второго файла
        merged_data.update(data2)

        # Сохраняем результат
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)

        # Статистика
        common_keys = set(data1.keys()) & set(data2.keys())
        unique_in_file1 = set(data1.keys()) - set(data2.keys())
        unique_in_file2 = set(data2.keys()) - set(data1.keys())

        print("=" * 50)
        print("ОТЧЕТ ОБ ОБЪЕДИНЕНИИ")
        print("=" * 50)
        print(f"Файл 1 ({file1}): {len(data1)} записей")
        print(f"Файл 2 ({file2}): {len(data2)} записей")
        print(f"Результат: {len(merged_data)} записей")
        print(f"Общие ключи: {len(common_keys)}")
        print(f"Уникальные в file1: {len(unique_in_file1)}")
        print(f"Уникальные в file2: {len(unique_in_file2)}")
        print(f"Файл сохранен: {output_file}")

        return merged_data

    except Exception as e:
        print(f"Ошибка при объединении: {e}")
        return None
# -----------------------------------------------------------------------------------------------------------------


# ------- код отвечающий за создание dbf из json
def parse_field_definitions(field_defs_str):
    """
    Парсит строку с определением полей DBF
    """
    field_definitions = []

    fields = [f.strip() for f in field_defs_str.split(',') if f.strip()]

    for field in fields:
        if ':' in field:
            parts = [p.strip() for p in field.split(':')]
        else:
            parts = [p.strip() for p in field.split()]

        if len(parts) < 2:
            continue

        field_name = parts[0]
        field_type = parts[1].upper()

        if field_type == 'C':
            length = int(parts[2]) if len(parts) >= 3 else 254
            field_def = f"{field_name} C({length})"

        elif field_type in ('N', 'F'):
            if len(parts) >= 4:
                length = int(parts[2])
                decimals = int(parts[3])
            elif len(parts) >= 3:
                length = int(parts[2])
                decimals = 2
            else:
                length = 10
                decimals = 2
            field_def = f"{field_name} N({length},{decimals})"

        elif field_type == 'D':
            field_def = f"{field_name} D"

        elif field_type == 'L':
            field_def = f"{field_name} L"

        else:
            field_def = f"{field_name} C(100)"

        field_definitions.append(field_def)

    return field_definitions


def clean_value(value, field_type):
    """Очищает значение для DBF"""
    if value is None:
        if field_type == 'D':
            return None
        elif field_type in ('N', 'F'):
            return 0.0
        elif field_type == 'L':
            return False
        else:
            return ''

    if field_type == 'D':
        if isinstance(value, str):
            try:
                return datetime.strptime(value.strip(), "%d.%m.%Y").date()
            except:
                return None
        return None

    elif field_type in ('N', 'F'):
        try:
            if isinstance(value, str):
                value = value.replace(',', '.')
            return float(value)
        except:
            return 0.0

    elif field_type == 'L':
        if isinstance(value, str):
            return value.strip().upper() in ('TRUE', 'T', 'YES', 'Y', '1', 'ON')
        return bool(value)

    else:  # Character
        return str(value) if value is not None else ''


def json_to_dbf_corrected(json_file_path, dbf_file_path, field_defs_str):
    """Конвертирует JSON в DBF с правильными методами"""

    # Парсим определение полей
    field_definitions = parse_field_definitions(field_defs_str)
    field_spec = ";".join(field_definitions)

    print("Создаваемые поля DBF:")
    for i, field in enumerate(field_definitions, 1):
        print(f"  {i}. {field}")

    # Создаем таблицу
    table = dbf.Table(dbf_file_path, field_spec, codepage='cp866')
    table.open(mode=dbf.READ_WRITE)

    # Читаем JSON
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    successful = 0

    for key, record in data.items():
        try:
            # Создаем запись с помощью tuple
            record_data = []

            for field_def in field_definitions:
                field_name = field_def.split()[0]
                field_type_char = field_def.split()[1][0]  # Первый символ типа

                value = record.get(field_name, '')
                cleaned_value = clean_value(value, field_type_char)
                record_data.append(cleaned_value)

            # Добавляем запись как кортеж
            table.append(tuple(record_data))
            successful += 1

        except Exception as e:
            print(f"Ошибка в записи {key}: {e}")
            continue

    table.close()
    print(f"Успешно создано записей: {successful}")



class ParserXlS:

    def __init__(self, filexls):
        if not os.path.exists(filexls):
            raise FileNotFoundError(f"Файл {filexls} не найден")
        self.filexls = filexls
        self.all_data = {}

    def __str__(self):
        return self.filexls

    def parser(self, output_dir="JSON", output_file="all_sheets.json", force_reload=False):
        """
        Используем встроенные возможности pandas для преобразования в JSON
        """
        if self.all_data is not None and not force_reload:
            return self.all_data

        os.makedirs(output_dir, exist_ok=True)

        try:
            excel_file = pd.ExcelFile(self.filexls)
            sheet_names = excel_file.sheet_names

            all_data = {}

            for sheet_name in sheet_names:
                df = pd.read_excel(self.filexls, sheet_name=sheet_name)

                # Используем to_json с последующим преобразованием обратно в dict
                json_str = df.to_json(orient='records', date_format='iso', force_ascii=False)
                all_data[sheet_name] = json.loads(json_str)

            json_path = os.path.join(output_dir, output_file)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)

            self.all_data = all_data
            return self.all_data

        except Exception as e:
            raise RuntimeError(f"Ошибка при парсинге файла {self.filexls}: {e}")