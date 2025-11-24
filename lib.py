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

    # print("Создаваемые поля DBF:")
    # for i, field in enumerate(field_definitions, 1):
    #     print(f"  {i}. {field}")

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


def update_json_by_lc_list(input_file, output_file, lc_list, new_destruction_date):
    """
    Обновляет поле D_DESTR в JSON файле для указанных лицевых счетов

    Args:
        input_file: путь к исходному JSON файлу
        output_file: путь для сохранения обновленного файла
        lc_list: список лицевых счетов для обновления
        new_destruction_date: новая дата для поля D_DESTR
    """

    # Чтение исходного файла
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Обновление записей
    updated = []
    not_found = []

    for lc in lc_list:
        if lc in data:
            data[lc]["D_DESTR"] = new_destruction_date
            updated.append(lc)
        else:
            not_found.append(lc)

    # Сохранение обновленного файла
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # Отчет
    print(f"Обновлено записей: {len(updated)}")
    if updated:
        print(f"Обновленные ЛС: {', '.join(updated)}")
    if not_found:
        print(f"Не найдено: {len(not_found)}")
        print(f"Не найдены в JSON: {', '.join(not_found)}")


class ParserXlS:

    def __init__(self, filexls):
        if not os.path.exists(filexls):
            raise FileNotFoundError(f"Файл {filexls} не найден")
        self.filexls = filexls
        self.all_data = None
        self.parser()

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

    def get_list_lc(self):
        lst1 = self.all_data["Лист1"]
        l = []
        for x in lst1:
            ls = str(x[" Л/счет"])
            if len(ls) == 5 :
                ls = "0" + ls
            l.append(ls)
        return l