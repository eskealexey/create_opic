from lib import dbf_to_json, smart_json_merge, json_to_dbf_corrected, ParserXlS


def main():
    dbf_to_json('F25SP.DBF', 'json_file')
    # xls = ParserXls('opis.xls')
    # xls.parser("json", "opis.json")
    # Пример использования
    pars = ParserXlS("opis.xls")
    data = pars.parser("json", "opis.json")
    for d in data:
        print(d.name)

if __name__ == "__main__":
    main()
