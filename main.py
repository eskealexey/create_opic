from lib import dbf_to_json, json_to_dbf_corrected, ParserXlS, update_json_by_lc_list


FIELD_DEFS = (
    "LC:C:6,FM:C:23,IM:C:21,OT:C:21,REM:C:10,GOD:C:4,"
    "N:C:2,KOD_OTKR:C:4,DAT_OTKR:D,KOD_ZAKR:C:11,DAT_ZAKR:D,"
    "DATR:D,VPEN:C:3,SNAZN:N:10:2,D_YXOD:D,D_DESTR:D,"
    "VPN:C:3,CART:C:2,DNASN:D"
)
# new_destruction_date="15.10.2025"


def main():
    dbf_to_json(file_dbf_in, 'json_file.json')
    xls = ParserXlS(file_xls_in)
    lc_list = xls.get_list_lc()  # ваш список

    update_json_by_lc_list(
        input_file='json_file.json',
        output_file='updated_file.json',
        lc_list=lc_list,
        new_destruction_date=new_destruction_date
    )
    json_to_dbf_corrected("updated_file.json", "output.dbf", FIELD_DEFS)


if __name__ == "__main__":
    file_dbf_in = str(input(" Введите входной файл dbf:"))
    if not file_dbf_in:
        file_dbf_in = 'F25SP.DBF'

    file_xls_in = str(input(" Введите файл xls:"))
    if not file_xls_in:
        file_xls_in = 'opis.xls'

    new_destruction_date = str(input(" Введите новую дату уничтожения (dd.mm.yyyy):"))

    main()
