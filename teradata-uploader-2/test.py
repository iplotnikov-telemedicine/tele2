import streamlit as st
print((st.selectbox.__dict__))






# import teradatasql
# from configparser import ConfigParser
#
# tables_str = '''
# TEST5347689
# TEST_1509
# TEST
# TEST_1243254
# TEST_SUNDAY_444
# TEST_SUNDAY_4
# TEST_MONDAY_54
# TEST_MONDAY_53
# TEST_MONDAY_100
# TEST_MONDAY_52
# TEST_MONDAY_51
# TEST_MONDAY_50
# TEST_MONDAY_42
# TEST_MONDAY_41
# TEST_MONDAY_40
# TEST_MONDAY_36
# TEST_MONDAY_35
# TEST_MONDAY_33
# TEST_MONDAY_30
# TEST_MONDAY_20
# TEST_MONDAY_15
# TEST_SUNDAY_11
# TEST_SUNDAY_10
# TEST_SUNDAY_9
# TEST_SUNDAY_8
# TEST_SUNDAY_7
# TEST_SUNDAY_6
# TEST_SUNDAY_5
# '''
#
# tables_list = tables_str.split('\n')[1:-1]
#
# serverconfig = ConfigParser()
# config_file = 'config.ini'
# serverconfig.read('config.ini')
# params = dict(serverconfig["SERVERCONFIG"])
#
#
# with teradatasql.connect(**params, user='igor.i.plotnikov', password='Anna032021') as con:
#     for table in tables_list:
#         with con.cursor() as cur:
#             cur.execute(f'DROP TABLE UAT_PRODUCT.{table}')
#             print(f'{table} dropped.')




#
# df['col2'] = round(df['col'], 0)
# e = df['col2'].equals(df['col'])
# print(e)
#
#
#
# dtypes = {col:None for col in df.columns}
# print(dtypes)
# from transliterate import translit
#
# eng_name = translit('ewrthij', "ru", reversed=True)
#
# print(eng_name)



#
#
# def get_reserved_words(rw_filename):
#     with open(rw_filename) as f:
#         content = f.readlines()
#         return [line.strip() for line in content]
#
# reserved_words = get_reserved_words('reserved_words.txt')
# print(reserved_words)