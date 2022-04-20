# import teradatasql
#
# with teradatasql.connect() as con:
#     with con.cursor() as cur:
#         cur.execute(f'''
#             SELECT
#                 MAX(CDR_DATE),
#                 COUNT(*) as ROW_COUNT
#             FROM UAT_PRODUCT.MN_IN_OUT
#             WHERE DIRECTION = 'Outgoing'
#                 AND CDR_MONTH = '2021-06-01'
#         ''')
#         aaa, bbb = cur.fetchall()[0]
# print(aaa, bbb)

