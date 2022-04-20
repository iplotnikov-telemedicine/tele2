# import pandas as pd
# from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
# from tableauhyperapi import escape_string_literal
# from tableau_api_lib import TableauServerConnection
# from tableau_api_lib.utils.querying import get_projects_dataframe
un = 'igor.i.plotnikov'
pw = 'Anna062020'
#
#
# tableau_server_config = {
#         'my_env': {
#                 'server': 'http://10.12.77.238',
#                 'api_version': '3.8',
#                 'username': 'igor.i.plotnikov',
#                 'password': pw,
#                 'site_name': 'Tableau Server',
#                 'site_url': 'http://t2ru-tableau-01/#/home',
#                 # 'site_url': 'http://10.12.77.238/api/3.8/serverinfo',
#         }
# }
#
# connection = TableauServerConnection(tableau_server_config, env='my_env')
# connection.sign_in()
#
# print(connection.query_sites().json())
#
# connection.sign_out()



import tableauserverclient as TSC

tableau_auth = TSC.TableauAuth(un, pw, 'http://t2ru-tableau-01/')
server = TSC.Server('http://10.12.77.238')

with server.auth.sign_in(tableau_auth):
    all_datasources, pagination_item = server.datasources.get()
    print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
    print([datasource.name for datasource in all_datasources])