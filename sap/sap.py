# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import teradatasql
import numpy as np
import os
import time
import msvcrt
import sys
import traceback


# %% [markdown]
# - Открываем файл.
# - Импортируем данные.
# - Обрабатываем данные.
# - Делаем бэкап UAT_PRODUCT.PRODUCT_PARAMETERS в UAT_PRODUCT.PRODUCT_PARAMETERS_BACKUP.
# - Грузим обработанные данные в UAT_PRODUCT.PRODUCT_PARAMETERS_TEST_FOR_SAP.
# - Запросом транформируем данные в подневные из UAT_PRODUCT.PRODUCT_PARAMETERS_TEST_FOR_SAP в UAT_PRODUCT.PRODUCT_PARAMETERS.


def secure_password_input(prompt=''):
    p_s = ''
    proxy_string = [' '] * 64
    while True:
        sys.stdout.write('\x0D' + prompt + ''.join(proxy_string))
        c = msvcrt.getch()
        if c == b'\r':
            break
        elif c == b'\x08':
            p_s = p_s[:-1]
            proxy_string[len(p_s)] = " "
        else:
            proxy_string[len(p_s)] = "*"
            p_s += c.decode()

    sys.stdout.write('\n')
    return p_s




HOST = 'td2800.corp.tele2.ru'
USR = input('Введите имя пользователя Teradata: ').lower()
PWD = secure_password_input(prompt='Введите пароль Teradata: ')
LOGMECH = 'LDAP'


# %%
class SAP():
    
    def __init__(self, path=r'P:\\CP_PLM\\Reporting\\Report_Data\\Revenue & Subs\\'):
        self.path = path
        self.filename = None
        self.condition = str()
        self.get_filename()
        self.raw = pd.DataFrame()
        
 
    def get_filename(self):
        files = []
        for file in os.listdir(self.path):
            fname = self.path + file
            modified_dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(fname)))
            files.append((file, modified_dt))
        sorted_files = sorted(files, key=lambda x: x[1])  
        for i, (file, modified_dt) in enumerate(sorted_files):
            print(i, '\t', modified_dt, '\t', file) 
        while not self.filename:
            file_index = int(input('\nКакой номер нужен?\n'))
            for i, (file, modified_dt) in enumerate(sorted_files):
                if i == file_index:
                    print(f'Выбран {file}')
                    self.shortname = file
                    self.filename = self.path + self.shortname
                    break
                
            
    def import_file(self, filename = None):
        '''
        как результат - получаем self.raw
        '''
        
        
        if not filename: filename = self.filename
        dtype_dict = {
            'version': str, 'month': str, 'year': str, 'region': str, 'base_type': str, 
            'tariff': str, 'account': str, 'param_value': float
        }
        
        header_dict = {
            'data' : ['version','month','year','region','account','param_value'],
            'Data' : ['version','month','year','region','account','param_value'],
            'phd' : ['version','month','year','region','base_type','tariff','account','param_value'],
            'rev' : ['version','month','year','region','base_type','tariff','account','param_value']
        }
        
        for sh_name in header_dict:
            # sh_df = pd.read_excel(filename, sheet_name = sh_name, engine='pyxlsb',
            #         names = header_dict[sh_name],
            #         usecols = header_dict[sh_name],
            #         dtype = dtype_dict, skiprows = 0)
            # sh_df = sh_df.dropna()
            # self.raw = pd.concat([self.raw,sh_df])

            try:
                sh_df = pd.read_excel(filename, sheet_name = sh_name, engine='pyxlsb',
                    names = header_dict[sh_name],
                    usecols = header_dict[sh_name],
                    dtype = dtype_dict, skiprows = 0)
            except ValueError:
                pass
            else:
                sh_df = sh_df.dropna()
                self.raw = pd.concat([self.raw,sh_df])
        print('\nСэмпл данных:')
        print(self.raw.head())
        
        
        
    def apply_condition(self, param_dict = None):
        '''
        применяем условия к датафрейму и как результат - получаем self.df для инсерта
        '''
        if not param_dict: param_dict = dict()
            
        if param_dict.get('min_date', None):
            min_date_dt = datetime.strptime(param_dict['min_date'], '%Y-%m-%d').date()
        else:
            min_date_dt = self.full_df.report_month.min()
        min_date_str = min_date_dt.strftime('%Y-%m-%d')
            
            
        if param_dict.get('max_date', None):
            max_date_dt = datetime.strptime(param_dict['max_date'], '%Y-%m-%d').date()
        else:
            max_date_dt = self.full_df.report_month.max()
        max_date_dt = (max_date_dt + relativedelta(months=1)).replace(day=1) - relativedelta(days=1)
        max_date_str = max_date_dt.strftime('%Y-%m-%d')
        
        
        
        if param_dict.get('param_1', None):
            param_1 = param_dict['param_1']
        else:
            param_1 = [param.strip() for param in self.full_df.version.unique()]
            
        if param_dict.get('param_2', None):
            param_2 = param_dict['param_2']
        else:
            param_2 = [param.strip() for param in self.full_df.account.unique()]
            
            
        self.df = self.full_df[
                (self.full_df.report_month >= min_date_dt)
                & (self.full_df.report_month <= max_date_dt)
                & (self.full_df.version.isin(param_1))
                & (self.full_df.account.isin(param_2))
        ]
        cols = ['report_month', 'version', 'account','NULL', 'NULL1', 'BRANCH_ID', 'base_type',
                'NULL2', 'NULL3', 'tariff', 'NULL4', 'param_value']
        self.df = self.df.reindex(columns = cols,  fill_value = np.NaN).where((pd.notnull(self.df)), None)
        
        
        '''
        формулируем те же условия в SQL, чтобы затирать при необходимости эти же данные в терадате
        '''
        self.condition = f'''WHERE REPORT_DATE BETWEEN DATE'{min_date_str}' AND DATE'{max_date_str}'
        AND PARAM_1 in ({','.join("'{0}'".format(p) for p in param_1)}) 
        AND PARAM_2 in ({','.join("'{0}'".format(p) for p in param_2)})
        '''

#         self.uat_pp_df = self.get_uat_pp_df()
        print('\nУсловие для обновления данных в хранилище:')
        print(self.condition)
        
    
    def get_sap_codes(self):
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            query = '''
                    sel min(branch_id) as BRANCH_ID, sap_code, sap_name_ru
                    FROM PRD2_DIC_V.BRANCH
                    where SAP_CODE is not null and b2c_flag = 1
                    group by 2,3
                    '''
            return pd.read_sql(query, session)
                
    
    def process_df(self):
        '''
        как результат - получаем self.full_df
        '''
        
        self.full_df = self.raw.copy()
        self.full_df['report_month'] = self.full_df.apply(lambda row: date(int(row.year), int(row.month), 1), axis = 1)
        self.full_df.param_value = self.full_df.param_value.apply(lambda x: round(x,4))
        
        if 'tariff' in self.full_df:
            tariff_dict = {'1100' : 'Bundle', '2000' : 'PAYG'}
            self.full_df.tariff = self.full_df.tariff.replace(tariff_dict)
            self.full_df.tariff = self.full_df.tariff.replace('#','NO SUBS')
        
        if 'base_type' in self.full_df:
            self.full_df.base_type = self.full_df.base_type.replace('#','NOT A')
        
        self.full_df.version = self.full_df.version.apply(lambda x: 'BU' if 'BU' in x else x.strip())
        self.sap_codes_df = self.get_sap_codes()
        self.full_df = pd.merge(self.full_df, self.sap_codes_df, how='inner', left_on='region', right_on='SAP_CODE')       
        
    
    def make_uat_pp_backup(self):
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            with session.cursor() as cur:
                print('Очищаю бэкап таблицу...')
                cur.execute('''
                    DELETE FROM UAT_PRODUCT.PRODUCT_PARAMETERS_BACKUP
                ''' + self.condition)
                print(f'{cur.rowcount} rows deleted from UAT_PRODUCT.PRODUCT_PARAMETERS_BACKUP\n')
                
                print('Обогащаю бэкап таблицу...')
                cur.execute(''' 
                    INSERT INTO UAT_PRODUCT.PRODUCT_PARAMETERS_BACKUP
                    SELECT * FROM UAT_PRODUCT.PRODUCT_PARAMETERS 
                '''  + self.condition)
        print(f'{cur.rowcount} rows inserted into UAT_PRODUCT.PRODUCT_PARAMETERS_BACKUP from UAT_PRODUCT.PRODUCT_PARAMETERS\n')
        
    
    def get_uat_pp_df(self):
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            query = 'sel * from UAT_PRODUCT.PRODUCT_PARAMETERS ' + self.condition
            return pd.read_sql(query, session)
            
            
    def get_df_diff(self, df1, df2, which='right_only'):
        comparison_df = df1.merge(df2,
                                  indicator=True,
                                  how='outer')
        if which is None:
            diff_df = comparison_df[comparison_df['_merge'] != 'both']
        else:
            diff_df = comparison_df[comparison_df['_merge'] == which]
        return diff_df
    
    
    def update_uat_pp_test(self, batchsize = 100000):
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            with session.cursor() as cur:
                print('Очищаю вспомогательную таблицу...')
                cur.execute('''delete from UAT_PRODUCT.PRODUCT_PARAMETERS_TEST_FOR_SAP
                ''')
                print(f'{cur.rowcount} rows deleted from UAT_PRODUCT.PRODUCT_PARAMETERS_TEST_FOR_SAP\n')

                print('Заполняю вспомогательную таблицу...')
                for num in range(0, len(self.df), batchsize):
                    cur.executemany(f'''
                     INSERT into UAT_PRODUCT.PRODUCT_PARAMETERS_TEST_FOR_SAP ({','.join('?'*len(self.df.columns))})
                    ''',
                        [tuple(row) for row in self.df.iloc[num:num+batchsize,:].itertuples(index=False)]
                        )
                print(f'{len(self.df)} rows inserted into UAT_PRODUCT.PRODUCT_PARAMETERS_TEST_FOR_SAP\n')
                
                
    def delete_from_uat_pp(self):
        print('Очищаю по условию PRODUCT_PARAMETERS...')
        query = 'DELETE from UAT_PRODUCT.PRODUCT_PARAMETERS ' + self.condition      
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            with session.cursor() as cur:
                cur.execute(query)      
                print(f'{cur.rowcount} rows deleted from UAT_PRODUCT.PRODUCT_PARAMETERS\n')
                
                
    def insert_into_uat_pp(self, replace=True):
        self.update_uat_pp_test()
        self.make_uat_pp_backup()
        if replace: self.delete_from_uat_pp()

        query = '''
            insert into UAT_PRODUCT.PRODUCT_PARAMETERS
            sel
                cal.calendar_date as REPORT_DATE, test.PARAM_1, test.PARAM_2, test.PARAM_3,
                test.PARAM_4, test.BRANCH_ID, test.BASE_TYPE, test.TP_ID_1, test.TP_ID_2,
                test.TARIFF_1, test.TARIFF_2, PARAM_VALUE/EXTRACT(DAY FROM LAST_DAY(REPORT_DATE)) as PARAM_VALUE
             from uat_product.product_parameters_test_for_sap test
             left join Sys_Calendar.BusinessCalendar cal
                on test.REPORT_DATE = trunc(cal.calendar_date,'mon')
             where test.PARAM_1 <> 'AC' AND test.PARAM_2 <> 'GMC  (w/o group )'
             union all
             sel *
             from uat_product.product_parameters_test_for_sap test
             where test.PARAM_1 = 'AC' or (test.PARAM_1 <> 'AC' AND test.PARAM_2 = 'GMC  (w/o group )')
        '''
                
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            with session.cursor() as cur:
                print('Обогащаю UAT_PRODUCT.PRODUCT_PARAMETERS...')
                cur.execute(query)      
                print(f'{cur.rowcount} rows inserted into UAT_PRODUCT.PRODUCT_PARAMETERS\n')


    def delete_expired_forecast(self):
        query = '''SELECT PARAM_1 FROM UAT_PRODUCT.PRODUCT_PARAMETERS WHERE PARAM_1 LIKE 'F%' GROUP BY PARAM_1
        '''
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            with session.cursor() as cur:
                cur.execute(query)      
                f_options = [row[0] for row in cur.fetchall()]
                print(f"Найдены варианты прогноза: {', '.join(f_options)}")
        actual_f = max(f_options, key=lambda f_option: (f_option.split('_')[1], f_option.split('_')[0]))
        print(f'Удаляю все кроме {actual_f}...')
        query = '''DELETE FROM UAT_PRODUCT.PRODUCT_PARAMETERS WHERE PARAM_1 LIKE 'F%' AND PARAM_1 <> ?
        '''
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            with session.cursor() as cur:
                cur.execute(query, params=(actual_f,))
                print(f'{cur.rowcount} rows deleted from UAT_PRODUCT.PRODUCT_PARAMETERS\n')


# %%
if __name__ == '__main__':
    try:
        sap = SAP()
        sap.import_file()
        sap.process_df()
        sap.apply_condition()
        sap.insert_into_uat_pp()
        sap.delete_expired_forecast()
        print('Готово\n')
    except BaseException:
        print(sys.exc_info()[0])
        print(traceback.format_exc())
    finally:
        print("Нажмите Enter, чтобы закрыть окно...")
        input()

        

