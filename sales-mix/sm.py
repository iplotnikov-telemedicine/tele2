import teradatasql
import numpy as np
import pandas as pd
from pyxlsb import open_workbook
import io
import csv
from datetime import datetime
import time
import os
# from os.path import join
import re
import numpy as np
from collections import defaultdict
import msvcrt
import sys
import traceback



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


class SkipRowsError(Exception):
    pass

import locale
class ReportMonthError(Exception):
    pass
locale.setlocale(locale.LC_ALL, "ru")


class SalesMix():
    
    def __init__(self, *args, **kwargs):        
        self.path = kwargs.get('path', r'\\corp.tele2.ru\\PLM_Cluster\\All\\_Плановый Sales mix\\')
        self.report_month_str = kwargs.get('report_month_str', None)
        self.fixed_cols_count = 3
        self.param_1 = 'SM Plan'
        self.filename = None
        self.regions_dict = {
            'Краснодар':'Краснодар и Адыгея'   , 
            'Салехард':'ЯНАО'    ,    
            'Абакан':'Хакасия'      ,      
            'Чебоксары':'Чувашия'      ,
            'Ростов-на-Дону':'Ростов на Дону' ,
            'Санкт-Петербург':'С.Петербург'  ,
            'Ханты-Мансийск':'ХМАО' ,
            'Нижний Новгород':'Н.Новгород'   , 
            'Великий Новгород':'В.Новгород'  ,
            'Саранск':'Мордовия'     ,
            'Йошкар-Ола':'Марий Эл'    ,
            'Улан-Удэ':'Бурятия'   ,  
        }
        self.get_report_month_dt()
        self.get_filename()
 

    def get_report_month_dt(self): 
        while 1:
            if self.report_month_str:
                try:    
                    self.report_month_dt = datetime.strptime(self.report_month_str, '%Y-%m-%d')
                except ValueError:
                    print('Некорректный формат. Попробуйте еще.\n')
                else:
                    break
            else:
                self.report_month_str = input('Введите месяц в формате yyyy-mm-dd? "q" for exit\n')
                if self.report_month_str == 'q': raise SystemExit("stopped")
                    
            
            
    def get_filename(self):
        files = []
        for file in os.listdir(self.path):
            filename = self.path + file
            modified_dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(filename)))
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
        
        

    def import_file(self):
        print('импортирую файл...')
        if self.report_month_dt >= datetime(2021, 3, 1):
            self.raw = pd.read_excel(self.filename, skiprows=1)
        else:
            with open_workbook(self.filename) as wb:
                output = io.StringIO()
                writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
                with wb.get_sheet('Channels_MIX') as sheet:
                    for row in sheet.rows():
                        csv_line = [r.v for r in row]
                        writer.writerow(csv_line)
                output.seek(0)
            csvreader = csv.reader(output)
            output.seek(0)
            for row_number, row in enumerate(csvreader):
                if row[0] == 'Регион':
                    col_row = row_number
                    break
            else:
                raise SkipRowsError
            col_indexes = [i for i in range(2)]  # столбцы
            for index, cell_value in enumerate(row):
                try:
                    if 'план' in cell_value: col_indexes.append(index)
                except:
                    continue
            print(f'Старые столбцы: {[row[index] for index in col_indexes]}')
            output.seek(0)
            df = pd.read_csv(output, skiprows=col_row)
            self.raw = df.iloc[:, col_indexes].dropna()
            
    
    def get_cols(self):
        col_num_list = [col.split('.') for col in self.raw.columns]
        col_num_dic = defaultdict(list)
        for col in col_num_list:
            try:
                col[1] = int(col[1])
            except:
                continue
            else:
                col_num_dic[col[0]].append(col[1])
        self.old_cols = [f'{k}.{str(max(v))}' for k, v in col_num_dic.items()]
        self.new_cols = [col.split('.')[0] for col in self.old_cols]        
        
      
    def process_data(self):
        print('обрабатываю данные...')
        if self.report_month_dt >= datetime(2021, 3, 1):
            self.get_cols()
            self.df = self.raw.loc[:, self.old_cols].dropna()
            self.df = self.df.rename(columns={pair[0] : pair[1] for pair in zip(self.old_cols, self.new_cols)})
            self.df = self.df.rename(columns={"Регион" : 'region'})
            self.df.insert(0, 'report_month', self.report_month_dt)
            self.df.insert(2, 'channel', 'Total')
            self.df = self.df.melt(id_vars=self.df.columns[:self.fixed_cols_count].to_list(),
                           value_vars=self.df.columns[self.fixed_cols_count:].to_list(),
                           var_name='tariff',
                           value_name='subs_count')
            self.df.subs_count = self.df.subs_count.astype(float).apply(lambda x: round(x, 2))
            
        else:
            col_names = {
                'Регион': 'region',
                'Channels': 'channel',
                'МР': 'macroregion',
                'Кластер': 'cluster',
                'Мой безлимит': 'Безлимит',
                'Мой разговор/Lite': 'Мой разговор',
                'Мой онлайн/Online': 'Мой онлайн',
                'Мой онлайн +': 'Мой онлайн+',
                'Мой Tele2': 'Лайт/Мой Tele2',
                'Лайт': 'Лайт/Мой Tele2',
                '*любое старое название': '*любое новое название',

            }
            sm1 = self.raw.copy()
            sm1 = sm1.rename(columns=lambda x: re.sub(r'(.+)\s*план', r'\1', x).strip()).rename(columns=col_names)
            sm1 = sm1.rename(columns=lambda x: re.sub(r'(.+)\s*план', r'\1', x).strip()).rename(columns=col_names)
            sm1.insert(0, 'report_month', self.report_month_dt)
            print(f'Новые столбцы: {[sm1.columns.to_list()]}')
            fixed_cols_count = 3
            sm2 = sm1.melt(id_vars=sm1.columns[:fixed_cols_count].to_list(),
                           value_vars=sm1.columns[fixed_cols_count:].to_list(),
                           var_name='tariff',
                           value_name='subs_count')
            self.df = sm2.loc[(sm2.channel == 'Total'),:].dropna()
            self.df.subs_count = self.df.subs_count.astype(float).apply(lambda x: round(x, 2))
                
        
        
    def map_dics(self):
        print('получаю словарь price_plan...')
        self.price_plan = self.get_price_plan()
        self.df = pd.merge(self.df, self.price_plan, how='inner', left_on='tariff', right_on='name_report')
        self.df.insert(0, 'param_1', self.param_1)
        
        print('получаю словарь branch...')
        self.branch_dic = self.get_branch_dic()
        self.df.region = self.df.region.replace(self.regions_dict)
        
        print('делаю маппинг...')
        self.df = pd.merge(self.df, self.branch_dic, how='inner', left_on='region', right_on='region')
        self.empty_regions_df = self.df[self.df['branch_id'].isna()]
        if len(self.empty_regions_df)>0:
            empty_regions_str = ','.join(region for region in self.empty_regions_df.region.unique())
            print(f"Не все регионы нашлись: {empty_regions_str}")
        
              
        
    def get_price_plan(self):
        tariffs_str = ','.join("'{0}'".format(tariff) for tariff in self.df.tariff.unique())
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            query = f'''
                sel
                    case when name_report in ({tariffs_str})
                        then name_report
                        else 'Other' end as name_report,
                    min(tp_id) as tp_id
                from PRD2_DIC_V.PRICE_PLAN
                group by 1
            '''
            price_plan = pd.read_sql(query, session)
        return price_plan
    
    
    def get_branch_dic(self):
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as session:
            query = '''
                sel
                    min(b.branch_id) as branch_id,
                    r.region_name as region
                from PRD2_DIC_V.BRANCH b
                inner join PRD2_DIC_V.REGION r
                    on b.region_id=r.region_id
                where product_cluster_name is not null
                    and branch_id is not null
                    and b.branch_name not like '%CDMA%'
                    and b.branch_name not like '%MVNO%'
                    and b.branch_name not like '%LTE450%'
                    and b.product_cluster_name<>'Deferred'
                group by 2;

            '''
            branch_dic = pd.read_sql(query, session)
        return branch_dic
    
    
    def prepare_cols_for_uat_pp(self):
        print('готовлю датафрейм...')
        cols = ['report_month', 'param_1', 'NULL8',
        'NULL', 'NULL1', 'branch_id', 'NULL6',
        'tp_id', 'NULL3', 'NULL7', 'NULL4', 'subs_count']
        self.df_for_insert = self.df.reindex(columns = cols, fill_value = np.NaN).where((pd.notnull(self.df)), None)

        
    def delete_from_uat_pp(self):
        #self.report_month_dt:%Y-%m-%d
        print('deleting from uat_product.product_parameters...')
        query = f'''DELETE from UAT_PRODUCT.PRODUCT_PARAMETERS
            WHERE param_1 = '{self.param_1}'
            AND report_date = DATE'{self.report_month_str}' 
        '''
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as con:
            with con.cursor() as cur:
                cur.execute(query)      
                print(f'{cur.rowcount} rows deleted from uat_product.product_parameters')
                
                
    def insert_into_uat_pp(self, replace=True, batchsize = 100000):
        print('загружаю в хранилище...')
        if replace: self.delete_from_uat_pp()
        print('inserting into UAT_PRODUCT.PRODUCT_PARAMETERS...')
        rows_count, cols_count = self.df_for_insert.shape
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as con:
            with con.cursor() as cur: 
                for num in range(0, rows_count, batchsize):
                    cur.executemany(f'''
                         INSERT into UAT_PRODUCT.PRODUCT_PARAMETERS ({','.join('?'*cols_count)})
                        ''', [tuple(row) for row in self.df_for_insert.iloc[num:num+batchsize,:].itertuples(index=False)]
                    )
                print(f'{rows_count} rows inserted into UAT_PRODUCT.PRODUCT_PARAMETERS in total')


if __name__ == '__main__':
    print('начинаю...')
    try:
        sales_mix = SalesMix(path=r'\\corp.tele2.ru\\PLM_Cluster\\All\\_Плановый Sales mix\\')
        sales_mix.import_file()
        sales_mix.process_data()
        sales_mix.map_dics()
        sales_mix.prepare_cols_for_uat_pp()
        sales_mix.insert_into_uat_pp()
    except BaseException:
        print(sys.exc_info()[0])
        print(traceback.format_exc())
    else:
        print('готово')
    finally:
        print("Нажмите Enter, чтобы закрыть окно...")
        input()