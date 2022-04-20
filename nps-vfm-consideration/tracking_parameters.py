import pandas as pd
from datetime import date
import teradatasql
import numpy as np
from dateutil.relativedelta import relativedelta
import os
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


class TrackingParameters():
    
    options = [
        {'sheets' : ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM','NPS','Для таких людей как я',
                     'Честный', 'Удобный, простой'],
         'option_name':'express',
         'name_rus' : 'Экспресс-мониторинг показателей трекинга',
         'kpis' : ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM','NPS','Для таких людей как я',
                     'Честный', 'Удобный, простой'],
         'area_cats' : ['Macro New', 'Region', 'Total'],
         'cols_dict' : {
            'Показатель':'kpi',
            'География\n (Total / macro / region)':'area_cat',
            'География':'area_cat',
            'Выбор':'area',
            'Оператор':'operator'
            },
        },

        {'sheets' : ['Динамика KPIs'],
         'option_name':'general',
         'name_rus' : 'Основные',
         'kpis': ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM'],
         'area_cats' : ['Cluster New', 'Macroregion new', 'Region new', 'Region old', 'Total Russia'],
         'cols_dict' : {
            'Показатель':'kpi',
            'Оператор':'operator',
            'Категория':'area_cat',   
            'Категория_Выбор':'area',
            'Вариант1':'cat_3' 
            },
        },

        {'sheets' : ['Динамика KPIs'],
         'option_name':'general',
         'name_rus' : 'Osnovnye',
         'kpis': ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM'],
         'area_cats' : ['Cluster New', 'Macroregion new', 'Region new', 'Region old', 'Total Russia'],
         'cols_dict' : {
            'Показатель':'kpi',
            'Оператор':'operator',
            'Категория':'area_cat',   
            'Категория_Выбор':'area',
            'Вариант1':'cat_3' 
            },
        },

        {'sheets' : ['data'],
         'option_name':'social_subs',
         'name_rus' : 'Соц дем абоненты',
         'kpis' : ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM','NPS'],
         'area_cats' : ['Cluster New', 'Macroregion New', 'Region new', 'Region old', 'Total Russia'],
         'cols_dict' : {
            'Показатель':'kpi',
            'Оператор':'operator',
            'Категория':'area_cat',   
            'Категория_Выбор':'area',
            'Разбивка1':'cat_1',
            'Вариант1':'cat_2',
            'Вариант2':'cat_3'
            },
        },

        {'sheets' : ['data'],
         'option_name':'social_subs',
         'name_rus' : 'Soc dem abonenty',
         'kpis' : ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM','NPS'],
         'area_cats' : ['Cluster New', 'Macroregion New', 'Region new', 'Region old', 'Total Russia'],
         'cols_dict' : {
            'Показатель':'kpi',
            'Оператор':'operator',
            'Категория':'area_cat',   
            'Категория_Выбор':'area',
            'Разбивка1':'cat_1',
            'Вариант1':'cat_2',
            'Вариант2':'cat_3'
            },
        },

        {'sheets' : ['data'],
         'option_name':'social',
         'name_rus' : 'Соцдем',
         'kpis' : ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM','NPS'],
         'area_cats' : ['Cluster New', 'Macroregion New', 'Region new', 'Region old', 'Total Russia'],
         'cols_dict' : {
            'Показатель':'kpi',
            'Оператор':'operator',
            'Категория':'area_cat',   
            'Категория_Выбор':'area',
            'Разбивка1':'cat_1',
            'Вариант1':'cat_2'
            },
        },

        {'sheets' : ['data'],
         'option_name':'social',
         'name_rus' : 'Socdem',
         'kpis' : ['AA','AL','BA','Consideration','CP','ICP','IQP','PLP','QP','VFM','NPS'],
         'area_cats' : ['Cluster New', 'Macroregion New', 'Region new', 'Region old', 'Total Russia'],
         'cols_dict' : {
            'Показатель':'kpi',
            'Оператор':'operator',
            'Категория':'area_cat',   
            'Категория_Выбор':'area',
            'Разбивка1':'cat_1',
            'Вариант1':'cat_2'
            },
        }
    ]

    
    def __init__(self, path=r'\\corp.tele2.ru\\plm_cluster\\All\\tracking_parameters\\'):
        self.option = None
        self.path = path
        self.shortname = None
        self.areas_dict = {
            'Total Russia all':'Total Russia',
            'Center':'Центр',
            'Chernozem':'Черноземье',
            'Baikal & Far East':'Байкал и Дальний Восток',
            'North-West':'Северо-Запад',
            'Siberia':'Сибирь',
            'South':'Юг',
            'Volga':'Волга',
            'Ural':'Урал',
            'Moscow':'Москва и область',
        }
        
        self.operators_dict = {
            'Tele2':'Tele2',
            'Yota':'YOTA',
            'МТС':'MTS',
            'Билайн':'BEE',
            'Мегафон':'MGF',
            'Мотив':'Мотив',
        }
        self.choose_file()
     
    
    def choose_file(self):
        for file in enumerate(os.listdir(self.path)):
            print(file)
        file_index = int(input('Какой номер нужен? '))
        shortname = None
        for i, file in enumerate(os.listdir(self.path)):
            if i == file_index:
                print(f'Выбран {file}\n')
                shortname = file
                self.filename = self.path + shortname
                break
        if not shortname:
            print('Файл не выбран\n')
        else:
            self.shortname = shortname
        

    def import_file(self, filename = None):       
        if not filename: filename = self.filename
        print('Читаю файл...')
        self.xls = pd.read_excel(filename, sheet_name = None)        


    def get_raw(self):
        print('Определяю сценарий для файла...')
        self.filename_wo_extension = self.shortname.split('.')[0]
        for option in self.options:
            for sheet in option['sheets']:
                if sheet in self.xls.keys() and self.filename_wo_extension.endswith(option['name_rus']):
                    self.option = option
                    print(f'Выбран сценарий для {option}\n')
                    break
                else:
                    continue   
        if not self.option:
            print('Файл не опознан, сценарий не выбран.')    
        union_df = pd.DataFrame()
        for sheet in self.option['sheets']:
            single_df = self.xls[sheet]
            if self.option['option_name'] == 'express': single_df['kpi'] = sheet
            union_df = pd.concat([union_df,single_df]).drop_duplicates()
        self.raw = union_df
#         print(self.raw.head())
    

    def process(self):
        print('processing...')
        df = self.raw.copy()
        df.rename(columns=self.option['cols_dict'], inplace=True) 
        df = df[df.kpi.isin(self.option['kpis'])]
        df = df[df.area_cat.isin(self.option['area_cats'])]
        if self.option['option_name'] == 'general':  
            df = df[df['Разбивка1'].isin(['Subs / nonsubs'])]
            df.cat_3 = df.cat_3.apply(lambda x: 'Свой' if 'Есть' in x else 'Чужой')
        elif self.option['option_name'] == 'social_subs':
            df = df[df.cat_1.isin(['Area', 'ARPU declared', 'Arpu declared'])]
            df.cat_3 = df.cat_3.apply(lambda x: 'Свой' if 'Есть' in x else 'Чужой')
        elif self.option['option_name'] == 'social':
            df = df[df.cat_1.isin(['Area', 'ARPU declared', 'Arpu declared'])]
            df['cat_3'] = None
        df = df[(~df.area.str.contains('Total')) | (df.area == 'Total Russia all')]
        for col in df.columns:
            if ('Изменение' in col or 'Base' in col or 'Q' not in col) and (col not in self.option['cols_dict'].values()):
                df.drop([col], axis=1, inplace=True)
        df = df.melt(id_vars = [col for col in df.columns if 'Q' not in col],
             value_vars=[col for col in df.columns if 'Q' in col],
             var_name='quarter',
             value_name='param_value')
        df.area.replace(self.areas_dict, inplace=True) 
        df['period_start'] = df.quarter.apply(lambda x: self.quarter_to_date(x))
        df['period_end'] = df['period_start'].apply(lambda x: x + relativedelta(months=3) - relativedelta(days=1))
        df['source_table'] = self.option['option_name']
        cols = ['kpi', 'operator', 'area_cat', 'area', 'cat_1', 'cat_2', 'cat_3',
               'period_start', 'period_end', 'param_value', 'source_table']
        df = df.reindex(columns = cols,  fill_value = np.NaN).where((pd.notnull(df)), None)
        self.df = df
        print(self.df.head())
        
        
    def insert_into_uat_tp(self):      
        with teradatasql.connect(host=HOST, user=USR, password=PWD, logmech=LOGMECH) as con:
            with con.cursor() as cur:
                print('deleting from UAT_PRODUCT.TRACKING_PARAMETERS...')
                cur.execute(f'''
                    delete from UAT_PRODUCT.TRACKING_PARAMETERS
                    where source_table in ('{self.option['option_name']}');
                ''')
                print(f'{cur.rowcount} rows deleted from UAT_PRODUCT.TRACKING_PARAMETERS')

                batchsize = 100000
                print('inserting into UAT_PRODUCT.TRACKING_PARAMETERS...')
                for num in range(0, len(self.df), batchsize):
                    cur.executemany(f'''
                     INSERT into UAT_PRODUCT.TRACKING_PARAMETERS ({','.join('?'*len(self.df.columns))})
                    ''',
                        [tuple(row) for row in self.df.iloc[num:num+batchsize,:].itertuples(index=False)]
                        )
                print(f'{len(self.df)} rows inserted into UAT_PRODUCT.TRACKING_PARAMETERS.')

                print('COLLECTING STATISTICS...')
                cur.execute('''
                    COLLECT STATISTICS
                        COLUMN(KPI)
                        ,COLUMN(OPERATOR)
                        ,COLUMN(AREA_CAT)
                        ,COLUMN(AREA)
                        ,COLUMN(CAT_1)
                        ,COLUMN(CAT_2)
                        ,COLUMN(CAT_3)
                        ,COLUMN(PERIOD_START)
                        ,COLUMN(PERIOD_END)
                        ,COLUMN(PARAM_VALUE)
                        ,COLUMN(SOURCE_TABLE)
                        ON UAT_PRODUCT.TRACKING_PARAMETERS;
                ''')
                print(f'{cur.rowcount} COLUMNS DONE.\n')
             
            
    @staticmethod
    def quarter_to_date(q_string):
        '''
        преобразует формат Q1'20 или 1Q'20 в 2020-01-01
        '''
        q_str, y_str = q_string.split("'")   
        month_int = 3 * int(''.join(c for c in q_str if c.isdigit())) - 2
        year_int = 2000 + int(''.join(c for c in y_str if c.isdigit()))
        return date(year_int, month_int, 1)



if __name__ == '__main__':
    try:
        tp1 = TrackingParameters()
        tp1.import_file()
        tp1.get_raw()
        tp1.process()
        tp1.insert_into_uat_tp()
    except BaseException:
        print(sys.exc_info()[0])
        print(traceback.format_exc())
    finally:
        print("Нажмите Enter, чтобы закрыть окно...")
        input()