#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# sm = сырая таблица
# sm1 = слайс по нужным столбцам и строкам + столбец с месяцем
# sm2 = анпивот по тарифам в плоскую таблицу


# In[1]:


import pandas as pd
from pyxlsb import open_workbook
import io
import csv
from datetime import datetime
import os
from os.path import join
import re

class SkipRowsError(Exception):
    pass

class ReportMonthError(Exception):
    pass

def process_file(filename, union_df):
    
    with open_workbook(filename) as wb:
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
        with wb.get_sheet('Channels_MIX') as sheet:
            for row in sheet.rows():
                csv_line = [r.v for r in row]
                writer.writerow(csv_line)
        output.seek(0)
    csvreader = csv.reader(output)

    # In[111]:

    output.seek(0)
    row1 = next(csvreader)
    try:
        ordinal_days = int(float(row1[0]))
    except:
        raise ReportMonthError
    report_month_dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + ordinal_days - 2).date()
    print(f'Отчетный месяц: {report_month_dt}')


    # In[113]:

    output.seek(0)
    for row_number, row in enumerate(csvreader):
        if row[0] == 'Регион':
            col_row = row_number
            break
    else:
        raise SkipRowsError

    # In[117]:

    col_indexes = [i for i in range(4)]  # столбцы
    for index, cell_value in enumerate(row):
        try:
            if 'план' in cell_value: col_indexes.append(index)
        except:
            continue
    print(f'Старые столбцы: {[row[index] for index in col_indexes]}')


    # In[117]:

    output.seek(0)
    sm = pd.read_csv(output, skiprows=col_row)
    sm1 = sm.iloc[:, col_indexes].dropna()

    # In[118]:

    col_names = {
        'Регион': 'region',
        'Channels': 'channel',
        'МР': 'macroregion',
        'Кластер': 'cluster',
        'Мой безлимит': 'Безлимит',
        '*любое старое название': '*любое новое название',

    }
    sm1 = sm1.rename(columns=lambda x: re.sub(r'(.+)план', r'\1', x).strip()).rename(columns=col_names)
    sm1.insert(0, 'report_month', report_month_dt)
    print(f'Новые столбцы: {[sm1.columns.to_list()]}')
    # In[122]:

    fixed_cols_count = 5
    sm2 = sm1.melt(id_vars=sm1.columns[:fixed_cols_count].to_list(),
                   value_vars=sm1.columns[fixed_cols_count:].to_list(),
                   var_name='tariff',
                   value_name='subs_count')
    sm2 = sm2.loc[(sm2.channel != 'Total'), :].dropna()

    # In[123]:

    sm2pivot = sm2.groupby(['tariff', 'channel']).sum().reset_index()
    sm2pivot.pivot(index='tariff', columns='channel', values='subs_count')

    # In[132]:

    union_df = pd.concat([union_df, sm2]).drop_duplicates()
    return union_df



if __name__ == '__main__':
    dirname = r'\\corp.tele2.ru\\plm_cluster\\All\\_Плановый Sales mix\\for Tableau\\'
    union_df = pd.DataFrame()
    
    for filename in os.listdir(dirname):
        print(filename, '...')
        
        # обработать filename, засунуть его в union_df и вернуть обогащенный union_df
        union_df = process_file(join(dirname,filename), union_df)
        
        print(f'Промежуточный размер датафрейма: {union_df.shape}\n')

    print(union_df.tariff.value_counts())
    newfilename = r'C:\Users\igor.i.plotnikov\Documents\My Tableau Repository\Datasources\TheGreatDashboard\Sales Mix.xlsx'
    union_df.to_excel(newfilename, index = False)

    print('Done')

