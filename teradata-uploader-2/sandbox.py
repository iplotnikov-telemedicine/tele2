from random import randint
import sqlite
from time import sleep, time
import streamlit as st
from teradatasql import connect, OperationalError
import pandas as pd
from configparser import ConfigParser
from PIL import Image
import os
import re
from transliterate import translit
import numpy as np
from itertools import zip_longest
from datetime import datetime
import logging
# from streamlit.report_thread import get_report_ctx
# ctx = get_report_ctx()

curdir = os.path.dirname(os.path.realpath(__file__)) + r'\\'
css_file = os.path.join(curdir, r'style.css')

title = 'Teradata Uploader'
st.set_page_config(page_title=title,  # String or None. Strings get appended with "• Streamlit".
    page_icon=Image.open(curdir + 'photo_53153.jpg'),
    # String, anything supported by st.image, or None.
    layout='wide',  # Can be "centered" or "wide". In the future also "dashboard"
    initial_sidebar_state='expanded'  # Can be "auto", "expanded", "collapsed"
)

NAMING_PATTERN = r"^[A-Za-z][A-Za-z0-9\_]*$"

FILE_FORMATS = ['csv','xls','xlsx', 'xlsb']

DATA_TYPES = [
    'VARCHAR(11)',
    'VARCHAR(100)',
    'VARCHAR(500)',
    'VARCHAR(5000)',
    'LONG VARCHAR',
    'FLOAT',
    'DECIMAL(18,6)',
    'DECIMAL(15,0)',
    'DECIMAL(12,0)',
    'DATE', #from datetime import datetime \n datetime.today().strftime('%Y-%m-%d')
    'TIMESTAMP(0)', #from datetime import datetime \n datetime.now().strftime(f'%Y-%m-%d %H:%M:%S.%f+00:00')
    'INTEGER',
    'BIGINT'
    # 'NUMERIC' : float,
    # 'CHAR' : str,
]

def stylize_text(text):
    return f'''<p style="overflow: hidden; border: 1px solid; border-radius: 5px;
                border-color: #1f2229; padding: 0.7em; background-color: #1f2229;
                font-weight: bold; font-size: 16px; ">{text}</p>'''


@st.experimental_memo(show_spinner=False)
def get_reserved_words(rw_filename):
    with open(rw_filename) as f:
        content = f.readlines()
        return [line.strip() for line in content]



def log(text):
    st.session_state.logger.info(f'''{ctx.session_id} \
        {'unknown' if not 'session_id' in st.session_state else st.session_state.session_id} \
        {'unknown' if not 'username' in st.session_state else st.session_state.username} \
        {text}''')

# logging.basicConfig(filename='app.log', encoding='utf-8', format='%(asctime)s - %(name)s - %(message)s', level=logging.INFO)


def local_css(css_file):
    with open(css_file) as f:
        st.markdown('<style>{}</style>'.format(f.read()), unsafe_allow_html=True)





@st.experimental_singleton
def get_db_connection(params, username, password, session_counter):
    session = connect(**params, user=username, password=password)
    return session


@st.experimental_memo(show_spinner=False)
def get_session_id(_session):
    with _session.cursor() as cur:
        query = 'SELECT SESSION'
        with st.spinner(text=f'Getting session id ...'):
            cur.execute(query)
            session_id = cur.fetchall()[0][0]
    return session_id


@st.experimental_memo(show_spinner=False)
def get_databases(_session):
    log(f'getting databases')
    query = f'''SELECT      distinct DatabaseName 
        FROM        DBC.UserRoleRightsV
        WHERE       AccessRight='I' 
    '''
    with st.spinner(text=f'Executing {query} ...'):
        result = pd.read_sql(query, _session)
        response = result.DatabaseName.to_list()
    return response



@st.experimental_memo(show_spinner=False)
def get_tables(_session, database, counter):
    # SHOW TABLES [ FROM schema ] [ LIKE pattern ]
    log(f'getting tables')
    query = f'''SELECT      distinct TableName
            FROM        DBC.TablesV
            WHERE       TableKind = 'T'
            AND         DatabaseName = '{database}'
            ORDER BY    1'''
    with st.spinner(text=f'Executing {query} ...'):
        result = pd.read_sql(query, _session)
        response = result.TableName.to_list()
    return response


@st.experimental_memo(show_spinner=False)
def get_help(_session, database, table):
    log(f'getting columns')
    with _session.cursor() as cur:
        query = f'HELP TABLE {database}."{table}"'
        with st.spinner(text=f'Executing {query} ...'):
            # print(query)
            cur.execute(query)
            target_dtype_codes = {col[0].strip():col[1].strip() for col in cur.fetchall()}
    return target_dtype_codes


@st.experimental_memo(show_spinner=False)
def get_df(file):
    fname = file.name
    df = pd.DataFrame()
    if fname.endswith('csv'):
        df = pd.read_csv(file, engine='python', delimiter=';', decimal=',') #encoding='cp1251', 
    elif fname.endswith('xlsb'):
        df = pd.read_excel(file, engine='pyxlsb')
    else:
        xls = pd.ExcelFile(file)
        sheetname = xls.sheet_names[0]
        df = pd.read_excel(file, sheet_name=sheetname, encoding='utf-8-sig')
    return df


def get_error_text(dtypes_dict, database, tablename, tables):
    if not re.match(NAMING_PATTERN, tablename):
        return f"Ошибка: выберите другое имя для таблицы"
    if tablename.upper() in reserved_words:
        return f"Ошибка: выберите другое имя для таблицы"
    if tablename in tables:
        return f"Ошибка: {database}.{tablename} уже существует"
    for column_dict in dtypes_dict.values():
        if column_dict['column_name'].upper() in reserved_words:
            return f"Ошибка: выберите другое имя для {column_dict['column_name']}"
        if not re.match(NAMING_PATTERN, column_dict['column_name']):
            return f"Ошибка: выберите другое имя для {column_dict['column_name']}"
        if column_dict['dtype'] == '':
            return f"Ошибка: не выбран тип данных для {column_dict['column_name']}"

    cols = [col_dict['column_name'] for col_dict in dtypes_dict.values()]
    if len(set(cols)) != len(cols):
        return f"Ошибка: имена полей должны быть уникальными"

    pk = [col_dict['pk'] for col_dict in dtypes_dict.values()]
    if True not in pk:
        return f"Ошибка: не определен первичный ключ"

    # если все проверки пройдены
    return None


@st.experimental_memo(show_spinner=False)
def create_table(_session, database, table):
    col_names = [col['column_name'] for col in st.session_state.created_dtypes.values()]
    data_types = []
    for col in st.session_state.created_dtypes.values():
        if col['dtype'] == 'DATE':
            data_types.append(col['dtype'] + " FORMAT 'YYYY-MM-DD'")
        elif col['dtype'] == 'TIMESTAMP(0)':
            data_types.append(col['dtype'] + " FORMAT 'YYYY-MM-DDbHH:MI:SS'")
        else:
            data_types.append(col['dtype'])
    # data_types = [col['dtype'] if col['dtype']!='DATE' else col['dtype']+" FORMAT 'YYYY-MM-DD'" for col in st.session_state.created_dtypes.values()]

    pk = [col['column_name'] for col in st.session_state.created_dtypes.values() if col['pk']]
    with _session.cursor() as cur:
        query = f'''CREATE MULTISET TABLE {database}."{table}"
                ({', '.join(map(' '.join, zip(col_names, data_types)))})
                PRIMARY INDEX({', '.join(pk)})'''
        # print(query)
        with st.spinner(text=f'Executing {query} ...'):
            cur.execute(query)


def fit_data_types(df):
    new_df = df.copy()
    null_num = 0
    for col, col_dict in st.session_state.mapping.items():
        column = col_dict['source_column']
        code = col_dict['dtype_code']
        if column == 'NULL':
            st.session_state.mapping[col]['source_column'] = column + str(null_num)
            new_df[column + str(null_num)] = np.NaN
            null_num += 1
            continue
        if code == 'DA':
            for date_format in ('%Y-%m-%d', '%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y'):
                try:
                    datetime.strptime(str(new_df[column][0]), date_format).date()
                except ValueError:
                    pass
                else:
                    new_df[column] = pd.to_datetime(new_df[column], format=date_format).apply(lambda x: x.date())
                    break
        elif code == 'TS':
            new_df[column] = pd.to_datetime(new_df[column], format='%Y-%m-%d %H:%M:%S') # FORMAT 'yyyy-mm-ddbhh:mi:ss'
            # new_df[column] = new_df[column].apply(lambda x: datetime.strptime(x[:19], '%Y-%m-%d %H:%M:%S'))
        elif code == 'CV':
            new_df[column] = np.where(pd.isnull(new_df[column]),new_df[column],new_df[column].astype(str))
        elif code in ['D','F']:
            new_df[column] = new_df[column].apply(lambda x: pd.to_numeric(x))
        elif code in ['I8','I']:
            new_df[column] = new_df[column].apply(lambda x: pd.to_numeric(x))
            # new_df[column] = new_df[column].astype(int)
    columns = [col['source_column'] for col in st.session_state.mapping.values()]
    new_df = new_df.reindex(columns=columns, fill_value=np.NaN).where((pd.notnull(new_df)), None)
    return new_df


def get_proper_name(old_name: str):
    eng_name = translit(old_name, "ru", reversed=True)  # переводим в латиницу
    eng_name_wo_blanks = re.sub('\s', '_', eng_name)
    new_name = re.sub("[^0-9a-zA-Z\_]+", "", eng_name_wo_blanks)
    return new_name.upper()


def get_supposed_dtypes(df):
    new_df = df.copy()
    dtypes = {column:dict() for column in new_df.columns}
    for column, dtype_dict in dtypes.items():
        dtype_dict['dtype'] = None
        dtype_dict['column_name'] = get_proper_name(column)
        dtype_dict['pk'] = False

        # try date
        for date_format in ('%Y-%m-%d', '%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y'):
            try:
                datetime.strptime(str(new_df[column][0]), date_format).date()
            except:
                pass
            else:
                dtype_dict['dtype'] = 'DATE'
                new_df[column] = pd.to_datetime(new_df[column], format=date_format).apply(lambda x: x.date())
                break

        if not dtype_dict['dtype'] and 'MSISDN' in dtype_dict['column_name'].upper():
            dtype_dict['dtype'] = 'VARCHAR(11)'

        # try number
        if not dtype_dict['dtype']:
            try:
                new_df[column].apply(lambda x: pd.to_numeric(x))
            except:
                pass
            else:
                new_df[column] = new_df[column].apply(lambda x: pd.to_numeric(x))
                # try float
                new_column = column+'_rounded'
                new_df[new_column] = round(new_df[column], 0)
                if new_df[new_column].equals(df[column]):
                    # these are integers
                    if 'SUBS_ID' in dtype_dict['column_name'].upper():
                        dtype_dict['dtype'] = 'DECIMAL(12,0)'
                    elif max(df[column]) > 2147483647:
                        dtype_dict['dtype'] = 'BIGINT'
                    else:
                        dtype_dict['dtype'] = 'INTEGER'
                else:
                    # these are floats
                    dtype_dict['dtype'] = 'FLOAT'
                del new_df[new_column]
        if not dtype_dict['dtype']:
            max_len = new_df[column].apply(str).str.len().max()
            if max_len<=11:
                dtype_dict['dtype'] = 'VARCHAR(11)'
            elif max_len<=100:
                dtype_dict['dtype'] = 'VARCHAR(100)'
            elif max_len<=500:
                dtype_dict['dtype'] = 'VARCHAR(500)'
            elif max_len<=5000:
                dtype_dict['dtype'] = 'VARCHAR(5000)'
            else:
                dtype_dict['dtype'] = 'LONG VARCHAR'
    return dtypes


# @st.experimental_memo(show_spinner=False)
def insert(_session, database, table, df):
    # print(database)
    # print(table)
    # print(df.head())
    with _session.cursor() as cur:
        batchsize = 100000
        with st.spinner(text=f'INSERTING INTO {database}.{table} ...'):
            for num in range(0, len(df), batchsize):
                query = f"INSERT INTO {database}.{table} ({','.join('?' * len(df.columns))})"
                cur.execute(query, [tuple(row) for row in df.iloc[num:num + batchsize, :].itertuples(index=False)])
                response = f'{len(df)} rows successfully inserted into {database}.{table}'
        return response


@st.experimental_memo(show_spinner=False)
def clear_table(_session, database, table):
    with _session.cursor() as cur:
        with st.spinner(text=f'CLEARING {database}.{table} ...'):
            query = f'''DELETE FROM {database}."{table}"'''
            cur.execute(query)
            response = f'{cur.rowcount} rows successfully deleted from {database}.{table}'
        return response


def set_up_sidebar(session):

    st.write('')
    file = st.file_uploader("Выберите файл для загрузки",
            accept_multiple_files=False, type=FILE_FORMATS, key=st.session_state.file_uploader_counter)
    if file:
        st.session_state.filename = file.name
        st.session_state.df = get_df(file)
        if st.session_state.df is None:
            st.error("Ошибка чтения файла")
            st.stop()
        databases = [''] + get_databases(st.session_state.session)
        database_index = databases.index(st.session_state.database)
        database = st.selectbox('Выберите схему', databases, index=database_index)
        if database:

            def init_create_table():
                st.session_state.main_page = 'init_create'

            def init_mapping():
                st.session_state.main_page = 'mapping'

            st.session_state.database = database
            if 'my_tables' not in st.session_state:
                st.session_state.my_tables = get_tables(session, st.session_state.database, st.session_state.counter)
                st.session_state.my_tables.insert(0, '')
            if 'new_table_name' in st.session_state:
                table_index = st.session_state.my_tables.index(st.session_state.new_table_name)
                del st.session_state.new_table_name
            elif 'table' in st.session_state:
                if st.session_state.table in st.session_state.my_tables:
                    if st.session_state.table == '':
                        table_index = 0
                    else:
                        table_index = st.session_state.my_tables.index(st.session_state.table)
                elif st.session_state.table not in st.session_state.my_tables:
                    st.session_state.my_tables.append(st.session_state.table)
                    table_index = st.session_state.my_tables.index(st.session_state.table)  
            else:
                table_index = 0
            table = st.selectbox('Выберите таблицу', 
                st.session_state.my_tables, 
                index=table_index, 
                key='table',
                on_change=init_mapping)
            cols = st.columns([1,5])
            cols[0].write('или')
            cols[1].button('создайте новую', on_click=init_create_table)
            # if table and st.session_state.df_for_insert.empty:
                # st.session_state.main_page = 'mapping'
            # elif table and not st.session_state.df_for_insert.empty:
            #     st.session_state.main_page = 'inserting'



def set_up_main_page(session):

    
    
    if st.session_state.main_page == 'welcome':
        st.header(f'Добрый день, {st.session_state.username}!')
        st.caption('История ваших загрузок')
        history_df = sqlite.get_history(st.session_state.conn, st.session_state.username)
        # history_df = pd.DataFrame(columns=['Дата и время', 'Имя файла', 'Схема', 'Таблица'])
        st.dataframe(history_df)

    elif st.session_state.main_page == 'init_create':
        st.markdown('### Создание таблицы')
        # st.session_state
        st.session_state.created_dtypes = get_supposed_dtypes(st.session_state.df)
        cols = st.columns([4, 2, 1])
        if 'filename' in st.session_state:
            new_table_name = cols[0].text_input('Введите имя для новой таблицы',
                value=get_proper_name(st.session_state.filename.split('.')[0])
            )
        else:
            st.error('Сначала выберите файл')
            st.stop()
        st.write('')
        table_header = st.columns([4, 2, 1])
        for col, col_name in enumerate(['Поле','Тип данных','ПК']):
            table_header[col].markdown(col_name)
        for column, dtype_dict in st.session_state.created_dtypes.items():
            cols = st.columns([4, 1.75, 0.25, 1])
            st.session_state.created_dtypes[column]['column_name'] = cols[0].text_input('',
                value=dtype_dict['column_name'], key=f'{column}'
            )
            dtype_index = DATA_TYPES.index(dtype_dict['dtype'])
            st.session_state.created_dtypes[column]['dtype'] = cols[1].selectbox('',
                DATA_TYPES, index=dtype_index, key=f'{column}_dtype'
            )
            cols[3].write('')
            st.session_state.created_dtypes[column]['pk'] = cols[3].checkbox('', key=f'{column}_pk')
        st.write('')
        
        slot = st.empty()
        cols = slot.columns(4)



        def run_create(new_table_name):
            error_text = get_error_text(
                st.session_state.created_dtypes, st.session_state.database, 
                new_table_name, st.session_state.my_tables
            )
            if error_text:
                with slot.error(error_text):
                    sleep(1)
            else:
                try:
                    create_table(session, st.session_state.database, new_table_name)
                except Exception:
                    st.error('Что-то пошло не так')
                else:
                    st.session_state.new_table_name = new_table_name
                    st.session_state.my_tables.append(st.session_state.new_table_name)
                    st.session_state.my_tables.sort()
                log(f'created table {st.session_state.database}.{st.session_state.new_table_name}')
                st.session_state.main_page = 'mapping'
        cancel_create_btn = cols[1].button('Отменить')
        if cancel_create_btn:
            st.session_state.main_page = 'welcome'
            st.experimental_rerun()
        cols[2].button('Создать', on_click=run_create, args=(new_table_name, ))
        if 'new_table_name' in st.session_state:
            st.experimental_rerun()


    elif st.session_state.main_page == 'mapping':
        st.markdown('### Маппинг полей')
        if 'table' in st.session_state:
            columns_dict = get_help(session, st.session_state.database, st.session_state.table)
        else:
            st.error('Сначала выберите таблицу')
            st.stop()
        st.session_state.mapping = {key:{'dtype_code':value} for key, value in columns_dict.items()}
        with st.form(key='mapping_form'):
            columns_from = st.session_state.df.columns.to_list() + ['NULL']
            columns_to = [col for col in st.session_state.mapping.keys()]
            dtype_codes = [col['dtype_code'] for col in st.session_state.mapping.values()]
            # col_1, col_2 = st.columns([11, 8])
            # col_1.write(f'{st.session_state.database}.{st.session_state.table}')
            # col_2.write(st.session_state.filename)
            triples = zip_longest(
                    columns_to,
                    dtype_codes,
                    columns_from[:min(len(columns_to),
                    len(columns_from))],
                    fillvalue='NULL'
            )
            col_1, col_2, col_3, col_4 = st.columns([8, 2, 1, 8])
            col_1.caption('Target Field')
            col_2.caption('DT Code')
            col_4.caption('Source Field')
            for target_field, dtype_code, source_field in triples:
                col_1, col_2, col_3, col_4 = st.columns([8, 2, 1, 8])
                col_1.markdown(stylize_text(target_field), unsafe_allow_html=True)
                col_2.markdown(stylize_text(dtype_code), unsafe_allow_html=True)
                col_3.write('')
                col_3.image(Image.open(curdir + 'left_arrow1.png'), width=20)
                st.session_state.mapping[target_field]['source_column'] = col_4.selectbox(
                    '', columns_from, index=columns_from.index(source_field), key=f'{target_field}_source')
            st.write('')
            slot = st.empty()
            cols = slot.columns(3)
            run_map_btn = cols[1].form_submit_button('Далее')
            if run_map_btn:
                st.session_state.df_for_insert = fit_data_types(st.session_state.df)
                log(f'mapped columns')
                st.session_state.main_page = 'inserting'


    elif st.session_state.main_page == 'inserting':
        st.markdown('### Загрузка данных в хранилище')
        st.caption('Сэмпл данных')
        st.dataframe(st.session_state.df_for_insert[:10])
        st.write('')

        # if not st.session_state.inserted:
        if st.session_state.table != '':
            def init_upload():
                inserted = insert(session, st.session_state.database, st.session_state.table, st.session_state.df_for_insert)
                if inserted:
                    sqlite.add_record(st.session_state.conn,
                        st.session_state.filename, 
                        st.session_state.database, 
                        st.session_state.table, 
                        st.session_state.username
                    )
                    log(f'{inserted}')
                    with st.balloons():
                        sleep(1)
                    st.session_state.main_page = 'welcome'

            if not st.session_state.deleted:
                cols = st.columns([4,2])
                cols[0].markdown('<div style="text-align: right;">Перед загрузкой вы можете</div>', unsafe_allow_html=True)
                clear_btn = cols[1].button("очистить таблицу")
            else:
                st.success(st.session_state.deleted)
                clear_btn = None
            insert_btn = st.button("Загрузить", on_click=init_upload)
            if clear_btn:
                st.session_state.deleted = clear_table(session, st.session_state.database, st.session_state.table)
                if st.session_state.deleted:
                    log(f'{st.session_state.deleted}')
                    st.experimental_rerun()
                # st.success(st.session_state.deleted)
        else:
            st.error('Сначала выберите таблицу!')


def set_up(session):

    if 'counter' not in st.session_state: st.session_state.counter = 0
    if 'create' not in st.session_state: st.session_state.create = False
    if 'filename' not in st.session_state: st.session_state.filename = None
    if 'inserted' not in st.session_state: st.session_state.inserted = False
    if 'deleted' not in st.session_state: st.session_state.deleted = False
    if 'database' not in st.session_state: st.session_state.database = ''
    if 'df_for_insert' not in st.session_state: st.session_state.df_for_insert = pd.DataFrame()
    if 'main_page' not in st.session_state: st.session_state.main_page = 'welcome'
    if 'history_df' not in st.session_state: st.session_state.history_df = pd.DataFrame()

    with st.sidebar:
        set_up_sidebar(session)
    set_up_main_page(session)

                    


    # INSERTING
    
    # if st.session_state.df_for_insert is not None and not st.session_state.create:
    #     with st.expander(label="Ready to insert"):
    #         st.dataframe(st.session_state.df_for_insert[:10])
    #         st.caption('Data sample')
    #         st.write('')

    #         if not st.session_state.inserted:
    #             if not st.session_state.deleted:
    #                 _, clear_col, insert_col, _ = st.columns([4, 4, 4, 4])
    #                 clear_btn = clear_col.button("CLEAR TABLE")
    #             else:
    #                 st.success(st.session_state.deleted)
    #                 _, insert_col, _ = st.columns([6, 4, 6])
    #                 clear_btn = None
    #             insert_btn = insert_col.button("INSERT")


    #             if insert_btn:
    #                 inserted = insert(session, database, table, st.session_state.df_for_insert)
    #                 if inserted:
    #                     st.balloons()
    #                     st.session_state.inserted = inserted
    #                     log(f'{st.session_state.inserted}')
    #                     st.experimental_rerun()
    #             if clear_btn:
    #                 st.session_state.deleted = clear_table(session, database, table)
    #                 if st.session_state.deleted:
    #                     log(f'{st.session_state.deleted}')
    #                     st.experimental_rerun()
    #                 # st.success(st.session_state.deleted)



    #     if st.session_state.inserted:
    #         st.success(st.session_state.inserted)

    #         _, upload_more_col, _ = st.columns([7, 5, 7])
    #         upload_more_btn = upload_more_col.button("Загрузить еще")
    #         if upload_more_btn:
    #             st.session_state.inserted = False
    #             for attr in st.session_state.keys():
    #                 st.session_state.widget_key = str(randint(1000, 100000000))
    #                 if attr not in ['username', 'password', 'session', 'session_id', 'logger']:
    #                     del st.session_state[attr]
    #             st.experimental_rerun()



def try_log_in(params, username, password, session_counter):
    log(f'tried to log in')
    try:
        st.session_state.session = get_db_connection(params, username, password, session_counter)
        # print(st.session_state.session)
    except OperationalError:
        st.sidebar.error("Ошибка подключения: неверно введено имя пользователя или пароль. Попробуйте еще")
    except BaseException:
        st.sidebar.error('Ошибка подключения: Teradata занята')
    else:
        st.session_state.session_id = get_session_id(st.session_state.session)
        log('logged in')
        st.experimental_rerun()


def get_seconds_after_last_action():
    if 'latest_time' not in st.session_state:
        st.session_state.latest_time = time()
        st.session_state.seconds_after_last_action = 0
    else:
        then = st.session_state.latest_time
        st.session_state.latest_time = time()
        st.session_state.seconds_after_last_action = int(st.session_state.latest_time - then)
    return st.session_state.seconds_after_last_action


def main():

    reserved_words = get_reserved_words('reserved_words.txt')

    serverconfig = ConfigParser()
    config_file = 'config.ini'

    serverconfig.read('config.ini')
    params = dict(serverconfig["SERVERCONFIG"])


    log_file = 'app.log'
    f_handler = logging.FileHandler(log_file)
    f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)



    if 'logger' not in st.session_state:
        st.session_state.logger = logging.getLogger()
        st.session_state.logger.addHandler(f_handler)

    local_css(css_file) 


    st.sidebar.title(title)
    if 'session_counter' not in st.session_state:
        st.session_state.session_counter = 0
    if 'file_uploader_counter' not in st.session_state:
        st.session_state.file_uploader_counter = 0
    log_in_slot = st.sidebar.empty()
    if get_seconds_after_last_action() > 1000:
        st.session_state.session_counter += 1
        st.session_state.file_uploader_counter += 1
        st.session_state.main_page = 'welcome'
        try_log_in(params, st.session_state.username, st.session_state.password, st.session_state.session_counter)
    if 'session' in st.session_state:
        if 'conn' not in st.session_state:
            st.session_state.conn = sqlite.create_connection('DB.sqlite3')
        set_up(st.session_state.session) 
    else:
        with log_in_slot.form(key='log_in'):
            st.session_state.username = st.text_input(f'Имя пользователя', autocomplete="username")
            st.session_state.password = st.text_input(f'Пароль', type='password', autocomplete="current-password")
            log_in_btn = st.form_submit_button('Подключиться')
        if log_in_btn and 'username' in st.session_state and 'password' in st.session_state:
            try_log_in(params, st.session_state.username, st.session_state.password, st.session_state.session_counter)


if __name__ == '__main__':
    main()