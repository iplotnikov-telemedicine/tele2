# CREATE MULTISET TABLE UAT_PRODUCT.MN_DIRECT_IN_OUT ,NO FALLBACK ,
#      NO BEFORE JOURNAL,
#      NO AFTER JOURNAL,
#      CHECKSUM = DEFAULT,
#      DEFAULT MERGEBLOCKRATIO,
#      MAP = TD_MAP1
#      (
#       CDR_MONTH DATE FORMAT 'YYYY-MM-DD',
#       CDR_DATE DATE FORMAT 'YYYY-MM-DD',
#       CARRIER VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
#       BRANCH VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
#       DIRECTION VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
#       DESTINATION VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
#       MINUTES DECIMAL(18,6),
#       COST DECIMAL(18,6),
#       CURRENCY VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC,
#       INSERT_DATE DATE FORMAT 'YYYY-MM-DD')
# PRIMARY INDEX ( CDR_MONTH ,CDR_DATE ,CARRIER ,BRANCH ,DIRECTION ,
# DESTINATION );


# скрипт идет в почту
# проверяет письма за {2} последних дня
# для каждого письма: если отправитель Interconnect Reporting System и есть вложение .csv,
#     то создаем объект класса Handler, прокидываем в него содержимое файла и его название
#       определяем, что это за отчет, смотрим максимальную дату для него. 
#   если она превышает максимальную дату в базе для этого отчета,
#     то удаляем в базе текущий месяц
#     заливаем в базу текущий месяц из файла
#     сохраняем актуальный файл в папку \Data


from exchangelib import Credentials, Account, FileAttachment, EWSDateTime, EWSTimeZone
import os.path
import csv
import pandas as pd
import teradatasql
from datetime import datetime, timedelta
import configparser
import numpy as np
import io
import sys
from pandas.errors import ParserError

curdir = os.path.dirname(os.path.realpath(__file__)) + '\\'

DATE_FORMATS = [
    '%d.%m.%Y',
    '%d.%m.%y',
    '%d/%m/%Y',
    '%m/%d/%Y %H:%M:%S'
]

def get_destination(code: str, dest_dict: dict):
    if len(code) >= 2:
        for dest, codes_list in dest_dict.items():
            if code in codes_list:
                return dest
        else:
            code = code[:-1]
            return get_destination(code, dest_dict)
    else:
        return None


class Handler():
    tablename = 'UAT_PRODUCT.MN_IN_OUT'
    cols_dict = {'CDRDate1': 'CDR_DATE',
                 'START_DATE': 'CDR_DATE',
                 'START_DTTM': 'CDR_DATE',
                 'CDR_Date': 'CDR_DATE',
                 'Carrier1': 'CARRIER',
                 'Traffic_Direction21': 'DIRECTION',
                 'Destination1': 'DESTINATION',
                 'TotalDurationInMinutes_Sum': 'MINUTES',
                 'DURATION': 'MINUTES',
                 'Total_Calls_Connected': 'TOTAL_CALLS_CONNECTED',
                 'Total_Cost': 'COST',
                 'Rate_List_Currency1': 'CURRENCY',
                 'GlobalCarrier': 'CARRIER',
                 'Branch': 'BRANCH',
                 'Destination_Band': 'DESTINATION',
                 'Minutes': 'MINUTES',
                 'Cost': 'COST',
                 'CDRDate_Value': 'CDR_DATE',
                 'GlobalCarrier_Value': 'CARRIER',
                 'Region_Value': 'BRANCH',
                 'OPERATOR_NAME': 'DESTINATION',
                 'Destination_Value': 'DESTINATION',
                 'TotalDurationInMinutes_Value': 'MINUTES',
                 'CARRIER_SHORT_NAME': 'CARRIER'
                 }


    def __init__(self, filename, filepath):
        self.filename = filename
        self.direction = None
        self.report_month = None
        self.max_date = None
        self.row_count = None
        self.db_max_date = None
        self.db_row_count = None
        self.raw = self.read_file(filename, filepath)
        self.scheme, self.table = self.tablename.split('.')
        self.df = pd.DataFrame()



    def read_file(self, filename, filepath):
        if filename.endswith('.txt.gz'):
            df = pd.read_csv(filepath, compression='gzip', header=0, quoting=3, sep=';', engine='python')
            for col in df.columns:
                df[col] = df[col].apply(str).str.replace('"', '')
            return df
        elif filename.endswith('.csv'):
            # csv_report = io.StringIO(filepath.decode('utf-8'))
            for engine in ['c', 'python']:
                for delimiter in [',', None]:
                    try:
                        df = pd.read_csv(filepath, delimiter=delimiter, sep=';', engine=engine)
                    except (UnicodeDecodeError, ParserError):
                        continue
                    else:
                        return df
        else:
            print('Unknown format')
            return None


    def process(self):
        print(f'Processing {self.filename}...')
        df = self.raw.copy()
        df['DATA_SOURCE'] = self.filename

        if 'OPERATOR_NAME' in df.columns:
            self.direction = 'Incoming'
        else:
            if 'DIRECTION' in df.columns:
                self.direction = df['DIRECTION'][0]
            else:
                self.direction = 'Outgoing'
        df['DIRECTION'] = self.direction
        
        df.rename(self.cols_dict, axis=1, inplace=True)
        
        if df['MINUTES'].dtype == 'O': df['MINUTES'] = df['MINUTES'].str.replace(',', '.')
        df['MINUTES'] = df['MINUTES'].apply(lambda x: pd.to_numeric(x, downcast='float'))

        if df['COST'].dtype == 'O': df['COST'] = df['COST'].str.replace(',', '.')
        df['COST'] = df['COST'].apply(lambda x: pd.to_numeric(x, downcast='float'))

        if 'BRANCH' not in df.columns:
            carrier_pattern = r'([A-Z]{1}\_[A-Z0-9]{3})\s([A-Z0-9]{2}\s[A-Z0-9]*)'
            df[['CARRIER', 'BRANCH']] = df['CARRIER'].str.extract(carrier_pattern, expand=True)

        if 'CURRENCY' not in df.columns:
            df['CURRENCY'] = np.NaN

        df['CARRIER_TYPE'] = df['CARRIER'].str.extract(r'([A-Z]{1})\_[A-Z0-9]{3}', expand=True)
        df = df[(df['CARRIER_TYPE'].isin(['T', 'H'])) & (~df['CARRIER'].isin(['T_INT', 'H_RTK']))]
        df.reset_index(inplace=True, drop=True)
        if len(df) > 0:
            for date_format in DATE_FORMATS:
                try:
                    df['CDR_DATE'] = df['CDR_DATE'].apply(lambda x: datetime.strptime(x, date_format).strftime('%Y-%m-%d'))
                except ValueError:
                    continue
                else:
                    df['CDR_MONTH'] = self.report_month = df['CDR_DATE'][0][0:-2] + '01'
                    self.max_date = max(df['CDR_DATE'])
                    break
        else:
            print('Данные не найдены в файле')
            return
        df['INSERT_DATE'] = datetime.today().strftime('%Y-%m-%d')
        if self.direction == 'Incoming':
            mn_in_dic = self.get_mn_in_dic()
            df = pd.merge(df, mn_in_dic, how='left', on='DESTINATION')
        elif self.direction == 'Outgoing':
            mn_out_mapping = self.get_mn_out_mapping()
            df = pd.merge(df, mn_out_mapping, how='left', on='DESTINATION')
            mn_out_dic = self.get_mn_out_dic()
            dest_dict = {row['NEW_DESTINATION']: row['DIAL_CODES'].split(', ') for i, row in mn_out_dic.iterrows()}
            df['NEW_DESTINATION'] = df['FIRST_CODE'].apply(lambda x: get_destination(str(x), dest_dict))
            df['DESTINATION'] = df.apply(
                lambda x: x['NEW_DESTINATION'] if x['NEW_DESTINATION'] else x['DESTINATION'], axis=1)
            df['COUNTRY'] = df['COUNTRY'].apply(lambda x: x if x else 'Unknown')
            df['AREA'] = df['AREA'].apply(lambda x: x if x else 'Unknown')

        try:
            db_columns = self.get_db_columns()
        except:
            print(f'Error: columns list is not available for {self.tablename}')
            sys.exit(0)
        else:
            df = df.reindex(columns=db_columns, fill_value=np.NaN).where((pd.notnull(df)), None)
        self.row_count = len(df)
        self.df = df


    def get_mn_in_dic(self):
        query = '''sel * from uat_product.incoming_mn_dic'''
        with teradatasql.connect() as session:
            df = pd.read_sql(query, session)
        df.columns = ['DESTINATION', 'COUNTRY', 'AREA']
        return df


    def get_mn_out_dic(self):
        query = '''sel * from uat_product.outgoing_mn_dic'''
        with teradatasql.connect() as session:
            df = pd.read_sql(query, session)
        df.columns = ['DIAL_CODES', 'NEW_DESTINATION']
        return df


    def get_mn_out_mapping(self):
        query = '''sel * from uat_product.outgoing_mn_mapping'''
        with teradatasql.connect() as session:
            df = pd.read_sql(query, session)
        df.columns = [
            'DESTINATION', 'DESTINATION_REGION_WO_TILDA', 'CODES', 'DESTINATION_WRONG', 'COUNTRY', 'AREA']
        df['CODES'] = df['CODES'].apply(str)
        df['FIRST_CODE'] = df['CODES'].apply(lambda x: x.split(',')[0].replace('810', ''))
        df = df[['DESTINATION', 'FIRST_CODE', 'COUNTRY', 'AREA']]
        return df


    def get_db_columns(self):
        with teradatasql.connect() as con:
            with con.cursor() as cur:
                cur.execute(f''' 
                    SEL rtrim(ColumnName) as ColumnName
                    FROM  DBC.Columns
                    WHERE DatabaseName = '{self.scheme}'
                        and TableName = '{self.table}'
                    order by ColumnId
                ''')
                db_columns = [col[0] for col in cur.fetchall()]
        return db_columns


    def get_db_max_date(self):
        '''
        смотрим, какие последняя дата и сколько строк для нее загружены в таблицу в терадате
        возвращаем кортеж (дата, число строк)
        '''
        print(f'Checking for relevance...')
        with teradatasql.connect() as con:
            with con.cursor() as cur:
                cur.execute(f'''
                    SELECT
                        MAX(CDR_DATE),
                        COUNT(*) as ROW_COUNT
                    FROM {self.tablename}
                    WHERE DIRECTION = '{self.direction}'
                        AND CDR_MONTH = '{self.report_month}'
                ''')
                db_max_date, row_count = cur.fetchall()[0]
        if db_max_date:
            return db_max_date.strftime('%Y-%m-%d'), row_count
        else:
            return None, None


    def check_for_insert(self):
        self.db_max_date, self.db_row_count = self.get_db_max_date()
        if not self.db_max_date: self.db_max_date = self.report_month
        if not self.db_row_count: self.db_row_count = 0
        if not self.max_date:
            print('Error: max_date is not defined.')
        elif self.max_date >= self.db_max_date:
            print(f'Max date {self.db_max_date} from database is older or equals {self.max_date}')
            if self.row_count > self.db_row_count:
                print(f'And row count from database ({self.db_row_count}) is fewer than imported ({self.row_count})')
                self.insert()
            else:
                print(f'But row count from database ({self.db_row_count}) is more than or equals imported ({self.row_count})')
                print('The report is outdated.\n')
            # self.filename = f'{sys.path[0]}\\data\\{self.report_month} {self.filename}'
            # self.df.to_excel(self.filename, index=False)
            # print(f'Saved as {self.filename}.\n')
        elif self.max_date < self.db_max_date:
            print('The report is outdated.\n')


    def insert(self, batchsize=100000, replace=True):
        print('Inserting...')
        rows_count, cols_count = self.df.shape
        with teradatasql.connect() as con:
            with con.cursor() as cur:
                if replace:
                    print(f'Deleting from {self.tablename}...')
                    cur.execute(f'''
                        DELETE FROM {self.tablename}
                        WHERE DIRECTION = '{self.direction}'
                            AND CDR_MONTH = '{self.report_month}'
                    ''')
                    print(f'{cur.rowcount} rows deleted from {self.tablename}.')

                print(f'Inserting into {self.tablename}...')
                for num in range(0, rows_count, batchsize):
                    cur.executemany(f'''
                         INSERT into {self.tablename} ({','.join('?' * cols_count)})
                        ''', [tuple(row) for row in self.df.iloc[num:num + batchsize, :].itertuples(index=False)]
                                    )
                print(f'{rows_count} rows inserted into {self.tablename} in total.\n')

                # print(f'Collecting statistics...')
                # cur.execute(f'''
                #     SELECT rtrim(ColumnName) as ColumnName
                #     FROM  dbc.indices
                #     where DatabaseName = '{self.scheme}'
                #         and TableName = '{self.table}'
                # ''')
                # columns = cur.fetchall()
                # col_name = [f'COLUMN({col[0]})' for col in columns]
                # collect_query = 'COLLECT STATISTICS ' + ', '.join(col_name) + f' ON {self.tablename};'
                # cur.execute(collect_query)
                # print(f'Statistics were collected.\n')


def log_into_account(mailbox: str):
    print('Logging in...\n')
    c = configparser.ConfigParser()
    c.read('C:\\Users\\igor.i.plotnikov\\setup.cfg')
    user = c['teradata']['user']
    pw = c['teradata']['password']
    # https://webmail.tele2.ru/owa/
    credentials = Credentials(username=user, password=pw)
    account = Account(mailbox, credentials=credentials, autodiscover=True)
    return account


def get_messages(account, folder, number_of_days: int):
    print('Checking email...\n')
    tz = EWSTimeZone.localzone()
    now = EWSDateTime.now().replace(tzinfo=tz)
    period = now - timedelta(days=number_of_days)
    target_folder = account.inbox / folder
    messages = target_folder.filter(datetime_received__gt=period).order_by('-datetime_received')
    return messages


def handle_file(name, filepath):
    print(f"Handling {name}...")
    handler = Handler(name, filepath)
    # try:
    #     handler = Handler(file, name)
    # except:
    #     print(f'Report version is not defined for {name}.')
    # else:
    handler.process()
    if not handler.df.empty:
        handler.check_for_insert()


def main(source_type: str, source: str, number_of_days: int = None):
    if source_type == 'mailbox':
        account = log_into_account(mailbox=source)  # 'igor.i.plotnikov@tele2.ru'
        last_msgs = get_messages(account, folder='МН_ИТК', number_of_days=number_of_days)
        for msg in last_msgs:
            for attachment in msg.attachments:
                if isinstance(attachment, FileAttachment) and (attachment.name.endswith('.txt.gz')
                or attachment.name.startswith('Outgoing MN direct daily')
                ):
                    filepath = os.path.join(curdir + '\\tmp', attachment.name)
                    with open(filepath, 'wb') as f:
                        f.write(attachment.content)
                    # msg_datetime_str = msg.datetime_received.replace(tzinfo=None).strftime("%Y-%m-%d %H-%M-%S")
                    handle_file(attachment.name, filepath)
    elif source_type == 'directory':
        if source[-1] != '\\': source = source + '\\'
        for file in os.listdir(source):
            if file.startswith('Outgoing MN direct daily') or file.endswith('.txt.gz'):
                filepath = source + file
                handle_file(file, filepath)
    print('\nAll IC data is up to date.\n')
    return None


if __name__ == '__main__':
    # main(source_type='directory', source=r"C:\Users\igor.i.plotnikov\PycharmProjects\work\MN\Отчеты\temp")
    main(source_type='mailbox', source='product_reporting@tele2.ru', number_of_days=1)