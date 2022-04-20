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
# определяем, что это за отчет, смотрим максимальную дату для него. если она превышает максимальную дату в базе для этого отчета,
#     то удаляем в базе текущий месяц
#     заливаем в базу текущий месяц из файла
#     сохраняем актуальный файл в папку \Data


from exchangelib import Credentials, Account, FileAttachment, EWSDateTime, EWSTimeZone
import os.path
import pandas as pd
import teradatasql
from datetime import datetime, timedelta
import configparser
import numpy as np
import io
import sys


class Handler():
    tablename = 'UAT_PRODUCT.MN_DIRECT_IN_OUT'
    cols_dict = {'CDRDate1': 'CDR_DATE',
                 'Carrier1': 'CARRIER',
                 'Traffic_Direction21': 'DIRECTION',
                 'Destination1': 'DESTINATION',
                 'TotalDurationInMinutes_Sum': 'MINUTES',
                 'Total_Calls_Connected': 'TOTAL_CALLS_CONNECTED',
                 'Total_Cost': 'COST',
                 'Rate_List_Currency1': 'CURRENCY',
                 'CDR_Date': 'CDR_DATE',
                 'GlobalCarrier': 'CARRIER',
                 'Branch': 'BRANCH',
                 'Destination_Band': 'DESTINATION',
                 'Minutes': 'MINUTES',
                 'Cost': 'COST',
                 'CDRDate_Value': 'CDR_DATE',
                 'GlobalCarrier_Value': 'CARRIER',
                 'Region_Value': 'BRANCH',
                 'Destination_Value': 'DESTINATION',
                 'TotalDurationInMinutes_Value': 'MINUTES',
                 }


    def __init__(self, csv_content, csv_name):
        self.csv_name = csv_name
        self.direction = self.csv_name.split(' ')[0]
        self.carrier_type = ('H' if 'H_' in self.csv_name else 'T')
        self.scheme, self.table = self.tablename.split('.')
        self.raw = pd.read_csv(csv_content, thousands=',')
        self.df = pd.DataFrame()
        self.report_month = None
        self.max_csv_date = None
        self.max_db_date = None


    def process(self):
        print(f'Processing {self.csv_name}...')
        df = self.raw.copy()
        df.rename(self.cols_dict, axis=1, inplace=True)
        if 'BRANCH' not in df.columns:
            df[['CARRIER', 'BRANCH']] = df['CARRIER'].str.extract(r'([A-Za-z0-9_]*)\s(T2\s[A-Za-z0-9]*)', expand=True, )
        df['CARRIER_TYPE'] = df['CARRIER'].str.extract(r'([HT])\_[A-Z0-9]*', expand=True, )
        df = df[df['CARRIER_TYPE'] == self.carrier_type].reset_index()
        df['DIRECTION'] = self.direction
        df['MINUTES'] = df['MINUTES'].apply(lambda x: pd.to_numeric(x, downcast='float'))
        df['INSERT_DATE'] = datetime.today().strftime('%Y-%m-%d')

        for date_format in ('%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y'):
            try:
                df['CDR_DATE'] = df['CDR_DATE'].apply(lambda x: datetime.strptime(x, date_format).strftime('%Y-%m-%d'))
            except ValueError:
                continue
            else:
                df['CDR_MONTH'] = self.report_month = df['CDR_DATE'][0][0:-2] + '01'
                self.max_csv_date = max(df['CDR_DATE'])
                break
        try:
            db_columns = self.get_db_columns()
        except:
            print(f'Error: columns list is not available for {self.tablename}')
            sys.exit(0)
        else:
            df = df.reindex(columns=db_columns, fill_value=np.NaN).where((pd.notnull(df)), None)
        self.df = df
        print(f'Data is processed.')


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


    def get_max_db_date(self):
        print(f'Checking for relevance...')
        with teradatasql.connect() as con:
            with con.cursor() as cur:
                cur.execute(f'''
                    SELECT
                        MAX(CDR_DATE)
                    FROM {self.tablename}
                    WHERE DIRECTION = '{self.direction}'
                        AND CDR_MONTH = '{self.report_month}'
                        AND CARRIER_TYPE = '{self.carrier_type}'
                ''')
                max_db_date = cur.fetchall()[0][0]
        if max_db_date:
            return max_db_date.strftime('%Y-%m-%d')
        else:
            return self.report_month


    def check_for_insert(self):
        self.max_db_date = self.get_max_db_date()
        if not self.max_db_date:
            print('Error: max_db_date is not defined')
        elif not self.max_csv_date:
            print('Error: max_csv_date is not defined.')
        elif self.max_csv_date >= self.max_db_date:
            self.insert()
            self.filename = f'{sys.path[0]}\\data\\{self.csv_name} {self.report_month}.xlsx'
            self.df.to_excel(self.filename, index=False)
            print(f'Saved as {self.filename}.\n')
        elif self.max_csv_date < self.max_db_date:
            print('The report is outdated.\n')


    def insert(self, batchsize=100000, replace=True):
        rows_count, cols_count = self.df.shape
        with teradatasql.connect() as con:
            with con.cursor() as cur:
                if replace:
                    print(f'Deleting from {self.tablename}...')
                    cur.execute(f'''
                        DELETE FROM {self.tablename}
                        WHERE DIRECTION = '{self.direction}'
                            AND CDR_MONTH = '{self.report_month}'
                            AND CARRIER_TYPE = '{self.carrier_type}'
                    ''')
                    print(f'{cur.rowcount} rows deleted from {self.tablename}.')

                print(f'Inserting into {self.tablename}...')
                for num in range(0, rows_count, batchsize):
                    cur.executemany(f'''
                         INSERT into {self.tablename} ({','.join('?' * cols_count)})
                        ''', [tuple(row) for row in self.df.iloc[num:num + batchsize, :].itertuples(index=False)]
                                    )
                print(f'{rows_count} rows inserted into {self.tablename} in total.')

                print(f'Collecting statistics...')
                cur.execute(f'''
                    SELECT rtrim(ColumnName) as ColumnName
                    FROM  dbc.indices
                    where DatabaseName = '{self.scheme}'
                        and TableName = '{self.table}'
                ''')
                columns = cur.fetchall()
                col_name = [f'COLUMN({col[0]})' for col in columns]
                collect_query = 'COLLECT STATISTICS ' + ', '.join(col_name) + f' ON {self.tablename};'
                cur.execute(collect_query)
                print(f'Statistics were collected.')


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


def main(source_type: str, source: str):
    if source_type == 'mailbox':
        account = log_into_account(mailbox=source)  # 'igor.i.plotnikov@tele2.ru'
        last_msgs = get_messages(account, folder='МН_ИТК', number_of_days=1)
        for msg in last_msgs:
            if msg.sender.name == 'Interconnect Reporting System':
                for attachment in msg.attachments:
                    if isinstance(attachment, FileAttachment) and attachment.name.endswith('.csv'):
                        msg_datetime_str = msg.datetime_received.replace(tzinfo=None).strftime("%Y-%m-%d %H-%M-%S")
                        print(f'Importing {attachment.name} received at {msg_datetime_str}...')
                        csv_report = io.StringIO(attachment.content.decode('utf-8'))
                        try:
                            handler = Handler(csv_report, attachment.name)
                        except:
                            print(
                                f'Report version is not defined for {attachment.name} received at {msg_datetime_str}.')
                        else:
                            handler.process()
                            handler.check_for_insert()
    elif source_type == 'directory':
        if source[-1] != '\\': source = source + '\\'
        for file in os.listdir(source):
            if file.endswith('.csv'):
                filepath = source + file
                print(f'Importing {filepath}...')
                try:
                    handler = Handler(filepath, file)
                except:
                    print(f'Report version is not defined for {filepath}')
                else:
                    handler.process()
                    handler.check_for_insert()
    print('\nAll IC data is up to date.\n')
    return None

if __name__ == '__main__':
#     main(source_type='directory', source=r'C:\Users\igor.i.plotnikov\PycharmProjects\work\ИК\2021-03')
    main(source_type='mailbox', source='product_reporting@tele2.ru')