import teradatasql
from datetime import date

proc = 'UAT_PRODUCT.MARKET_TELE2_INSERT_DAILY'
weekday = date.today().isoweekday()
monthday = date.today().day


def call_proc(proc, update_period):
    with teradatasql.connect() as con:
        print(f'Calling {proc} ({update_period})...')
        with con.cursor() as cur:
            cur.callproc(proc, [update_period])
        print(f'{cur.rowcount} rows processed.')



if __name__ == '__main__':
    print(weekday)
    print(monthday)

    call_proc(proc, 'day')
    call_proc(proc, 'week')
    call_proc(proc, 'month')

    # if weekday == 2 or weekday == 5:
    #     call_proc(proc, 'week')
    #
    # if monthday == 5:
    #     call_proc(proc, 'month')
    #
    #
    #
    #
