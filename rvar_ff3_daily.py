import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime
import pyarrow.feather as feather
import multiprocessing as mp
import pickle as pkl

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

# CRSP Block
crsp = conn.raw_sql("""
                      select a.permno, a.date, a.ret, (a.ret - b.rf) as exret, b.mktrf, b.smb, b.hml
                      from crsp.dsf as a
                      left join ff.factors_daily as b
                      on a.date=b.date
                      where a.date > '01/01/2014'
                      """)

# sort variables by permno and date
crsp = crsp.sort_values(by=['permno', 'date'])

# change variable format to int
crsp['permno'] = crsp['permno'].astype(int)

# Line up date to be end of month
crsp['date'] = pd.to_datetime(crsp['date'])

# find the closest trading day to the end of the month
crsp['monthend'] = crsp['date'] + MonthEnd(0)
crsp['date_diff'] = crsp['monthend'] - crsp['date']
date_temp = crsp.groupby(['permno', 'monthend'])['date_diff'].min()
date_temp = pd.DataFrame(date_temp)  # convert Series to DataFrame
date_temp.reset_index(inplace=True)
date_temp.rename(columns={'date_diff': 'min_diff'}, inplace=True)
crsp = pd.merge(crsp, date_temp, how='left', on=['permno', 'monthend'])
crsp['sig'] = np.where(crsp['date_diff'] == crsp['min_diff'], 1, np.nan)

# label every date of month end
crsp['month_count'] = crsp[crsp['sig'] == 1].groupby(['permno']).cumcount()

# label numbers of months for a firm
month_num = crsp[crsp['sig'] == 1].groupby(['permno'])['month_count'].tail(1)
month_num = month_num.astype(int)
month_num = month_num.reset_index(drop=True)

# mark the number of each month to each day of this month
crsp['month_count'] = crsp.groupby(['permno'])['month_count'].fillna(method='bfill')

# crate a firm list
df_firm = crsp.drop_duplicates(['permno'])
df_firm = df_firm[['permno']]
df_firm['permno'] = df_firm['permno'].astype(int)
df_firm = df_firm.reset_index(drop=True)
df_firm = df_firm.reset_index()
df_firm = df_firm.rename(columns={'index': 'count'})
df_firm['month_num'] = month_num

# daily number
crsp['day_count'] = crsp.groupby(['permno']).cumcount()
day_num = crsp.groupby(['permno'])['day_count'].tail(1)
day_num = day_num.astype(int)
day_num = day_num.reset_index(drop=True)
df_firm['day_num'] = day_num

######################
# Calculate the char #
######################


def get_char_daily(df, firm_list):
    """
    :param df: stock dataframe
    :param firm_list: list of firms matching stock dataframe
    :return: dataframe with variance of residual
    """
    for firm, count, prog in zip(firm_list['permno'], firm_list['day_num'], range(firm_list['permno'].count()+1)):
        prog = prog + 1
        print('processing permno %s' % firm, '/', 'finished', '%.2f%%' % ((prog/firm_list['permno'].count())*100))
        for i in range(count + 1):
            # if you want to change the rolling window, please change here: i - 59 means 60 days is a window.
            temp = df[(df['permno'] == firm) & (i - 59 <= df['day_count']) & (df['day_count'] <= i)]
            # if observations in less than 60 days, we drop the characteristic of this month
            if temp['permno'].count() < 60:
                pass
            else:
                rolling_window = temp['permno'].count()
                index = temp.tail(1).index
                X = pd.DataFrame()
                X[['mktrf', 'smb', 'hml']] = temp[['mktrf', 'smb', 'hml']]
                X['intercept'] = 1
                X = X[['intercept', 'mktrf', 'smb', 'hml']]
                X = np.mat(X)
                Y = np.mat(temp[['exret']])
                res = (np.identity(rolling_window) - X.dot(X.T.dot(X).I).dot(X.T)).dot(Y)
                res_var = res.var(ddof=1)
                df.loc[index, 'rvar'] = res_var
    return df


def sub_df(start, end, step):
    """
    :param start: the quantile to start cutting, usually it should be 0
    :param end: the quantile to end cutting, usually it should be 1
    :param step: quantile step
    :return: a dictionary including all the 'firm_list' dataframe and 'stock data' dataframe
    """
    # we use dict to store different sub dataframe
    temp = {}
    for i, h in zip(np.arange(start, end, step), range(int((end-start)/step))):
        print('processing splitting dataframe:', round(i, 2), 'to', round(i + step, 2))
        if i == 0:  # to get the left point
            temp['firm' + str(h)] = df_firm[df_firm['count'] <= df_firm['count'].quantile(i + step)]
            temp['crsp' + str(h)] = pd.merge(crsp, temp['firm' + str(h)], how='left',
                                             on='permno').dropna(subset=['count'])
        else:
            temp['firm' + str(h)] = df_firm[(df_firm['count'].quantile(i) < df_firm['count']) & (
                    df_firm['count'] <= df_firm['count'].quantile(i + step))]
            temp['crsp' + str(h)] = pd.merge(crsp, temp['firm' + str(h)], how='left',
                                             on='permno').dropna(subset=['count'])
    return temp


def main(start, end, step):
    """
    :param start: the quantile to start cutting, usually it should be 0
    :param end: the quantile to end cutting, usually it should be 1
    :param step: quantile step
    :return: a dataframe with calculated variance of residual
    """
    df = sub_df(start, end, step)
    pool = mp.Pool()
    p_dict = {}
    for i in range(int((end-start)/step)):
        p_dict['p' + str(i)] = pool.apply_async(get_char_daily, (df['crsp%s' % i], df['firm%s' % i],))
    pool.close()
    pool.join()
    result = pd.DataFrame()
    print('processing pd.concat')
    for h in range(int((end-start)/step)):
        result = pd.concat([result, p_dict['p%s' % h].get()])
    return result


# calculate variance of residual through rolling window
# Note: please split dataframe according to your CPU situation. For example, we split dataframe to (1-0)/0.05 = 20 sub
# dataframes here, so the function will use 20 cores to calculate variance of residual.
if __name__ == '__main__':
    crsp_out = main(0, 1, 0.05)

# process dataframe
crsp_out = crsp_out.dropna(subset=['rvar'])  # drop NA due to rolling
crsp_out = crsp_out.rename(columns={'rvar': 'rvar_ff3'})
crsp_out = crsp_out.reset_index(drop=True)
crsp_out = crsp_out[['permno', 'date', 'rvar_ff3']]

with open('rvar_ff3_daily.feather', 'wb') as f:
    feather.write_feather(crsp_out, f)