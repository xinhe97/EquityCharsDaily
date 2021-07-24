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
                      select a.permno, a.date, a.ret, (a.ret - b.rf) as exret, b.mktrf, b.smb, b.hml, a.vol, a.prc, 
                             a.shrout, a.askhi, a.bidlo
                      from crsp.dsf as a
                      left join ff.factors_daily as b
                      on a.date=b.date
                      where a.date > '01/01/2014'
                      """)

with open('crsp_dsf_2014.feather', 'wb') as f:
    feather.write_feather(crsp, f)