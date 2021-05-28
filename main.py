import glob
import logging
import os
import sys
from concurrent import futures
from shutil import copyfile

import pandas as pd
import yahoo_fin.stock_info as si
import yfinance as yf
from pandas_datareader import data as pdr
from pandas_datareader._utils import RemoteDataError

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

all_files = glob.glob(os.path.join('.', "*.csv"))

for f in all_files:
    os.remove(f)

yf.pdr_override()
logger.info('Load tickers')
tickers = si.tickers_other()
tickers.extend(si.tickers_dow())
tickers.extend(si.tickers_nasdaq())
tickers.sort()
# Don't duplicate
tickers = dict.fromkeys(tickers)
# Index for graham
aaaEUBondIndex = pd.read_csv(
    'https://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y&type=xls',
    header=4, nrows=1)
aaaEUBondIndex = aaaEUBondIndex['[Percent per annum ]'].iloc[0]
logger.info('Tickers Loaded')
# Default params
paramMargin = 4
paramRoe = 4
paramPayout = 100
paramDividend = 3


def rank_tickers(ticker):
    try:
        df = pdr.get_quote_yahoo(ticker)
        df = df.reindex(
            columns=['epsForward', 'epsCurrentYear', 'twoHundredDayAverageChangePercent', 'epsTrailingTwelveMonths',
                     'regularMarketPrice', 'twoHundredDayAverage', 'price', 'displayName'])
        df.index.names = ['ticker']
        df.reset_index()
        # Apply graham and rules
        df['scoreEps'] = (df['epsForward'][ticker] / df['price'][ticker]) - \
                         (df['epsTrailingTwelveMonths'][ticker] / df['twoHundredDayAverage'][ticker])
        df['scoreEpsMatch'] = df['scoreEps'][ticker] > 0
        df['graham'] = abs(
            (df['epsCurrentYear'][ticker] * (
                        7 + df['twoHundredDayAverageChangePercent'][ticker] * 4.4) / aaaEUBondIndex))
        df['scoreGrahamMatch'] = df['graham'][ticker] > df['price'][ticker]
        stats = si.get_stats(ticker)['Value']
        df['margin'] = float(str(stats.iloc[32]).replace('%', '').replace(',', ''))
        df['scoreMarginMatch'] = df['margin'][ticker] > paramMargin
        df['roe'] = float(str(stats.iloc[34]).replace('%', '').replace(',', ''))
        df['scoreRoeMatch'] = df['roe'][ticker] > paramRoe
        df['payout'] = float(str(stats.iloc[24]).replace('%', '').replace(',', ''))
        df['scorePayoutMatch'] = df['payout'][ticker] < paramPayout
        df['dividendYield'] = float(str(stats.iloc[22]).replace('%', '').replace(',', ''))
        df['scoreDividendYieldMatch'] = paramDividend < df['dividendYield'][ticker] < 15
        info = yf.Ticker(ticker).info
        df['sector'] = info['sector']
        df['industry'] = info['industry']
        df['longBusinessSummary'] = info['longBusinessSummary']
        current_score = sum([df['scoreEpsMatch'], df['scoreGrahamMatch'], df['scoreMarginMatch'],
                             df['scoreRoeMatch'], df['scorePayoutMatch'], df['scoreDividendYieldMatch']])
        # End Apply graham and rules
        conditional = df['scoreDividendYieldMatch'][ticker] and current_score[ticker] >= 5
        if conditional:
            current_score += (df['dividendYield'][ticker] * 100) + (df['scoreEps'][ticker] * 100)
            df['score'] = int(current_score)
            df.to_csv(f'{ticker}.csv')
            logger.info(f'{ticker} - exported')
        else:
            logger.info(f'{ticker} - ignored')

    except (IndexError, KeyError, RemoteDataError, ValueError) as e:
        logger.info(f'{ticker} - no data found')
    except Exception as e:
        raise e


# Parallel process
executor = futures.ProcessPoolExecutor(len(os.sched_getaffinity(0)))
tasks = [executor.submit(rank_tickers, ticker) for ticker in tickers]
futures.wait(tasks)

all_files = glob.glob(os.path.join('.', "*.csv"))
df_from_each_file = (pd.read_csv(f, sep=',') for f in all_files)
df_merged = pd.concat(df_from_each_file, ignore_index=True)
df_merged.to_csv('stocksScreened.csv')

all_files = glob.glob(os.path.join('.', "*.csv"))

for f in all_files:
    if not str(f).endswith('stocksScreened.csv'):
        os.remove(f)

copyfile('stocksScreened.csv', '/home/jessicam/GDrive/Finanças/StockScreener/stocksScreened.csv')
