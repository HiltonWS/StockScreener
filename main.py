import glob
import os
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from shutil import copyfile

import pandas as pd
import yahoo_fin.stock_info as si
import yfinance as yf
from git import Repo
from pandas_datareader import data as pdr
from pandas_datareader._utils import RemoteDataError

# Git params
PATH_OF_GIT_REPO = r'./data'  # make sure .git folder is properly configured
COMMIT_MESSAGE = 'Generated by StockScreener on ' + str(datetime.now())
# Default params
PARAM_MARGIN = 4
PARAM_ROE = 4
PARAM_PAYOUT = 100
PARAM_DIVIDEND = 3
PARAM_COLUMNS = ['epsForward', 'epsCurrentYear', 'twoHundredDayAverageChangePercent', 'epsTrailingTwelveMonths',
                 'regularMarketPrice', 'twoHundredDayAverage', 'price']
tickers = []
aaaEUBondIndex = 0


def remove_old():
    all_files = glob.glob(os.path.join('.', "*.csv"))
    for f in all_files:
        os.remove(f)


def load_tickets():
    global tickers
    global aaaEUBondIndex
    yf.pdr_override()
    # Generate by Fast-Yahoo-Ticker-Symbol-Downloader at home dir
    # https://github.com/TobiasPankner/Fast-Yahoo-Ticker-Symbol-Downloader
    print('Load tickers')
    df = pd.read_csv(str(Path.home()) + "/generic.csv", usecols=["symbol"])
    tickers = df['symbol'].values
    tickers.sort()
    # Don't duplicate
    tickers = dict.fromkeys(tickers)
    # Index for graham; Europe triple AAA bonds changing
    aaaEUBondIndex = pd.read_csv(
        'https://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y&type=xls',
        header=4, nrows=1)
    aaaEUBondIndex = aaaEUBondIndex['[Percent per annum ]'].iloc[0]
    print('Tickers Loaded')
    all_files = glob.glob(os.path.join('./data', "*.skip"))
    try:
        for f in all_files:
            skipped_ticker = f.split('/')[2].replace('.skip', '')
            tickers.pop(skipped_ticker)
            print(skipped_ticker + " skipped")
    except KeyError as e:
        print(e)


def init():
    remove_old()
    load_tickets()


def rules(ticker, df):
    stats = si.get_stats(ticker)['Value']
    info = yf.Ticker(ticker).info
    df['sector'] = info['sector']
    df['industry'] = info['industry']
    df['longBusinessSummary'] = info['longBusinessSummary']
    df['margin'] = float(str(stats.iloc[32]).replace('%', '').replace(',', ''))
    df['roe'] = float(str(stats.iloc[34]).replace('%', '').replace(',', ''))
    df['payout'] = float(str(stats.iloc[24]).replace('%', '').replace(',', ''))
    df['scoreMarginMatch'] = df['margin'][ticker] > PARAM_MARGIN
    df['dividendYield'] = float(str(stats.iloc[22]).replace('%', '').replace(',', ''))
    df['graham'] = abs(
        (df['epsCurrentYear'][ticker] * (
                7 + df['twoHundredDayAverageChangePercent'][ticker] * 4.4) / aaaEUBondIndex))
    df['scoreEps'] = (df['epsForward'][ticker] / df['price'][ticker]) - \
                     (df['epsTrailingTwelveMonths'][ticker] / df['twoHundredDayAverage'][ticker])
    df['scoreEpsMatch'] = df['scoreEps'][ticker] > 0
    df['scoreGrahamMatch'] = df['graham'][ticker] > df['price'][ticker]
    df['scoreRoeMatch'] = df['roe'][ticker] > PARAM_ROE
    df['scorePayoutMatch'] = df['payout'][ticker] < PARAM_PAYOUT
    df['scoreDividendYieldMatch'] = df['dividendYield'][ticker] < 15 and df['dividendYield'][ticker] > PARAM_DIVIDEND


def rank_tickers(ticker):
    filename = './data/%s.skip' % ticker
    try:
        df = pdr.get_quote_yahoo(ticker)
        df = df.reindex(columns=PARAM_COLUMNS)
        df.index.names = ['ticker']
        df.reset_index()
        rules(ticker=ticker, df=df)
        current_score = sum([df['scoreEpsMatch'], df['scoreGrahamMatch'], df['scoreMarginMatch'],
                             df['scoreRoeMatch'], df['scorePayoutMatch'], df['scoreDividendYieldMatch']])

        conditional = df['scoreDividendYieldMatch'][ticker] and current_score[ticker] >= 5
        if conditional:
            current_score += (df['dividendYield'][ticker] * 100) + (df['scoreEps'][ticker] * 100)
            df['score'] = int(current_score)
            df.to_csv(f'{ticker}.csv')
            print(f'{ticker} - exported')
        else:
            print(f'{ticker} - ignored')

    except (IndexError, KeyError, ValueError) as e:
        print(f'{ticker} - no data found')
        option = 'w'
        if os.path.exists(filename):
            option = 'a'
        with open(filename, option) as file:
            file.write(e)
            file.write("\n")
    except RemoteDataError:
        pass
    except Exception as e:
        raise e


def main():
    init()
    # Parallel execution
    with Pool(os.cpu_count() * 2) as pool:
        pool.map(rank_tickers, tickers)
    # Delete stocks csv and concatenate
    all_files = glob.glob(os.path.join('.', "*.csv"))
    df_from_each_file = (pd.read_csv(f, sep=',') for f in all_files)
    df_merged = pd.concat(df_from_each_file, ignore_index=True)
    df_merged.to_csv('stocksScreened.csv')
    all_files = glob.glob(os.path.join('.', "*.csv"))
    for f in all_files:
        if not str(f).endswith('stocksScreened.csv'):
            os.remove(f)
    copyfile("stocksScreened.csv", "./data/stocksScreened.csv")
    try:
        copyfile("stocksScreened.csv", str(Path.home()) + "/GDrive/Finanças/StockScreener/stocksScreened.csv")
    except Exception:
        print("Drive not available")

    # Update module
    repo = Repo(PATH_OF_GIT_REPO)
    repo.git.add(update=True)
    repo.index.commit(COMMIT_MESSAGE)
    origin = repo.remote(name='origin')
    origin.push()
    # Update project
    repo = Repo('.')
    repo.git.add(update=True)
    repo.index.commit(COMMIT_MESSAGE)
    origin = repo.remote(name='origin')
    origin.push()


if __name__ == '__main__':
    main()
