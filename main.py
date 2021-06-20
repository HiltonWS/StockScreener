import glob
import os
import sys
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from shutil import copyfile
from time import sleep

import pandas as pd
import yahoo_fin.stock_info as si
import yfinance as yf
from git import Repo
from pandas.errors import EmptyDataError
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
sleeps = 5
resume = None

if len(sys.argv) >= 2 and sys.argv[1] == '-r':
    resume = sys.argv[1]
    print('Resuming process')


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
    tickers = set(df['symbol'].values)
    # Index for graham; Europe triple AAA bonds changing
    aaaEUBondIndex = pd.read_csv(
        'https://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y&type=xls',
        header=4, nrows=1)
    aaaEUBondIndex = aaaEUBondIndex['[Percent per annum ]'].iloc[0]
    print('Tickers Loaded')
    try:
        print("Skipping")
        with open('./data/ignore.data', 'r') as file:
            lines = file.read().splitlines()
        tickers = list(tickers - set(lines))
        tickers.sort()
        all_files = glob.glob(os.path.join("*.resume"))
        print("Resuming")
        if resume:
            for f in all_files:
                resume_ticker = f.replace('.resume', '')
                tickers = tickers[tickers.index(resume_ticker)+1:]
                print(resume_ticker + " - resuming")
    except (KeyError, ValueError) as e:
        pass


def init():
    if not resume:
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
    if resume and os.path.exists(f'{ticker}.csv'):
        return
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
            filename = '%s.resume' % ticker
            all_files = glob.glob(os.path.join('.', "*.resume"))
            try:
                for f in all_files:
                    os.remove(f)
            except FileNotFoundError:
                pass
            with open(filename, 'w') as file:
                file.write(str('Last ticker'))
                file.write("\n")

    except (IndexError, KeyError, ValueError, TypeError) as e:
        print(f'{ticker} - no data found')
        global sleeps
        if str(e).lower().endswith('no tables found') and sleeps < 900:
            print('Sleeping ' + str(sleeps))
            sleep(sleeps)
            sleeps = sleeps * 2
            rank_tickers(ticker)
        else:
            sleeps = 5
        with open('./data/ignore.data', 'r') as file:
            lines = file.readlines()
            last_line = lines[len(lines) - 1].replace('\n', '')
        if last_line != ticker:
            with open('./data/ignore.data', 'a') as file:
                file.write(ticker)
                file.write("\n")
    except RemoteDataError:
        pass
    except Exception as e:
        return e


def main():
    init()
    # Parallel execution
    with Pool(os.cpu_count() * 15) as pool:
        pool.map(rank_tickers, tickers)
    # Delete stocks csv and concatenate
    all_files = glob.glob(os.path.join('.', "*.csv"))
    df_from_each_file = []
    for f in all_files:
        try:
            df_from_each_file.append(pd.read_csv(f, sep=',', error_bad_lines=False))
        except EmptyDataError:
            continue
    df_merged = pd.concat(df_from_each_file, ignore_index=True)
    df_merged.to_csv('stocksScreened.csv')
    all_files = glob.glob(os.path.join('.', "*.csv"))
    for f in all_files:
        if not str(f).endswith('stocksScreened.csv'):
            os.remove(f)
    copyfile("stocksScreened.csv", "./data/stocksScreened.csv")
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
