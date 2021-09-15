#%%
import warnings

warnings.filterwarnings("ignore")
import re
import pickle
import requests
import json
from threading import Thread
import pandas as pd

# from bs4 import BeautifulSoup

from persiantools.jdatetime import JalaliDate
import time
import pandas as pd
import datetime

import urllib.request


sessions = 0
start = datetime.datetime.now()
totalSessions = 0
#%%
stock_id, date = 2400322364771558, 20210911


def clean_LOB(i):
    second = int(str(i["hEven"])[-2:])
    minute = int(str(i["hEven"])[-4:-2])
    hour = int(str(i["hEven"])[:-4])
    position = i["number"]
    bid_volume = i["qTitMeDem"]
    bid_quent = i["zOrdMeDem"]
    bid_price = i["pMeDem"]
    ask_price = i["pMeOf"]
    ask_quent = i["zOrdMeOf"]
    ask_volume = i["qTitMeOf"]
    return (
        hour,
        minute,
        second,
        position,
        bid_volume,
        bid_quent,
        bid_price,
        ask_volume,
        ask_quent,
        ask_price,
    )


def get_LOB(stock_id, date, result):
    url = "http://cdn.tsetmc.com/api/BestLimits/{}/{}".format(stock_id, date)
    r = requests.get(url, timeout=15)
    r = json.loads(r.text)
    global sessions
    sessions += 1
    LOB = []
    for i in r["bestLimitsHistory"]:
        LOB.append(clean_LOB(i))
    result[0] = LOB


def clean_Trade(i):
    position = i["nTran"]
    second = int(str(i["hEven"])[-2:])
    minute = int(str(i["hEven"])[-4:-2])
    hour = int(str(i["hEven"])[:-4])
    quant = i["qTitTran"]
    price = i["pTran"]
    canceled = i["canceled"]
    return (
        hour,
        minute,
        second,
        position,
        quant,
        price,
        canceled,
    )


def get_Trade(stock_id, date, result):
    url = "http://cdn.tsetmc.com/api/Trade/GetTradeHistory/{}/{}/false".format(
        stock_id, date
    )
    r = requests.get(url, timeout=15)
    r = json.loads(r.text)
    global sessions
    sessions += 1
    Trade = []
    for i in r["tradeHistory"]:
        Trade.append(clean_Trade(i))
    result[1] = Trade


def get_daily_data(date, stock_id):
    threads = [None] * 2
    results = [None] * 2
    for i, foo in enumerate(
        [
            get_Trade,
            get_LOB,
        ]
    ):
        threads[i] = Thread(target=foo, args=(stock_id, date, results))
        threads[i].start()
    for i in range(len(threads)):
        threads[i].join()
        # threads[i] = foo(date, stock_id, results, i)

    return results


def get_request(date, stock_id, LOBs, Trades):
    result = get_daily_data(date, stock_id)
    LOBs[date] = result[0]
    Trades[date] = result[1]


def get_stock_LOB_and_Trade_history(stock_id, dates):
    LOBs = {}
    Trades = {}
    threads = {}
    for i, date in enumerate(dates):
        step = 1
        threads[date] = Thread(target=get_request, args=(date, stock_id, LOBs, Trades))

        threads[date].start()

    for i in threads:
        threads[i].join()

    return LOBs, Trades


def function(g):
    return g.date.to_list()


def sessionLimit(number, stat):
    if stat:
        global sessions, start, totalSessions
        # print((datetime.datetime.now() - start).seconds, sessions)
        if ((datetime.datetime.now() - start).seconds < 60) and (sessions >= number):
            sleeptime = 60 - (datetime.datetime.now() - start).seconds
            print("sleep {}".format(sleeptime), sessions)
            time.sleep(sleeptime)
            start = datetime.datetime.now()
            totalSessions += sessions
            # print(totalSessions)
            sessions = 0
        elif (datetime.datetime.now() - start).seconds >= 60:
            start = datetime.datetime.now()
            totalSessions += sessions
            # print(totalSessions)
            sessions = 0


def aggregateSessions():
    global sessions, totalSessions
    return totalSessions + sessions


def get_stock_all_LOB_and_Trade(stock_id, dates, number, stat,number_days):
    i = 0
    Except = []
    # Excepted_id = 0
    all_LOB = dict.fromkeys(dates, 0)
    all_Trade = dict.fromkeys(dates, 0)
    while i < len(dates) + 1 or i != len(dates) + 1:
        j = min(number_days + i, len(dates) + 1)
        print(i, j)
        OpenConnectWait()
        LOB, Trade = get_stock_LOB_and_Trade_history(stock_id, dates[i:j])
        all_LOB.update(LOB)
        all_Trade.update(Trade)
        i = j
        if j >= len(dates):
            continue
        sessionLimit(number, stat)

    return all_LOB, all_Trade


def connect():
    host = "http://google.com"
    try:
        urllib.request.urlopen(host)  # Python 3.x
        return True
    except:
        return False


def connectSleep():
    t = datetime.datetime.now()
    while not connect():
        t = datetime.datetime.now()
        print("No internet!", t)
        time.sleep(30)


def ColseCheck():
    url = "http://tsetmc.com/Loader.aspx?ParTree=15"
    r = requests.get(url, timeout=15)
    close = True
    if len(re.findall(r"باز&nbsp", r.text)) > 0:
        close = False
    return close


def OpenConnectWait():
    connectSleep()
    t = datetime.datetime.now()
    while not ColseCheck():
        t = datetime.datetime.now()
        print("Open Market", t)
        if t.hour < 12:
            dt = datetime.datetime(t.year, t.month, t.day, 12, 0, 0, 0)
            time.sleep((dt - t).total_seconds())
        elif t.hour < 12 and t.minute >= 20:
            time.sleep(60)
        else:
            time.sleep(600)


def clean_LOB_on_Stock(all_LOB):
    df = pd.DataFrame()
    for i in all_LOB:
        t = pd.DataFrame.from_dict(all_LOB[i])
        t["date"] = i
        df = df.append(t)
    df.columns = [
        "hour",
        "minute",
        "second",
        "position",
        "bid_volume",
        "bid_quent",
        "bid_price",
        "ask_volume",
        "ask_quent",
        "ask_price",
        "date",
    ]
    return df


def clean_Trade_on_Stock(all_Trade):
    df = pd.DataFrame()
    for i in all_Trade:
        t = pd.DataFrame.from_dict(all_Trade[i])
        t["date"] = i
        df = df.append(t)
    df.columns = [
        "hour",
        "minute",
        "second",
        "position",
        "quant",
        "price",
        "canceled",
        "date",
    ]
    return df


def gen_LOB_Trade(stock_id, dates, number, path, stat,number_days):
    LOB, Trade = get_stock_all_LOB_and_Trade(stock_id, dates, number, stat,number_days)
    pickle.dump(LOB, open(path.format("LOB", "LOB_" + str(stock_id)), "wb"))
    pickle.dump(Trade, open(path.format("Trade", "Trade" + str(stock_id)), "wb"))


#%%
