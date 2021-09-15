#%%
import warnings

warnings.filterwarnings("ignore")
import re

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
def date_of_stocks(df, y):
    t = int(y) + 621
    t = int(str(t) + "0325")
    df["date"] = df.date.astype(int)
    gg = df[(df["date"] < t + 10000) & (df["date"] > t)][["date", "stock_id"]].groupby(
        "stock_id"
    )

    def function(g):
        return g.date.to_list()

    date = gg.apply(function).to_dict()
    print("date be done")
    dfname = df[(df["date"] < t + 10000) & (df["date"] > t)][
        [
            "jalaliDate",
            "date",
            "name",
            "stock_id",
            "group_name",
            "close_price",
            # "group_id",
        ]
    ]
    return date, dfname


def text_num_split(item):
    for index, letter in enumerate(item, 0):
        if letter.isdigit():
            return [item[:index], item[index:]]


def removecomma(m):
    m = m.split(",")
    try:
        m = int(m[0] + m[1])
    except:
        m = int(m[0])
    return m


def getHolder(date, stock_id, result, j):
    url = "http://cdn.tsetmc.com/api/Shareholder/{}/{}".format(stock_id, date)
    r = requests.get(url, timeout=15)
    global sessions
    sessions += 1
    r = json.loads(r.text)
    holder = []
    for i in r["shareShareholder"]:
        if i["dEven"] > date:
            holder.append(i)
    result[j] = holder


def getStockDetail(date, stock_id, result, i):
    url = "http://cdn.tsetmc.com/api/Instrument/GetInstrumentHistory/{}/{}".format(
        stock_id, date
    )
    try:
        r = requests.get(url, timeout=15)
        global sessions
        sessions += 1
        r = json.loads(r.text)
        result[i] = r
    except:
        result[i] = "-"


def getStockTrade(date, stock_id, result, i):
    url = "http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{}/{}".format(
        stock_id, date
    )
    try:
        r = requests.get(url, timeout=15)
        global sessions
        sessions += 1
        r = json.loads(r.text)
        result[i] = r
    except:
        result[i] = "-"


def getMaxMin(date, stock_id, result, i):
    url = "http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{}/{}".format(
        stock_id, date
    )
    try:
        r = requests.get(url, timeout=15)
        global sessions
        sessions += 1
        r = json.loads(r.text)
        result[i] = (
            r["staticThreshold"][-1]["psGelStaMax"],
            r["staticThreshold"][-1]["psGelStaMin"],
        )
    except:
        result[i] = "-"


def getPrice(date, stock_id, result, i):
    url = "http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDaily/{}/{}".format(
        stock_id, date
    )
    try:
        r = requests.get(url, timeout=15)
        global sessions
        sessions += 1
        r = json.loads(r.text)
        result[i] = r
    except:
        result[i] = "-"


def get_daily_data(date, stock_id):
    threads = [None] * 5
    results = [None] * 5
    for i, foo in enumerate(
        [getHolder, getStockDetail, getPrice, getMaxMin, getStockTrade]
    ):
        threads[i] = Thread(target=foo, args=(date, stock_id, results, i))
        threads[i].start()
    for i in range(len(threads)):
        threads[i].join()
        # threads[i] = foo(date, stock_id, results, i)

    return results


def generate_holder(result):
    Firm = result[1]["instrumentHistory"]["lVal30"]
    name = result[1]["instrumentHistory"]["lVal18AFC"]
    shrout = result[1]["instrumentHistory"]["zTitad"]
    basevalue = result[1]["instrumentHistory"]["baseVol"]
    market = result[1]["instrumentHistory"]["flowTitle"]
    pricechange = result[2]["closingPriceDaily"]["priceChange"]
    pricechange = result[2]["closingPriceDaily"]["priceChange"]
    priceMin = result[2]["closingPriceDaily"]["priceMin"]
    priceMax = result[2]["closingPriceDaily"]["priceMax"]
    priceYesterday = result[2]["closingPriceDaily"]["priceYesterday"]
    priceFirst = result[2]["closingPriceDaily"]["priceFirst"]
    stock_id = result[2]["closingPriceDaily"]["insCode"]
    close_price = result[2]["closingPriceDaily"]["pClosing"]
    last_price = result[2]["closingPriceDaily"]["pDrCotVal"]
    count = result[2]["closingPriceDaily"]["zTotTran"]
    volume = result[2]["closingPriceDaily"]["qTotTran5J"]
    value = result[2]["closingPriceDaily"]["qTotCap"]
    try:
        max = result[3][0]
    except:
        max = ""
    try:
        min = result[3][1]
    except:
        min = ""
    try:
        ind_buy_volume = result[4]["clientType"]["buy_I_Volume"]
    except:
        ind_buy_volume = "-"
    try:
        ins_buy_volume = result[4]["clientType"]["buy_N_Volume"]
    except:
        ins_buy_volume = "-"
    try:
        ind_buy_value = result[4]["clientType"]["buy_I_Value"]
    except:
        ind_buy_value = "-"
    try:
        ins_buy_value = result[4]["clientType"]["buy_N_Value"]
    except:
        ins_buy_value = "-"
    try:
        ins_buy_count = result[4]["clientType"]["buy_N_Count"]
    except:
        ins_buy_count = "-"
    try:
        ind_buy_count = result[4]["clientType"]["buy_I_Count"]
    except:
        ind_buy_count = "-"
    try:
        ind_sell_volume = result[4]["clientType"]["sell_I_Volume"]
    except:
        ind_sell_volume = "-"
    try:
        ins_sell_volume = result[4]["clientType"]["sell_N_Volume"]
    except:
        ins_sell_volume = "-"
    try:
        ind_sell_value = result[4]["clientType"]["sell_I_Value"]
    except:
        ind_sell_value = "-"
    try:
        ins_sell_value = result[4]["clientType"]["sell_N_Value"]
    except:
        ins_sell_value = "-"
    try:
        ins_sell_count = result[4]["clientType"]["sell_N_Count"]
    except:
        ins_sell_count = "-"
    try:
        ind_sell_count = result[4]["clientType"]["sell_I_Count"]
    except:
        ind_sell_count = "-"
    shareholder = {}
    if result[0] != []:
        for i in result[0]:
            shareholder[i["shareHolderID"]] = (
                i["shareHolderName"],
                i["numberOfShares"],
                i["perOfShares"],
                i["change"],
                i["changeAmount"],
                Firm,
                name,
                shrout,
                basevalue,
                market,
                pricechange,
                priceMin,
                priceMax,
                priceYesterday,
                priceFirst,
                stock_id,
                close_price,
                last_price,
                count,
                volume,
                value,
                max,
                min,
                ind_buy_volume,
                ins_buy_volume,
                ind_buy_value,
                ins_buy_value,
                ins_buy_count,
                ind_buy_count,
                ind_sell_volume,
                ins_sell_volume,
                ind_sell_value,
                ins_sell_value,
                ins_sell_count,
                ind_sell_count,
            )
    else:
        shareholder[13731126] = (
            "-",
            "-",
            "-",
            "-",
            "-",
            Firm,
            name,
            shrout,
            basevalue,
            market,
            pricechange,
            priceMin,
            priceMax,
            priceYesterday,
            priceFirst,
            stock_id,
            close_price,
            last_price,
            count,
            volume,
            value,
            max,
            min,
            ind_buy_volume,
            ins_buy_volume,
            ind_buy_value,
            ins_buy_value,
            ins_buy_count,
            ind_buy_count,
            ind_sell_volume,
            ins_sell_volume,
            ind_sell_value,
            ins_sell_value,
            ins_sell_count,
            ind_sell_count,
        )

    return shareholder


def get_request(date, stock_id, dates, i, Excepted, history, step):
    result = get_daily_data(date, stock_id)
    holder = generate_holder(result)
    if holder is None:
        Excepted.append(dates[i])
    else:
        history[dates[i]] = holder
    return history, Excepted
    # except:
    #     history, Excepted = get_request(url,dates,i,Excepted,history,step)


def get_stock_holder_history(stock_id, dates):
    history = {}
    Excepted = []

    threads = {}
    for i, date in enumerate(dates):
        step = 1
        # print(i, date, end="\r", flush=True)
        threads[date] = Thread(
            target=get_request, args=(date, stock_id, dates, i, Excepted, history, step)
        )
        # OpenConnectWait()

        threads[date].start()

    for i in threads:
        threads[i].join()

    # for i, date in enumerate(dates):
    #     OpenConnectWait()
    #     print(i, date, end="\r", flush=True)
    #     step = 1
    #     history, Excepted = get_request(
    #         date, stock_id, dates, i, Excepted, history, step
    #     )
    return history, Excepted


def sessionLimit(number):
    global sessions, start, totalSessions
    # print((datetime.datetime.now() - start).seconds, sessions)
    if ((datetime.datetime.now() - start).seconds < 60) and (sessions >= number):
        sleeptime = 60 - (datetime.datetime.now() - start).seconds
        print("sleep {}".format(sleeptime))
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


def get_stock_all_history(stock_id, dates, number):
    i = 0
    Except = []
    # Excepted_id = 0
    all_history = dict.fromkeys(dates, 0)
    while i < len(dates) + 1 or i != len(dates) + 1:
        j = min(number / 5 + i, len(dates) + 1)
        # print(i,j)
        OpenConnectWait()
        holder_history, Excepted = get_stock_holder_history(stock_id, dates[i:j])
        Except.append(Excepted)
        all_history.update(holder_history)
        i = j
        if j >= len(dates):
            continue
        sessionLimit(number)

    # step = 0
    # while excepted_again != [] and step < 2:
    #     step += 1
    #     holder_history, Excepted = get_stock_holder_history(stock_id, excepted_again)

    #     excepted_again = [x for x in Excepted]

    #     all_history.update(holder_history)

    # if step == 2 and excepted_again != []:
    #     print("Can't conect to this %s id" % stock_id, len(excepted_again) ,end='\r',flush=True)

    delete = [key for (key, value) in all_history.items() if value == 0]
    excepted_again = [x for x in Excepted]
    excepted_again = excepted_again + delete

    for i in delete:
        del all_history[i]
    return all_history, excepted_again


def sort_stock_holders_history(id, holder):
    stock = agregate_stock_holders_history(holder)
    df = pd.DataFrame.from_dict(stock).sort_values(by=["date"])
    df["id"] = id
    return df.to_dict()


def agregate_stock_holders_history(holder):
    Holder = []
    Holder_id = []
    date = []
    Value = []
    Percent = []
    Change = []
    ChangeAmount = []
    Firm = []
    name = []
    shrout = []
    basevalue = []
    market = []
    pricechange = []
    priceMin = []
    priceMax = []
    priceYesterday = []
    priceFirst = []
    stock_id = []
    close_price = []
    last_price = []
    count = []
    volume = []
    value = []
    max = []
    min = []
    ind_buy_volume = []
    ins_buy_volume = []
    ind_buy_value = []
    ins_buy_value = []
    ins_buy_count = []
    ind_buy_count = []
    ind_sell_volume = []
    ins_sell_volume = []
    ind_sell_value = []
    ins_sell_value = []
    ins_sell_count = []
    ind_sell_count = []
    stock = {}
    for day in holder:
        for holder_id in holder[day]:
            Holder.append(holder[day][holder_id][0])
            Holder_id.append(int(holder_id))
            date.append(str(day))
            Value.append(holder[day][holder_id][1])
            Percent.append(holder[day][holder_id][2])
            Change.append(holder[day][holder_id][3])
            ChangeAmount.append(holder[day][holder_id][4])
            Firm.append(holder[day][holder_id][5])
            name.append(holder[day][holder_id][6])
            shrout.append(holder[day][holder_id][7])
            basevalue.append(holder[day][holder_id][8])
            market.append(holder[day][holder_id][9])
            pricechange.append(holder[day][holder_id][10])
            priceMin.append(holder[day][holder_id][11])
            priceMax.append(holder[day][holder_id][12])
            priceYesterday.append(holder[day][holder_id][13])
            priceFirst.append(holder[day][holder_id][14])
            stock_id.append(holder[day][holder_id][15])
            close_price.append(holder[day][holder_id][16])
            last_price.append(holder[day][holder_id][17])
            count.append(holder[day][holder_id][18])
            volume.append(holder[day][holder_id][19])
            value.append(holder[day][holder_id][20])
            max.append(holder[day][holder_id][21])
            min.append(holder[day][holder_id][22])
            ind_buy_volume.append(holder[day][holder_id][23])
            ins_buy_volume.append(holder[day][holder_id][24])
            ind_buy_value.append(holder[day][holder_id][25])
            ins_buy_value.append(holder[day][holder_id][26])
            ins_buy_count.append(holder[day][holder_id][27])
            ind_buy_count.append(holder[day][holder_id][28])
            ind_sell_volume.append(holder[day][holder_id][29])
            ins_sell_volume.append(holder[day][holder_id][30])
            ind_sell_value.append(holder[day][holder_id][31])
            ins_sell_value.append(holder[day][holder_id][32])
            ins_sell_count.append(holder[day][holder_id][33])
            ind_sell_count.append(holder[day][holder_id][34])

    stock["Holder"] = Holder
    stock["Holder_id"] = Holder_id
    stock["date"] = [int(x) for x in date]
    stock["Value"] = Value
    stock["Percent"] = Percent
    stock["jalaliDate"] = [
        JalaliDate.to_jalali(int(item[0:4]), int(item[4:6]), int(item[6:8])).isoformat()
        for item in date
    ]
    stock["Holder"] = Holder
    stock["Holder_id"] = Holder_id
    stock["Number"] = Value
    stock["Percent"] = Percent
    stock["Change"] = Change
    stock["ChangeAmount"] = ChangeAmount
    stock["Firm"] = Firm
    stock["name"] = name
    stock["shrout"] = shrout
    stock["basevalue"] = basevalue
    stock["market"] = market
    stock["pricechange"] = pricechange
    stock["priceMin"] = priceMin
    stock["priceMax"] = priceMax
    stock["priceYesterday"] = priceYesterday
    stock["priceFirst"] = priceFirst
    stock["stock_id"] = stock_id
    stock["close_price"] = close_price
    stock["last_price"] = last_price
    stock["count"] = count
    stock["volume"] = volume
    stock["value"] = value
    stock["max"] = max
    stock["min"] = min
    stock["ind_buy_volume"] = ind_buy_volume
    stock["ins_buy_volume"] = ins_buy_volume
    stock["ind_buy_value"] = ind_buy_value
    stock["ins_buy_value"] = ins_buy_value
    stock["ins_buy_count"] = ins_buy_count
    stock["ind_buy_count"] = ind_buy_count
    stock["ind_sell_volume"] = ind_sell_volume
    stock["ins_sell_volume"] = ins_sell_volume
    stock["ind_sell_value"] = ind_sell_value
    stock["ins_sell_value"] = ins_sell_value
    stock["ins_sell_count"] = ins_sell_count
    stock["ind_sell_count"] = ind_sell_count
    return stock


def cleaning(all_stock_data):
    DATES = []
    JALALI = []
    Holder = []
    Holder_id = []
    date = []
    Value = []
    Percent = []
    ID = []
    Holder = []
    Holder_id = []
    date = []
    Value = []
    Percent = []
    Change = []
    ChangeAmount = []
    Firm = []
    name = []
    shrout = []
    basevalue = []
    market = []
    pricechange = []
    priceMin = []
    priceMax = []
    priceYesterday = []
    priceFirst = []
    stock_id = []
    close_price = []
    last_price = []
    count = []
    volume = []
    value = []
    max = []
    min = []
    ind_buy_volume = []
    ins_buy_volume = []
    ind_buy_value = []
    ins_buy_value = []
    ins_buy_count = []
    ind_buy_count = []
    ind_sell_volume = []
    ins_sell_volume = []
    ind_sell_value = []
    ins_sell_value = []
    ins_sell_count = []
    ind_sell_count = []

    stocks = pd.DataFrame()
    for item in all_stock_data:
        h = item["holder_history"]
        DATES = DATES + list(h["date"].values())
        JALALI = JALALI + list(h["jalaliDate"].values())
        Holder = Holder + list(h["Holder"].values())
        Holder_id = Holder_id + list(h["Holder_id"].values())
        Value = Value + list(h["Value"].values())
        Percent = Percent + list(h["Percent"].values())
        Change = Change + list(h["Change"].values())
        ChangeAmount = ChangeAmount + list(h["ChangeAmount"].values())
        Firm = Firm + list(h["Firm"].values())
        name = name + list(h["name"].values())
        shrout = shrout + list(h["shrout"].values())
        basevalue = basevalue + list(h["basevalue"].values())
        market = market + list(h["market"].values())
        pricechange = pricechange + list(h["pricechange"].values())
        priceMin = priceMin + list(h["priceMin"].values())
        priceMax = priceMax + list(h["priceMax"].values())
        priceYesterday = priceYesterday + list(h["priceYesterday"].values())
        priceFirst = priceFirst + list(h["priceFirst"].values())
        stock_id = stock_id + list(h["stock_id"].values())
        close_price = close_price + list(h["close_price"].values())
        last_price = last_price + list(h["last_price"].values())
        count = count + list(h["count"].values())
        volume = volume + list(h["volume"].values())
        value = value + list(h["value"].values())
        max = max + list(h["max"].values())
        min = min + list(h["min"].values())
        ind_buy_volume = ind_buy_volume + list(h["ind_buy_volume"].values())
        ins_buy_volume = ins_buy_volume + list(h["ins_buy_volume"].values())
        ind_buy_value = ind_buy_value + list(h["ind_buy_value"].values())
        ins_buy_value = ins_buy_value + list(h["ins_buy_value"].values())
        ins_buy_count = ins_buy_count + list(h["ins_buy_count"].values())
        ind_buy_count = ind_buy_count + list(h["ind_buy_count"].values())
        ind_sell_volume = ind_sell_volume + list(h["ind_sell_volume"].values())
        ins_sell_volume = ins_sell_volume + list(h["ins_sell_volume"].values())
        ind_sell_value = ind_sell_value + list(h["ind_sell_value"].values())
        ins_sell_value = ins_sell_value + list(h["ins_sell_value"].values())
        ins_sell_count = ins_sell_count + list(h["ins_sell_count"].values())
        ind_sell_count = ind_sell_count + list(h["ind_sell_count"].values())

    stocks["jalaliDate"] = JALALI
    stocks["date"] = DATES
    stocks["Holder"] = Holder
    stocks["Holder_id"] = Holder_id
    stocks["Number"] = Value
    stocks["Percent"] = Percent
    stocks["Change"] = Change
    stocks["ChangeAmount"] = ChangeAmount
    stocks["Firm"] = Firm
    stocks["name"] = name
    stocks["shrout"] = shrout
    stocks["basevalue"] = basevalue
    stocks["market"] = market
    stocks["pricechange"] = pricechange
    stocks["priceMin"] = priceMin
    stocks["priceMax"] = priceMax
    stocks["priceYesterday"] = priceYesterday
    stocks["priceFirst"] = priceFirst
    stocks["stock_id"] = stock_id
    stocks["close_price"] = close_price
    stocks["last_price"] = last_price
    stocks["count"] = count
    stocks["volume"] = volume
    stocks["value"] = value
    stocks["max"] = max
    stocks["min"] = min
    stocks["ind_buy_volume"] = ind_buy_volume
    stocks["ins_buy_volume"] = ins_buy_volume
    stocks["ind_buy_value"] = ind_buy_value
    stocks["ins_buy_value"] = ins_buy_value
    stocks["ins_buy_count"] = ins_buy_count
    stocks["ind_buy_count"] = ind_buy_count
    stocks["ind_sell_volume"] = ind_sell_volume
    stocks["ins_sell_volume"] = ins_sell_volume
    stocks["ind_sell_value"] = ind_sell_value
    stocks["ins_sell_value"] = ins_sell_value
    stocks["ins_sell_count"] = ins_sell_count
    stocks["ind_sell_count"] = ind_sell_count
    return stocks


#%%
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
    # while not ColseCheck():
    #     t = datetime.datetime.now()
    #     print("Open Market", t)
    #     if t.hour < 12:
    #         dt = datetime.datetime(t.year, t.month, t.day, 12, 0, 0, 0)
    #         time.sleep((dt - t).total_seconds())
    #     elif t.hour < 12 and t.minute >= 20:
    #         time.sleep(60)
    #     else:
    #         time.sleep(600)


def mainCrawl(counter, stock_id, dates, Excepted_stock, number):
    now = datetime.datetime.now()
    holder = {}
    print(
        "Parsed stock count",
        counter,
        stock_id,
        now.hour,
        now.weekday(),
        len(dates[stock_id]),
        end="\n",
        flush=True,
    )  # printing number of stocks parsed
    excepted_again = []
    holder, excepted_again = get_stock_all_history(stock_id, dates[stock_id], number)
    if excepted_again != []:
        step = 0
        while excepted_again != [] and step < 5:
            step += 1
            # print("step is ", step, stock_id, len(excepted_again), end="\r", flush=True)
            holder2, excepted_again = get_stock_all_history(
                stock_id, excepted_again, number
            )
            holder.update(holder2)
            excepted_again = list(set(excepted_again) - set(holder.keys()))
        if excepted_again != []:
            # print(
            #     "Excepted stock with id of %s " % stock_id,
            #     len(excepted_again),
            #     end="\r",
            #     flush=True,
            # )
            Excepted_stock.append((stock_id, excepted_again))
    # print(len(Excepted_stock), len(holder.keys()), end="\r", flush=True)
    return holder, Excepted_stock


def Main(counter, stock_id, dates, Excepted_stock, result, number):
    stock = {}
    holder, Excepted_stock = mainCrawl(counter, stock_id, dates, Excepted_stock, number)
    stock["holder_history"] = sort_stock_holders_history(stock_id, holder)
    result[stock_id] = stock


def Main2(counter, stock_id, dates, Excepted_stock, result, number):
    stock = {}
    holder, Excepted_stock = mainCrawl(counter, stock_id, dates, Excepted_stock, number)
    stock["holder_history"] = sort_stock_holders_history(stock_id, holder)
    return stock


# %%
def clean(all_stock_data, dfname):
    stocks = cleaning(all_stock_data)
    # stocks = pd.merge(
    #     stocks, dfname, left_on=["stock_id", "date"], right_on=["stock_id", "date"]
    # )
    # stocks["jalaliDate"] = stocks["jalaliDate_x"]
    # stocks = stocks.drop(columns=["jalaliDate_y", "jalaliDate_x"])
    return stocks
