import requests
import pandas
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import smtplib
import dateutil.parser
import datetime


# ==============================================================================
# Matchbook (1.7% win/loss)
# ==============================================================================

def MK():
    url = "https://api.matchbook.com/edge/rest/events"

    querystring = {"per-page": "1000", "sport-ids": "9"}

    response = requests.request("GET", url, params=querystring)

    Z = response.json()

    match_list = []
    home_odds = []
    away_odds = []
    lay_home_odds = []
    lay_away_odds = []

    for i in Z['events']:
        if i['in-running-flag'] == False and ('vs' in i['name']):
            if '/' in i:
                doubles = True
            else:
                doubles = False
            a = i['name'].replace('.', ' ').replace('-', ' ').replace('/', ' ').split()
            b = a.index('vs')
            if doubles:
                match_list.append(a[b - 1].title() + ' v(+) ' + a[-1])
            else:
                match_list.append(a[b - 1].title() + ' v ' + a[-1])

            home_back = []
            home_lay = []

            for j in i['markets'][0]['runners'][0]['prices']:
                if j['side'] == 'back':
                    home_back.append(j['odds'])
                else:
                    home_lay.append(j['odds'])
            try:
                home_odds.append(str(max(home_back)))
            except:
                home_odds.append('1.01')
            try:
                lay_home_odds.append(str(min(home_lay)))
            except:
                lay_home_odds.append('1000')

            away_back = []
            away_lay = []

            for k in i['markets'][0]['runners'][1]['prices']:
                if k['side'] == 'back':
                    away_back.append(k['odds'])
                else:
                    away_lay.append(k['odds'])
            try:
                away_odds.append(str(max(away_back)))
            except:
                away_odds.append('1.01')
            try:
                lay_away_odds.append(str(min(away_lay)))
            except:
                lay_away_odds.append('1000')

    A = [home_odds, match_list, away_odds]
    B = pandas.DataFrame(A).transpose().drop_duplicates()
    C = [lay_home_odds, match_list, lay_away_odds]
    D = pandas.DataFrame(C).transpose().drop_duplicates()

    return A, B, C, D


# ==============================================================================
# Betfair Exchange (5% win)
# ==============================================================================

def BX():
    #    https://developers.betfair.com/visualisers/api-ng-account-operations/

    Session = ''

    Key = ''

    url = "https://api.betfair.com/exchange/betting/json-rpc/v1"

    header = {'X-Application': Key, 'X-Authentication': Session, 'content-type': 'application/json'}

    jsonrpc_req = '''
            {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listEvents",
            "params": {
                "filter": {
                    "eventTypeIds": [
                        "2"
                    ]
                    }
                }
            },
            "id": 1
            }'''

    response = requests.post(url, data=jsonrpc_req, headers=header)

    Z2 = response.json()

    event_id = {}
    names_id = {}

    for i in Z2['result']:
        if ' v ' in i['event']['name']:
            game_time = dateutil.parser.parse(i['event']['openDate']).isoformat()
            current_time = datetime.datetime.now().isoformat()
            if game_time > current_time:
                event_id[i['event']['name']] = i['event']['id']
                names_id[i['event']['id']] = i['event']['name']

    events_all = [i for i in event_id.values()]

    market_id = {}
    markeve_id = {}

    for i in range(0, len(events_all), 20):
        start = i
        if i + 20 <= len(events_all):
            end = i + 20
        else:
            end = len(events_all) - 1
        events = str(events_all[start:end]).replace('\'', '"')

        jsonrpc_req = '''
            {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listMarketCatalogue",
            "params": {
                "filter": {
                    "eventIds": ''' + events + '''},
                "maxResults": "200",
                "marketProjection": [
                    "COMPETITION",
                    "EVENT",
                    "EVENT_TYPE",
                    "RUNNER_DESCRIPTION",
                    "RUNNER_METADATA",
                    "MARKET_START_TIME"
                ]
            },
            "id": 1
            }'''

        response = requests.post(url, data=jsonrpc_req, headers=header)

        Z3 = response.json()

        for i in Z3['result']:
            if i['marketName'] == 'Match Odds':
                market_id[i['event']['id']] = i['marketId']
                markeve_id[i['marketId']] = i['event']['id']

    markets_all = [i for i in market_id.values()]

    match_list = []
    home_odds = []
    away_odds = []
    lay_home_odds = []
    lay_away_odds = []

    for i in range(0, len(markets_all), 10):
        start = i
        if i + 10 <= len(markets_all):
            end = i + 10
        else:
            end = len(markets_all) - 1

        markets = str(markets_all[start:end]).replace('\'', '"')

        jsonrpc_req = '''
                {
                    "jsonrpc": "2.0",
                    "method": "SportsAPING/v1.0/listMarketBook",
                    "params": {
                        "marketIds": ''' + markets + ''',
                        "priceProjection": {
                            "priceData": ["EX_BEST_OFFERS", "EX_TRADED"],
                            "virtualise": "true"
                        }
                    },
                    "id": 1
                }'''

        response = requests.post(url, data=jsonrpc_req, headers=header)

        Z4 = response.json()

        for i in Z4['result']:
            mar = i['marketId']
            eve = markeve_id[mar]
            nam = names_id[eve]
            if '/' in nam:
                doubles = True
            else:
                doubles = False
            a = nam.replace('\n', ' v ').replace('-', ' ').replace('/', ' ').split()
            b = a.index('v')
            if doubles:
                match_list.append(a[:b][-1].title() + ' v(+) ' + a[b + 1:][-1].title())
            else:
                match_list.append(a[:b][-1].title() + ' v ' + a[b + 1:][-1].title())

            try:
                home_odds.append(str(i['runners'][0]['ex']['availableToBack'][0]['price']))
            except:
                home_odds.append('1.01')
            try:
                away_odds.append(str(i['runners'][1]['ex']['availableToBack'][0]['price']))
            except:
                away_odds.append('1.01')
            try:
                lay_home_odds.append(str(i['runners'][0]['ex']['availableToLay'][0]['price']))
            except:
                lay_home_odds.append('1000')
            try:
                lay_away_odds.append(str(i['runners'][1]['ex']['availableToLay'][0]['price']))
            except:
                lay_away_odds.append('1000')

    A = [home_odds, match_list, away_odds]
    B = pandas.DataFrame(A).transpose().drop_duplicates()
    C = [lay_home_odds, match_list, lay_away_odds]
    D = pandas.DataFrame(C).transpose().drop_duplicates()

    return A, B, C, D


# ==============================================================================
# Skybet
# ==============================================================================

def SY():
    SY = 'https://m.skybet.com/tennis/coupon/10011159'
    r = requests.get(SY)
    soup = BeautifulSoup(r.text, 'lxml')

    home_odds = []
    match_list = []
    away_odds = []

    odds = soup.find_all('span', {'class': 'js-oc-price js-not-in-slip'})

    for i in range(len(odds)):
        a = odds[i].text.replace('\n', '').replace(' ', '')
        if i % 2 == 0:
            home_odds.append(a)
        else:
            away_odds.append(a)

    matches = soup.find_all('b', {'class': 'cell-text__line'})

    for i in matches:
        if '/' in i.text:
            doubles = True
        else:
            doubles = False
        a = i.text.replace('.', ' ').replace('-', ' ').replace('/', ' ').split()
        index = a.index('v')
        if doubles:
            match_list.append(a[index - 1].title() + ' v(+) ' + a[-1].title())
        else:
            match_list.append(a[index - 1].title() + ' v ' + a[-1].title())

    A = [home_odds, match_list, away_odds]
    B = pandas.DataFrame(A).transpose().drop_duplicates()

    return A, B


# ==============================================================================
# Bet365
# ==============================================================================

def B3():
    B3 = "https://www.google.co.uk/url?sa=t&rct=j&q=&esrc=s&source=web&cd=5&ved=0ahUKEwj92v2ClpzYAhXHI8AKHVwHAXYQjBAIQDAE&url=https%3A%2F%2Fwww.bet365.com%2Fdl%2FH&usg=AOvVaw12O2AyuBZlzReaOdSs1shD"

    browser.get(B3)

    WebDriverWait(browser, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//div[contains(text(),'Full List')]"))).click();

    time.sleep(1)

    site_element = browser.find_elements_by_xpath("//div[@class='gl-MarketGrid ']")
    site = [x.text for x in site_element]

    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    c = site[0].split('\n')

    d = []

    for i in range(len(c)):
        if c[i].split()[0] in days:
            d.append(('Day', i))
        if '/' in c[i]:
            try:
                float(c[i].replace('/', ''))
                d.append(('Odd', i))
            except:
                pass

    d1 = [d[0][1]]

    for i in range(1, len(d)):
        if d[i][0] == 'Day' and d[i - 1][0] == 'Odd':
            d1.append(d[i][1])

    e = []
    for i in range(len(d1)):
        start = d1[i]
        try:
            end = d1[i + 1]
        except:
            end = len(c)
        e.append(c[start:end])

    data = []
    for i in e:
        f = []
        g = []
        for j in i:
            if ' vs ' in j:
                f.append(j)
            if '/' in j:
                try:
                    float(j.replace('/', ''))
                    g.append(j)
                except:
                    pass
        data.append(f)
        data.append(g)

    names = []
    home_odds = []
    away_odds = []

    for i in range(0, len(data), 2):
        if 2 * len(data[i]) == len(data[i + 1]):
            for j in data[i]:
                names.append(j)
            mid = int(len(data[i + 1]) / 2)
            for k in data[i + 1][:mid]:
                home_odds.append(k)
            for l in data[i + 1][mid:]:
                away_odds.append(l)

    match_list = []

    for i in names:
        if '/' in i:
            doubles = True
        else:
            doubles = False
        a = i.replace('-', ' ').replace('/', ' ').split()
        b = a.index('vs')
        if doubles:
            match_list.append(a[b - 1].title() + ' v(+) ' + a[-1].title())
        else:
            match_list.append(a[b - 1].title() + ' v ' + a[-1].title())

    A = [home_odds, match_list, away_odds]
    B = pandas.DataFrame(A).transpose().drop_duplicates()

    return A, B


# ==============================================================================
# Arbitrage
# ==============================================================================

def comp(A, B, C):
    A_b_w = C[A[1]][0]
    A_b_l = C[A[1]][1]
    B_b_w = C[B[1]][0]
    B_b_l = C[B[1]][1]

    Home_odds = []
    Home = []
    Match = []
    Away = []
    Away_odds = []
    Ratio = []
    Return = []

    for i in range(len(A[0][1])):
        if A[0][1][i] in B[0][1]:
            j = B[0][1].index(A[0][1][i])

            Match.append(A[0][1][i])
            Home.append(A[1])
            Away.append(B[1])
            a = A[0][0][i]
            b = B[0][2][j]
            Home_odds.append(a)
            Away_odds.append(b)
            try:
                a_pro = float(a) - 1
            except:
                index = a.index('/')
                left = float(a[0:index])
                right = float(a[index + 1:])
                total = (left + right) / right
                a_pro = float(total) - 1
            try:
                b_pro = float(b) - 1
            except:
                index = b.index('/')
                left = float(b[0:index])
                right = float(b[index + 1:])
                total = (left + right) / right
                b_pro = float(total) - 1
            ratio = round((b_pro * B_b_w + B_b_l) / (a_pro * A_b_w + A_b_l), 5)
            Ratio.append(ratio)
            ret = round(((a_pro * A_b_w) * ratio - (B_b_l)) * 100 / (ratio + 1), 1)
            Return.append(ret)

            Match.append(A[0][1][i])
            Home.append(B[1])
            Away.append(A[1])
            a = A[0][2][i]
            b = B[0][0][j]
            Home_odds.append(b)
            Away_odds.append(a)
            try:
                a_pro = float(a) - 1
            except:
                index = a.index('/')
                left = float(a[0:index])
                right = float(a[index + 1:])
                total = (left + right) / right
                a_pro = float(total) - 1
            try:
                b_pro = float(b) - 1
            except:
                index = b.index('/')
                left = float(b[0:index])
                right = float(b[index + 1:])
                total = (left + right) / right
                b_pro = float(total) - 1
            ratio = round((a_pro * A_b_w + A_b_l) / (b_pro * B_b_w + B_b_l), 5)
            Ratio.append(ratio)
            ret = round(((b_pro * B_b_w) * ratio - (A_b_l)) * 100 / (ratio + 1), 1)
            Return.append(ret)

    Table = [Home_odds, Home, Match, Away, Away_odds, Ratio, Return]

    DF = pandas.DataFrame(Table).transpose().drop_duplicates()

    return DF


# ==============================================================================
# Compilation
# ==============================================================================

def compi(Bookies, Commissions):
    compi = []

    for i in range(len(Bookies)):
        for j in range(i, len(Bookies)):
            compi.append((Bookies[i], Bookies[j], Commissions))

    frames = []

    for i in compi:
        frames.append(comp(i[0], i[1], i[2]))

    DF_comp = pandas.concat(frames)

    return DF_comp.sort_values(3)


# ==============================================================================
# Back-Lay
# ==============================================================================

def Ex_comp(A, B, C):
    match = []
    side = []
    back = []
    bookie = []
    lay = []
    exchange = []
    ratio = []
    profit = []

    b_w = C[A[1]][0]
    b_l = C[A[1]][1]
    l_w = C[B[1]][2]
    l_l = C[B[1]][3]

    for i in A[0][1]:
        if i in B[0][1]:

            match.append(i)
            side.append(i.split()[0])
            bookie.append(A[1])
            exchange.append(B[1])
            back_index = A[0][1].index(i)
            a = A[0][0][back_index]
            back.append(a)
            try:
                a_pro = float(a) - 1
            except:
                index = a.index('/')
                left = float(a[0:index])
                right = float(a[index + 1:])
                total = (left + right) / right
                a_pro = float(total) - 1
            lay_index = B[0][1].index(i)
            b = B[0][0][lay_index]
            lay.append(b)
            try:
                b_pro = float(b) - 1
            except:
                index = b.index('/')
                left = float(b[0:index])
                right = float(b[index + 1:])
                total = (left + right) / right
                b_pro = float(total) - 1
            HOME = (l_w + b_pro * l_l) / (b_l + a_pro * b_w)
            ratio.append(round(HOME, 5))
            profit.append(round((l_w - HOME * b_l) / (HOME + b_pro) * 100, 1))

            match.append(i)
            side.append(i.split()[-1])
            bookie.append(A[1])
            exchange.append(B[1])
            back_index = A[0][1].index(i)
            a = A[0][2][back_index]
            back.append(a)
            try:
                a_pro = float(a) - 1
            except:
                index = a.index('/')
                left = float(a[0:index])
                right = float(a[index + 1:])
                total = (left + right) / right
                a_pro = float(total) - 1
            lay_index = B[0][1].index(i)
            b = B[0][2][lay_index]
            lay.append(b)
            try:
                b_pro = float(b) - 1
            except:
                index = b.index('/')
                left = float(b[0:index])
                right = float(b[index + 1:])
                total = (left + right) / right
                b_pro = float(total) - 1
            AWAY = (l_w + b_pro * l_l) / (b_l + a_pro * b_w)
            ratio.append(round(AWAY, 5))
            profit.append(round((l_w - AWAY * b_l) / (AWAY + b_pro) * 100, 1))

    Table = [match, back, bookie, side, exchange, lay, ratio, profit]

    DF = pandas.DataFrame(Table).transpose().drop_duplicates()

    return DF


# ==============================================================================
# Exchange Compilation
# ==============================================================================

def Ex_compi(Bookies, Exchanges, Commissions):
    Ex_compi = []

    for i in range(len(Bookies)):
        for j in range(len(Exchanges)):
            Ex_compi.append((Bookies[i], Exchanges[j], Commissions))

    frames = []

    for i in Ex_compi:
        frames.append(Ex_comp(i[0], i[1], i[2]))

    DF_Ex_comp = pandas.concat(frames)
    return DF_Ex_comp.sort_values(0)


# ==============================================================================
# Run
# ==============================================================================

begin = time.time()

browser = webdriver.Chrome("/usr/local/bin/chromedriver")

while time.time() <= begin + 30000:

    start = time.time()

    try:
        B3_table, B3_df = B3()
    except:
        print('\nB3 error')

    time1 = time.time()

    try:
        BX_table, BX_df, BX_table_lay, BX_df_lay = BX()
    except:
        print('\nBX error')

    time2 = time.time()

    try:
        SY_table, SY_df = SY()
    except:
        print('\nSY error')

    time3 = time.time()

    try:
        MK_table, MK_df, MK_table_lay, MK_df_lay = MK()
    except:
        print('\nMK error')

    end = time.time()

    # ==============================================================================
    # Display
    # ==============================================================================

    Bookies = [
        (B3_table, 'B3'),
        # (SY_table, 'SY'),
        #             (BX_table,'BX'),
        (MK_table, 'MK')
    ]

    Exchanges = [
        #             (BX_table_lay,'BX'),
        (MK_table_lay, 'MK')
    ]

    Commissions = {
        # (back_win, back_lose, lay_win, lay_lose)
        'B3': (1, 1, 1, 1),
        'SY': (1, 1, 1, 1),
        'BX': (0.949, 1, 0.949, 1),
        'MK': (0.983, 1.017, 0.983, 1.017)
    }

    Data = compi(Bookies, Commissions)
    Data.columns = ['[BK]', '[HOME]', '[MATCH]', '[AWAY]', '[BK]', '[RATIO]', '[PROFIT]']

    Ex_Data = Ex_compi(Bookies, Exchanges, Commissions)
    Ex_Data.columns = ['[MATCH]', '[BK]', '[BACK]', '[SIDE]', '[LAY]', '[LY]', '[RATIO]', '[PROFIT]']

    pandas.set_option('display.max_columns', 500)
    pandas.set_option('display.width', 1000)

    if Data[Data['[PROFIT]'] > 0].empty == False:
        print('--------------------------------------------------------------------------------------')
        print(Data[Data['[PROFIT]'] > 0].sort_values('[PROFIT]', ascending=0))

    if Ex_Data[Ex_Data['[PROFIT]'] > 0].empty == False:
        print('--------------------------------------------------------------------------------------')
        print(Ex_Data[Ex_Data['[PROFIT]'] > 0].sort_values('[PROFIT]', ascending=0))

    limit = 1.5

    password = "Northfleet1!"

    if (not Ex_Data[Ex_Data['[PROFIT]'] > limit].empty or not Data[Data['[PROFIT]'] > limit].empty) == True:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login("infoamolexis@gmail.com", password)
        msg = (
                "BACK\n" +
                str(Data[Data['[PROFIT]'] > limit].sort_values('[PROFIT]', ascending=0)) +
                "\n\nLAY\n" +
                str(Ex_Data[Ex_Data['[PROFIT]'] > limit].sort_values('[PROFIT]', ascending=0))
        )
        server.sendmail("infoamolexis@gmail.com", "infoamolexis@gmail.com", msg)
        server.quit()

    print('--------------------------------------------------------------------------------------')
    print('Time: ' + time.strftime('%H:%M'))
    print('Running time: ' + str(round(end - start, 1)) + 's')
    print('B3: ' + str(round(time1 - start, 1)) + 's')
    print('BX: ' + str(round(time2 - time1, 1)) + 's')
    print('SY: ' + str(round(time3 - time2, 1)) + 's')
    print('MK: ' + str(round(end - time3, 1)) + 's')

browser.quit()
