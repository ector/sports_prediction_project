import pandas as pd
import time
from selenium import webdriver
from utils import get_config

leagues = get_config(file="flashscore_leagues")
translation = get_config(file='team_translation')

driver = webdriver.Chrome('/usr/local/bin/chromedriver')

for k in leagues.keys():
    country, league = leagues.get(k).split("_")
    translate = translation.get(k)

    url = "https://www.flashscore.com/football/{}/{}/fixtures".format(country, league)
    print(url)
    driver.get(url)
    time.sleep(2)
    ddt = []  # date
    dtm = []  # time
    home = []
    away = []
    result = []

    for e in driver.find_elements_by_class_name("event__time"):
        dte = e.text
        # 23.12.12:00
        tmp_day, tmp_mth, tm = dte.split('.')
        if int(tmp_mth) >= 7:
            tmp_yr = '2019'
        else:
            tmp_yr = '2020'
        ddt.append(tmp_yr + '-' + tmp_mth + '-' + tmp_day + ' 00:00:00')
        dtm.append(tm.strip())

    for e in driver.find_elements_by_class_name("event__participant--home"):
        home.append(e.text)
    for e in driver.find_elements_by_class_name("event__participant--away"):
        away.append(e.text)

    er = pd.DataFrame(ddt, columns=["Date"])
    er["Time"] = dtm
    er["HomeTeam"] = home
    er["AwayTeam"] = away

    er["HomeTeam"].replace(translate, inplace=True)
    er["AwayTeam"].replace(translate, inplace=True)
    er.to_csv('../data/fixtures/all_fixtures/{}.csv'.format(k), index=False)

driver.close()
