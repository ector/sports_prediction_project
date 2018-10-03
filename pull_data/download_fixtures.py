import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys

from utils import get_config
from te_logger.logger import log

leagues = get_config(file="flashscore_leagues")
translation = get_config(file='team_translation')

for k in leagues.keys():
    country, league = leagues.get(k).split("_")
    translate = translation.get(k)

    url = "https://www.flashscore.com/football/{}/{}/fixtures".format(country, league)
    print(url)
    driver = webdriver.Chrome('/usr/local/bin/chromedriver')
    driver.get(url)
    time.sleep(2)
    ddt = []  # date
    dtm = []  # time
    home = []
    away = []
    result = []

    for e in driver.find_elements_by_class_name("time"):
        dte = e.text
        # 23.12.12:00
        tmp_day, tmp_mth, tm = dte.split('.')
        if int(tmp_mth) >= 7:
            tmp_yr = '2018'
        else:
            tmp_yr = '2019'
        ddt.append(tmp_yr + '-' + tmp_mth + '-' + tmp_day + ' 00:00:00')
        dtm.append(tm.strip())

    for e in driver.find_elements_by_class_name("padr"):
        home.append(e.text)
    for e in driver.find_elements_by_class_name("padl"):
        away.append(e.text)

    driver.close()

    er = pd.DataFrame(ddt, columns=["Date"])
    er["Time"] = dtm
    er["HomeTeam"] = home
    er["AwayTeam"] = away

    er["HomeTeam"].replace(translate, inplace=True)
    er["AwayTeam"].replace(translate, inplace=True)
    er.to_csv('../../tools/data/fixtures/all_fixtures/{}.csv'.format(k), index=False)
