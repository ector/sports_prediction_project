import json
import os
import calendar
import pandas as pd
from datetime import date, datetime, timedelta

weekday = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']


def convert_str_fraction_to_float(s):
    """
    :param s: fraction in string
    :return: float to 2 d.p
    """
    num, denom = s.split('/')
    decimal = float(num) / float(denom)
    dp_2 = round(decimal, 2)
    return dp_2


def day_of_the_week():
    """
    Get today's date to work out what day tomorrow is
    :return: string
    """
    my_date = str(date.today())
    year, month, day = my_date.split('-')
    today = calendar.weekday(year=int(year), month=int(month), day=int(day))
    tomorrow = today + 1

    if tomorrow >= 7:
        tomorrow = 0

    return weekday[tomorrow]


def odd_price(s):
    """
    :param s: fraction in string
    :return: float odd to 2 d.p
    """
    odd = 1 + convert_str_fraction_to_float(s)
    return odd


def get_config(file="league"):
    """
    Read the football league config from a file
    :return: json
    """
    filename = "tools/config/{}.json".format(file)
    file_path = get_analysis_root_path(filepath=filename)
    config = json.loads(open(file_path).read())
    return config


def save_fixtures_to_file(data):
    """
    Read the football league config from a file
    :return: None
    """
    filename = "prototype/data/fixtures/fixtures"
    file_path = get_analysis_root_path(filepath=filename)
    fixtures = pd.DataFrame(data=data, columns=['Date', 'Time', 'HomeTeam', 'AwayTeam', 'League'])
    fixtures.to_csv(file_path, index=False)
    return


def get_analysis_root_path(filepath):
    """
    return the full path of a given path
    :type filepath: sting
    :return: string
    """
    complete_path = os.getcwd()
    root_path = os.path.join(complete_path.split('analysis')[0], 'analysis', filepath)
    return root_path


def get_start_and_end_dates(end_days=2):
    """
    Compute 8 days using today as the starts date
    :return: string list of two values
    """
    start_date = datetime.now().strftime("%Y-%m-%d")
    date_1 = datetime.strptime(start_date, "%Y-%m-%d")

    end_date = (date_1 + timedelta(days=end_days)).strftime("%Y-%m-%d")

    return start_date, end_date

if __name__ == '__main__':
    print(get_start_and_end_dates(1))
