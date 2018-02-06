# -*- coding: utf-8 -*-
"""
Created on 29-01-2018 at 12:19 PM 

@author: desco
"""

from te_logger.logger import log
from tools.games_feed.competitions import Competitions
from tools.games_feed.matches import Matches
from tools.utils import date_change


class ExtractAndManipulateData(object):
    def __init__(self):
        self.matches = Matches()
        self.log = log

    def ft_score(self, fthg, ftag):

        fthg = self.convert_to_int(fthg)
        ftag = self.convert_to_int(ftag)

        if fthg > ftag:
            return 'H'
        elif fthg < ftag:
            return 'A'
        else:
            return 'D'

    def convert_to_int(self, num_string):
        """
        Convert sting to integer
        :param num_string: string
        :return: int
        """
        try:
            num = int(float(num_string))
        except ValueError:
            num = 0
        return num

    def season_calc(self, season):
        season1 = season[2:4]
        season2 = season[-2:]
        new_season = season1 + season2
        return new_season

    def matches_extract_and_manipulate_by_id(self, comp_id):

        data = self.matches.get_all_matches(condition={"comp_id": comp_id})
        clubs = []
        for i in data:
            footy = {
                'Season': self.season_calc(season=i['season']),
                'Date': date_change(old_format=i['formatted_date']),
                'Week': i['week'],
                'HomeTeam': i['localteam_name'],
                'AwayTeam': i['visitorteam_name'],
                'FTHG': i['localteam_score'],
                'FTAG': i['visitorteam_score'],
                'FTR': self.ft_score(fthg=i['localteam_score'], ftag=i['visitorteam_score']),
                'Comp_id': i["comp_id"]
            }
            self.log.info(msg="football-api.com data: {}".format(footy))
            clubs.append(footy)

        return clubs


if __name__ == "__main__":
    emd = ExtractAndManipulateData()
    competitions = Competitions()

    for comp in competitions.get_all_competitions():
        emd.matches_extract_and_manipulate_by_id(comp_id=comp.get('id'))
