from time import time

import pandas as pd
import numpy as np
from multiprocessing import Pool, Process
from te_logger.logger import MyLogger
from tools.home_draw_away_suite import DeriveFootballFeatures
from tools.utils import get_analysis_root_path, get_config

leagues = {
    'england_premiership': 'E0',
    'england_championship': 'E1',
    'england_league1': 'E2',
    'england_league2': 'E3',

    'scotland_premiership': 'SC0',
    'scotland_championship': 'SC1',
    'scotland_league1': 'SC2',
    'scotland_league2': 'SC3',

    'germany_bundesliga': 'D1',
    'germany_bundesliga2': 'D2',

    'italy_serie_a': 'I1',
    'italy_serie_b': 'I2',

    'spain_la_liga_premera': 'SP1',
    'spain_la_liga_segunda': 'SP2',

    'france_le_championnat': 'F1',
    'france_division_2': 'F2',

    'netherlands_eredivisie': 'N1',

    'belgium_jupiler.csv': 'B1',

    'portugal_liga_1': 'P1',

    'turkey_futbol_ligi_1': 'T1',

    'greece_ethniki_katigoria': 'G1',
}


class CleanProcessStore(MyLogger):
    def __init__(self):
        self.football_data = None
        self.raw_data_directory = get_analysis_root_path('prototype/data/raw_data/{}.csv')
        self.clean_last_win_data_directory = get_analysis_root_path('prototype/data/clean_data/last_win/{}.csv')
        self.clean_team_trend_data_directory = get_analysis_root_path('prototype/data/clean_data/team_trend/{}.csv')
        self.column_dict = None
        self.team_mapping = None
        self.league = None
        self.home_draw_away_suite = DeriveFootballFeatures()
        MyLogger.logger(self)

    def win_loss(self, array_list, team):
        """
        :param array_list: Last game result 
        :param team: team id
        :return: 3 or 1 or 0
        """
        self.log.info("Team: {}".format(team))
        self.log.info("Last data: {}".format(array_list))

        if team == array_list[self.column_dict.get('AwayTeam')]:
            if array_list[self.column_dict.get('FTR')] == 'H':
                return 0
            elif array_list[self.column_dict.get('FTR')] == 'A':
                return 3
            else:
                return 1
        if team == array_list[self.column_dict.get('HomeTeam')]:
            if array_list[self.column_dict.get('FTR')] == 'H':
                return 3
            elif array_list[self.column_dict.get('FTR')] == 'A':
                return 0
            else:
                return 1

    def last_games_trend(self, array_list, team):
        """
        Calculate the last n games performance, n being the length of the array_list
        :param array_list: Last n game trend 
        :param team: team id
        :return: dataframe
        """
        self.log.info("Team: {}".format(team))
        self.log.info("Last n data: {}".format(array_list))

        teams_trend = 0
        for team_row in array_list:
            if isinstance(team_row, list):
                if team == team_row[self.column_dict.get('AwayTeam')] or team == team_row[self.column_dict.get('AwayTeam')]:
                    teams_trend += team_row[self.column_dict.get('AwayLastWin')]
                if team == team_row[self.column_dict.get('HomeTeam')] or team == team_row[self.column_dict.get('HomeTeam')]:
                    teams_trend += team_row[self.column_dict.get('HomeLastWin')]
        return teams_trend

    def team_games_trend(self, array_list, team):
        """
        Calculate the last n games performance, n being the length of the array_list
        """
        trend = ""
        for team_row in array_list:
            if isinstance(team_row, list):
                if team is team_row[self.column_dict.get('AwayTeam')] or team == team_row[self.column_dict.get('AwayTeam')]:
                    trend += team_row[self.column_dict.get('AwayLastTrend')]
                if team is team_row[self.column_dict.get('HomeTeam')] or team == team_row[self.column_dict.get('HomeTeam')]:
                    trend += team_row[self.column_dict.get('HomeLastTrend')]
        return trend

    def team_ft_ave_goals(self, array_list, team):
        """
        Calculate average goals scored by the team
        """
        ave_list = []
        for team_row in array_list:
            if isinstance(team_row, list):
                if team is team_row[self.column_dict.get('AwayTeam')] or team == team_row[self.column_dict.get('AwayTeam')]:
                    ave_list.append(team_row[self.column_dict.get('FTAG')])
                if team is team_row[self.column_dict.get('HomeTeam')] or team == team_row[self.column_dict.get('HomeTeam')]:
                    ave_list.append(team_row[self.column_dict.get('FTHG')])
        ave = float(np.mean(ave_list))
        return ave

    def team_ft_ave_goals_concided(self, array_list, team):
        """
        Calculate average goals concided by the team
        """
        ave_cd_list = []
        for team_row in array_list:
            if isinstance(team_row, list):
                if team is team_row[self.column_dict.get('AwayTeam')] or team == team_row[self.column_dict.get('AwayTeam')]:
                    ave_cd_list.append(team_row[self.column_dict.get('FTHG')])
                if team is team_row[self.column_dict.get('HomeTeam')] or team == team_row[self.column_dict.get('HomeTeam')]:
                    ave_cd_list.append(team_row[self.column_dict.get('FTAG')])
        ave = float(np.mean(ave_cd_list))
        return ave

    def clean_football_data(self, league):
        """
        :param league: name of league
        :return: cleaned dataframe
        """
        self.league = league

        data = pd.read_csv(self.raw_data_directory.format(league), usecols=['HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'Date', 'Season'])

        data = data.dropna(how='any')
        # Making map with team names

        # Team mapping
        self.team_mapping = self.home_draw_away_suite.encode_teams(data=data)
        data = self.home_draw_away_suite.home_and_away_team_mapper(data=data, mapper=self.team_mapping)

        # Create new attributes HomeLastWin and AwayLastWin
        data['HomeLastWin'] = 0
        data['AwayLastWin'] = 0

        # Create keys and values for columns
        self.column_dict = {value: key for key, value in enumerate(list(data.columns))}
        # self.log.info("Column dict: \n{}".format(self.column_dict))

        # Copy football data
        df = data

        # Convert data to numpy array
        df_new = df.values.tolist()

        # Set homelastwin and AwayLastWin
        for key in self.team_mapping.keys():
            last_row = [0]
            team_name = self.team_mapping.get(key)

            for idx, df_row in enumerate(df_new):

                if (idx != 0) & ((team_name is int(df_row[self.column_dict.get('AwayTeam')])) | (
                    team_name is int(df_row[self.column_dict.get('HomeTeam')]))):
                    df_list = df_row

                    prev_row = last_row[-1]

                    if prev_row != 0:
                        prev_row_data = df_new[prev_row]
                        if team_name is df_list[self.column_dict.get('AwayTeam')]:
                            df_list[self.column_dict.get('AwayLastWin')] = self.win_loss(prev_row_data, team_name)
                        else:
                            df_list[self.column_dict.get('HomeLastWin')] = self.win_loss(prev_row_data, team_name)

                        data.iloc[idx] = df_list
                    last_row.append(idx)
            # self.log.info("home and away last wins completed for {} :- {}".format(league, key))

        # Inverse team mapping
        inverse_team_mapping = self.home_draw_away_suite.decode_teams(team_mapping=self.team_mapping)
        data = self.home_draw_away_suite.home_and_away_team_mapper(data=data, mapper=inverse_team_mapping,
                                                                   info="inverse")

        # self.log.info("{} HomeLastWin and AwayLastWin completed".format(league))
        data.to_csv(self.clean_last_win_data_directory.format(self.league), index=False)
        return data

    def compute_teams_trend(self, league):
        """
        :param league: string
        :param data: data with HomeLastWin and AwayLastWin
        :return: cleaned dataframe with team's trends
        """
        self.league = league
        data = pd.read_csv(self.clean_last_win_data_directory.format(league),
                           usecols=['HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'Date', 'Season', 'HomeLastWin',
                                    'AwayLastWin'])

        self.team_mapping = self.home_draw_away_suite.encode_teams(data=data)
        data = self.home_draw_away_suite.home_and_away_team_mapper(data=data, mapper=self.team_mapping)

        trend_mapping = {0: "L", 1: "D", 3: "W"}
        data['AwayLastTrend'] = data.AwayLastWin.map(trend_mapping)
        data['HomeLastTrend'] = data.HomeLastWin.map(trend_mapping)

        data['HomeLast3Games'] = 0
        data['AwayLast3Games'] = 0
        data['HomeLast5Games'] = 0
        data['AwayLast5Games'] = 0
        data['HomeTrend'] = 0
        data['AwayTrend'] = 0
        data['HomeAveG'] = 0
        data['AwayAveG'] = 0
        data['HomeAveGC'] = 0
        data['AwayAveGC'] = 0

        # Create keys and values for columns
        self.column_dict = {value: key for key, value in enumerate(list(data.columns))}
        self.log.info("Column dict: \n{}".format(self.column_dict))

        bel_data = data
        df_to_list = bel_data.values.tolist()
        for key in self.team_mapping.keys():
            last_row = []
            team_name = self.team_mapping.get(key)
            for idx, df_row in enumerate(df_to_list):
                if (idx != 0) & ((team_name is int(df_row[self.column_dict.get('AwayTeam')])) | (
                            team_name is int(df_row[self.column_dict.get('HomeTeam')]))):
                    df_list = df_row
                    #             print('current row: ', idx)

                    if len(last_row) != 0:
                        prev_rows = last_row[-5:]

                        prev_five_rows_data = [df_to_list[i] for i in prev_rows]
                        prev_three_rows_data = prev_five_rows_data[-3:]

                        if team_name is df_list[self.column_dict.get('AwayTeam')]:
                            df_list[self.column_dict.get('AwayLast5Games')] = self.last_games_trend(prev_five_rows_data,
                                                                                                    team_name)
                            df_list[self.column_dict.get('AwayLast3Games')] = self.last_games_trend(
                                prev_three_rows_data,
                                team_name)
                            df_list[self.column_dict.get('AwayTrend')] = self.team_games_trend(prev_five_rows_data,
                                                                                               team_name)
                            df_list[self.column_dict.get('AwayAveG')] = self.team_ft_ave_goals(prev_five_rows_data,
                                                                                               team_name)
                            df_list[self.column_dict.get('AwayAveGC')] = self.team_ft_ave_goals_concided(prev_five_rows_data,
                                                                                               team_name)
                        else:
                            df_list[self.column_dict.get('HomeLast5Games')] = self.last_games_trend(prev_five_rows_data,
                                                                                                    team_name)
                            df_list[self.column_dict.get('HomeLast3Games')] = self.last_games_trend(
                                prev_three_rows_data,
                                team_name)
                            df_list[self.column_dict.get('HomeTrend')] = self.team_games_trend(prev_five_rows_data,
                                                                                               team_name)
                            df_list[self.column_dict.get('HomeAveG')] = self.team_ft_ave_goals(prev_five_rows_data,
                                                                                              team_name)
                            df_list[self.column_dict.get('HomeAveGC')] = self.team_ft_ave_goals_concided(prev_five_rows_data,
                                                                                               team_name)

                        data.iloc[idx] = df_list
                    last_row.append(idx)
            self.log.info("home and away last 3 and 5 games completed for {} :- {}".format(self.league, key))

        self.log.info(
            "{} HomeLast3Games and AwayLast3Games, HomeLast5Games and AwayLast5Games completed".format(self.league))

        # Inverse team mapping
        inverse_team_mapping = self.home_draw_away_suite.decode_teams(team_mapping=self.team_mapping)
        data = self.home_draw_away_suite.home_and_away_team_mapper(data=data, mapper=inverse_team_mapping,
                                                                   info="inverse")
        data.to_csv(self.clean_team_trend_data_directory.format(self.league), index=False)
        self.log.info("{} data saved in clean folder".format(self.league))
        return data

    # Method not in use for now, will soon be deprecated
    def save_data(self, league):
        """
        Not in use for now
        :return: None 
        """
        # for league in leagues.keys():
        self.league = league
        self.clean_football_data(league=self.league)
        self.compute_teams_trend(data=league)
        # process_df.to_csv(self.clean_data_directory.format(self.league), index=False)
        # self.log.info("{} data saved in clean folder".format(self.league))


if __name__ == '__main__':
    dr = CleanProcessStore()
    leagues_data = get_config(file="leagues_id")
    league_list = list(leagues_data.keys())

    procs = []

    for index, number in enumerate(league_list):
        proc = Process(target=dr.clean_football_data, args=(number,))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()

    procs_trend = []

    for index, number in enumerate(league_list):
        proc_tr = Process(target=dr.compute_teams_trend, args=(number,))
        procs_trend.append(proc_tr)
        proc_tr.start()

    for proc_tr in procs_trend:
        proc_tr.join()
