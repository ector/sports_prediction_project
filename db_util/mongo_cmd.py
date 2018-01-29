from pymongo import MongoClient
from bson.objectid import ObjectId

from te_logger.logger import log


class MongoConnect(object):
    """
    Connect and retrieve tables/collections in mongodb
    """
    def __init__(self):
        self.log = log
        self.client = MongoClient()

    def competition_db(self):
        db = self.client["local"]
        comps_db = db.competitions
        return comps_db

    def matches_db(self):
        db = self.client["local"]
        match_db = db.matches
        return match_db

    def standings_db(self):
        db = self.client["local"]
        standing_db = db.standings
        return standing_db

    def teams_db(self):
        db = self.client["local"]
        team_db = db.teams
        return team_db
