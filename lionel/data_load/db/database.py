import sqlalchemy as sa
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Table,
    DateTime,
    Boolean,
    create_engine,
    text,
)
from sqlalchemy.orm import relationship, backref, sessionmaker, declarative_base

import json
import pandas as pd

Base = declarative_base()

# TODO: Start by making it quite simple: Just get all the data in for a single
# season, then I can progress from there...


# what is this versus the class Teams type thing? I don't understand the difference
# Many-to-many relationships are created by an association table acting as a bridge between the two related tables.
# so this is an association table - I think i'll need one of these to account
# for team ids changing across seasons
# author_publisher = Table(
#     "author_publisher",
#     Base.metadata,
#     Column("author_id", Integer, ForeignKey("author.author_id")),
#     Column("publisher_id", Integer, ForeignKey("publisher.publisher_id")),
# )


# this info can change season by season so will need to account for that
# does it actually matter? I think it may because I don't want to confuse
# e.g. the promoted and demoted teams...# tbf i may not need it if the #
## i just also merge on seasons... is that right?
class Teams(Base):
    # from team_map
    __tablename__ = "teams"

    # team_id = Column(Integer, primary_key=True) # FOR NOW IGNORE, BUT NOTE
    # I WILL NEED TO UPDATE THIS FOR NEW SEASONS...
    team_id_web = Column(Integer, primary_key=True)  # this may change season to season
    team_constant_id = Column(Integer, ForeignKey("teams_constant.team_id"))

    season = Column(Integer, nullable=False)
    team_name = Column(String, nullable=False, unique=True)
    strength = Column(Integer, nullable=False)  # THESE MAY ALL CHANGE SEASON TO SEASON
    strength_overall_home = Column(Integer, nullable=False)
    strength_overall_away = Column(Integer, nullable=False)
    strength_attack_home = Column(Integer, nullable=False)
    strength_attack_away = Column(Integer, nullable=False)
    strength_defence_home = Column(Integer, nullable=False)
    strength_defence_away = Column(Integer, nullable=False)
    fixtures = relationship("Fixtures", backref=backref("teams"))

    # akin to the author -> books relationship (one-to-many)
    # fixtures = relationship("Fixtures", backref=backref("teams"))

    # might not be as simple because players can change teams - probably come
    # back to this one..
    # players = relationship("Players", backref=backref("teams"))


class TeamsConstant(Base):
    __tablename__ = "teams_constant"
    team_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False, unique=True)
    # seasons = # can I include a list of seasons here?


# map changeable team ids to unchanging team ids
TeamTeamSeasonID = Table(
    "team_team_id",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("team_id", Integer, ForeignKey("teams_constant.team_id")),
    Column("team_id_web", Integer, ForeignKey("teams.team_id_web")),
    Column("season", Integer),
)


class Fixtures(Base):
    __tablename__ = "fixtures"

    fixture_id = Column(Integer, primary_key=True)
    gameweek = Column(
        Integer,
        ForeignKey("gameweeks.gameweek"),
        nullable=False,
    )
    finished = Column(Boolean, nullable=False)
    kickoff_time = Column(DateTime, nullable=False)
    season = Column(Integer, nullable=False)

    # ideally want to map to an unchanging ID
    team_a_id = Column(Integer, ForeignKey("teams.team_id_web"), nullable=False)
    team_h_id = Column(Integer, ForeignKey("teams.team_id_web"), nullable=False)
    # team_a_difficulty = Column(Integer)
    # team_h_difficulty = Column(Integer)
    team_a_score = Column(Integer)
    team_h_score = Column(Integer)


class Gameweeks(Base):
    __tablename__ = "gameweeks"

    # gameweek_id = Column(Integer, primary_key=True)
    gameweek = Column(Integer, primary_key=True)
    deadline_time = Column(DateTime)
    season = Column(Integer, nullable=False)
    fixtures = relationship("Fixtures", backref=backref("gameweeks"))


# # Again, players can have their names and IDs change season to season, so may
# # need to account for that...
class Players(Base):
    __tablename__ = "players"

    # player_id = Column(Integer, primary_key=True)
    player_id_web = Column(Integer, primary_key=True)
    web_name = Column(String)
    first_name = Column(String)
    second_name = Column(String)

    # may need to account for players and positions which does happen
    team_id_web = Column(Integer, ForeignKey("teams.team_id_web"))
    position = Column(String)


[
    "player_web_id",
    "fixture_id",
    "total_points",
    "round",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "value",
    "transfers_balance",
    "selected",
    "transfers_in",
    "transfers_out",
]


class PlayerStats(Base):
    __tablename__ = "player_stats"

    player_stat_id = Column(Integer, primary_key=True)
    player_id_web = Column(Integer, ForeignKey("players.player_id_web"))
    fixture_id = Column(Integer, ForeignKey("fixtures.fixture_id"))
    season = Column(Integer)

    total_points = Column(Integer)
    gameweek = Column(Integer, ForeignKey("gameweeks.gameweek"))
    minutes = Column(Integer)
    goals_scored = Column(Integer)
    assists = Column(Integer)
    clean_sheets = Column(Integer)
    goals_conceded = Column(Integer)
    own_goals = Column(Integer)
    penalties_saved = Column(Integer)
    penalties_missed = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    saves = Column(Integer)
    bonus = Column(Integer)
    bps = Column(Integer)
    influence = Column(Integer)
    creativity = Column(Integer)
    threat = Column(Integer)
    ict_index = Column(Integer)
    expected_goals = Column(Integer)
    expected_assists = Column(Integer)
    expected_goal_involvements = Column(Integer)
    expected_goals_conceded = Column(Integer)
    value = Column(Integer)
    transfers_balance = Column(Integer)
    selected = Column(Integer)
    transfers_in = Column(Integer)
    transfers_out = Column(Integer)


# Start the stuff
database_url = "sqlite:///my_first_database.db"
engine = create_engine(database_url)
Base.metadata.create_all(engine)
Session = sessionmaker(
    bind=engine
)  # session conflicts with when I want to use pd.to_sql
# session = Session()

# Load the data
path = "/Users/toby/Dev/lionel/data/raw/scraped_data_20240905.json"
with open(path, "r") as fp:
    data = json.load(fp)

# Teams data
df_teams = (
    pd.DataFrame.from_dict(data["team_map"], orient="index").reset_index()
    # .reset_index()
    .rename(columns={"index": "team_id_web", "name": "team_name"})
)
# Need to update names here!
df_teams.to_sql("teams", con=engine, if_exists="append", index=False)

# Fixture
df_fixtures = (
    pd.DataFrame(data["fixtures"])
    .rename(
        columns={
            "id": "fixture_id",
            "team_a": "team_a_id",
            "team_h": "team_h_id",
            "event": "gameweek",
        }
    )
    .drop(columns=["team_a_difficulty", "team_h_difficulty"])
)
df_fixtures["season"] = 25
# df_fixtures['kickoff_time'] = pd.to_datetime(df_fixtures['kickoff_time'])
df_fixtures.to_sql("fixtures", con=engine, if_exists="append", index=False)

# Gameweek dates
df_gameweeks = (
    pd.DataFrame.from_dict(data["gw_deadlines"], orient="index")
    .reset_index()
    .rename(columns={"index": "gameweek", 0: "deadline_time"})
)
df_gameweeks["season"] = 25
df_gameweeks["deadline_time"] = pd.to_datetime(df_gameweeks["deadline_time"])
df_gameweeks.dtypes
df_gameweeks.to_sql("gameweeks", con=engine, if_exists="append", index=False)
pd.read_sql("gameweeks", con=engine)


df_players = pd.DataFrame.from_dict(data["element_map"], orient="index").rename(
    columns={"id": "player_id_web", "team_id": "team_id_web"}
)
position_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
df_players["position"] = df_players["element_type"].map(position_map)
df_players = df_players.drop(columns=["element_type"])
df_players.to_sql("players", con=engine, if_exists="append", index=False)


# GW stats
player_keys = [data[k] for k in data.keys() if "player_stats_" in k]
player_keys = [x for xs in player_keys for x in xs]
df_player_stats = pd.DataFrame(player_keys)
df_player_stats["season"] = 25
keep_cols = [
    "element",
    "fixture",
    "season",
    "total_points",
    "round",
    "minutes",
    "goals_scored",
    "assists",
    "clean_sheets",
    "goals_conceded",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "value",
    "transfers_balance",
    "selected",
    "transfers_in",
    "transfers_out",
]
df_player_stats = df_player_stats[keep_cols].rename(
    columns={"element": "player_id_web", "round": "gameweek", "fixture": "fixture_id"}
)
df_player_stats["player_stat_id"] = (
    df_player_stats.index
)  # I can really just overwrite the table each time I think lol...
# actually not true because eventually I'll have to add other seasons

df_player_stats.to_sql("player_stats", con=engine, if_exists="append", index=False)

# df_player_stats = df_player_stats.drop(columns=["player_stat_id"])
# df_player_stats.to_sql("player_stats", con=engine, if_exists="append", index=False)
# automatically adds primary key - that is useful, so I can just append as long as I have
# checked for duplicates ... good to know..

df = pd.read_sql("player_stats", con=engine)
df

# d = addresses_table.delete().where(addresses_table.c.retired == 1)
# d.execute()

# not sure how this whole thing works tbh
# session.execute(text("DELETE FROM player_stats WHERE season=25"))

Session = sessionmaker(bind=engine)
session = Session()
# session.execute(text("SELECT * FROM player_stats")).fetchall()
session.execute(text("DELETE FROM player_stats WHERE season=25"))
session.commit()

df = pd.read_sql("player_stats", con=engine)
df.shape
# I can remove all the entries from the current season and replace with these...
# just an easy approach ig...

# https://stackoverflow.com/questions/76781372/upsert-append-to-sql-database-using-sql-alchemy-pandas
# Use temp table to upsert...

# metadata = sa.MetaData()
# player_stats = sa.Table("player_stats", metadata)
# player_stats.query()
# sa.select()
# sa.query(PlayerStats).filter(PlayerStats.season == 25)


select_obj = sa.select(PlayerStats).where(PlayerStats.season == 25)
# read the data from this into a list of dictionaries
# d_ = [dict(row) for row in session.execute(select_obj)]
print(select_obj)
engine.execute(select_obj)


# session
# session.close_all()


# TODO: What is an efficient way to upload everything? In form of dictionaries
# or pandas dataframe? not clear that putting everything in a dataframe is
# needed and surely it just adds an unneeded step.
