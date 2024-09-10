import sqlalchemy as sa
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    DateTime,
    Boolean,
    create_engine,
)
from sqlalchemy.orm import relationship, backref, sessionmaker, declarative_base
from lionel.data_load.constants import DATA

Base = declarative_base()


### PRODUCTION TABLES ###
class TeamSeason(Base):
    __tablename__ = "teams_season"

    team_season_id = Column(Integer, primary_key=True)
    team_constant_id = Column(Integer, ForeignKey("teams.team_id"))
    team_name = Column(String, nullable=False)

    team_id_web = Column(Integer)
    season = Column(Integer, nullable=False)

    strength = Column(Integer, nullable=False)  # THESE MAY ALL CHANGE SEASON TO SEASON
    strength_overall_home = Column(Integer, nullable=False)
    strength_overall_away = Column(Integer, nullable=False)
    strength_attack_home = Column(Integer, nullable=False)
    strength_attack_away = Column(Integer, nullable=False)
    strength_defence_home = Column(Integer, nullable=False)
    strength_defence_away = Column(Integer, nullable=False)


class Team(Base):
    __tablename__ = "teams"
    team_id = Column(Integer, primary_key=True, autoincrement=True)
    team_name = Column(String, nullable=False, unique=True)
    team_seasons = relationship("TeamSeason", backref=backref("teams"))


class PlayerSeason(Base):
    __tablename__ = "player_seasons"

    player_season_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey("players.player_id"))
    player_id_web = Column(Integer)
    season = Column(Integer, nullable=False)
    player_name = Column(String, nullable=False)
    player_full_name = Column(String)
    team_season_id = Column(Integer, ForeignKey("teams_season.team_season_id"))
    position = Column(String)
    player_stats = relationship(
        "PlayerStats", back_populates="player_season", lazy="dynamic"
    )


class Player(Base):
    __tablename__ = "players"
    player_id = Column(Integer, primary_key=True, autoincrement=True)
    player_name = Column(String, nullable=False)
    player_full_name = Column(String)
    player_seasons = relationship("PlayerSeason", backref=backref("players"))


class Fixtures(Base):
    __tablename__ = "fixtures"

    fixture_season_id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer)
    gameweek_id = Column(Integer, ForeignKey("gameweeks.gameweek_id"))
    gameweek = Column(Integer)
    finished = Column(Boolean, nullable=False)
    kickoff_time = Column(DateTime, nullable=False)
    season = Column(Integer, nullable=False)

    # ideally want to map to an unchanging ID
    team_a_season_id = Column(
        Integer, ForeignKey("teams_season.team_season_id"), nullable=False
    )
    team_h_season_id = Column(
        Integer, ForeignKey("teams_season.team_season_id"), nullable=False
    )
    team_a_score = Column(Integer)
    team_h_score = Column(Integer)


class Gameweeks(Base):

    __tablename__ = "gameweeks"

    gameweek_id = Column(Integer, primary_key=True)  # e.g. 251 - 2538
    gameweek = Column(Integer)
    deadline_time = Column(DateTime)
    season = Column(Integer, nullable=False)
    fixtures = relationship("Fixtures", backref=backref("gameweeks"))


class PlayerStats(Base):
    __tablename__ = "player_stats"

    player_stat_id = Column(Integer, primary_key=True)
    # player_id_web = Column(Integer, ForeignKey("players.player_id_web"))
    player_season_id = Column(Integer, ForeignKey("player_seasons.player_season_id"))
    player_id = Column(Integer, ForeignKey("players.player_id"))
    fixture_season_id = Column(Integer, ForeignKey("fixtures.fixture_season_id"))
    player = relationship("PlayerSeason", back_populates="player_stats")
    fixture_id = Column(Integer)
    gameweek_id = Column(Integer, ForeignKey("gameweeks.gameweek_id"))
    gameweek = Column(Integer)

    season = Column(Integer)
    total_points = Column(Integer)
    was_home = Column(Boolean)
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


### STAGING TABLES ###
class TeamSeasonStaging(Base):
    __tablename__ = "teams_season_staging"
    team_season_id = Column(Integer, primary_key=True)
    team_name = Column(String, nullable=False)
    team_id_web = Column(Integer)
    season = Column(Integer, nullable=False)
    strength = Column(Integer, nullable=False)
    strength_overall_home = Column(Integer, nullable=False)
    strength_overall_away = Column(Integer, nullable=False)
    strength_attack_home = Column(Integer, nullable=False)
    strength_attack_away = Column(Integer, nullable=False)
    strength_defence_home = Column(Integer, nullable=False)
    strength_defence_away = Column(Integer, nullable=False)


class PlayerSeasonStaging(Base):
    __tablename__ = "player_seasons_staging"
    player_season_id = Column(Integer, primary_key=True)
    player_id_web = Column(Integer)
    web_name = Column(String)
    season = Column(Integer)
    player_full_name = Column(String)
    team_id_web = Column(Integer)
    position = Column(String)


class FixtureStaging(Base):
    __tablename__ = "fixtures_staging"

    fixture_season_id = Column(Integer, primary_key=True)
    fixture_id = Column(Integer)
    gameweek_id = Column(Integer)
    gameweek = Column(Integer)
    finished = Column(Boolean, nullable=False)
    kickoff_time = Column(DateTime, nullable=False)
    season = Column(Integer, nullable=False)
    team_a_season_id = Column(Integer)
    team_h_season_id = Column(Integer)
    team_a_score = Column(Integer)
    team_h_score = Column(Integer)


class GameweekStaging(Base):

    __tablename__ = "gameweeks_staging"

    gameweek_id = Column(Integer, primary_key=True)  # e.g. 251 - 2538
    gameweek = Column(Integer)
    deadline_time = Column(DateTime)
    season = Column(Integer, nullable=False)


class PlayerStatStaging(Base):
    __tablename__ = "player_stats_staging"

    player_stat_id = Column(Integer, primary_key=True)
    player_season_id = Column(Integer)
    # In main table player_id will go here
    fixture_season_id = Column(Integer)
    fixture_id = Column(Integer)
    gameweek_id = Column(Integer)
    gameweek = Column(Integer)

    season = Column(Integer)
    total_points = Column(Integer)
    was_home = Column(Boolean)
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


if __name__ == "__main__":
    engine = create_engine(f"sqlite:///{str(DATA)}/fpl.db")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
