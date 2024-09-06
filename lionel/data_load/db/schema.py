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
from lionel.data_load.constants import DATA

Base = declarative_base()


class Teams(Base):
    # from team_map
    __tablename__ = "teams"

    # team_id = Column(Integer, primary_key=True) # FOR NOW IGNORE, BUT NOTE
    # I WILL NEED TO UPDATE THIS FOR NEW SEASONS...
    team_id_web = Column(Integer, primary_key=True)  # this may change season to season
    team_name = Column(String, nullable=False, unique=True)
    strength = Column(Integer, nullable=False)  # THESE MAY ALL CHANGE SEASON TO SEASON
    strength_overall_home = Column(Integer, nullable=False)
    strength_overall_away = Column(Integer, nullable=False)
    strength_attack_home = Column(Integer, nullable=False)
    strength_attack_away = Column(Integer, nullable=False)
    strength_defence_home = Column(Integer, nullable=False)
    strength_defence_away = Column(Integer, nullable=False)
    fixtures = relationship("Fixtures", backref=backref("teams"))


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
    team_a_id = Column(Integer, ForeignKey("teams.team_id_web"), nullable=False)
    team_h_id = Column(Integer, ForeignKey("teams.team_id_web"), nullable=False)
    team_a_score = Column(Integer)
    team_h_score = Column(Integer)


class Gameweeks(Base):
    __tablename__ = "gameweeks"

    gameweek_id = Column(Integer, primary_key=True)
    gameweek = Column(Integer)
    deadline_time = Column(DateTime)
    season = Column(Integer, nullable=False)
    fixtures = relationship("Fixtures", backref=backref("gameweeks"))


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


class PlayerStats(Base):
    __tablename__ = "player_stats"

    player_stat_id = Column(Integer, primary_key=True)
    player_id_web = Column(Integer, ForeignKey("players.player_id_web"))
    fixture_season_id = Column(Integer, ForeignKey("fixtures.fixture_season_id"))
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


if __name__ == "__main__":
    engine = create_engine(f"sqlite:///{str(DATA)}/fpl.db")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    # session = Session()
    # session.close()
    # engine.dispose()
