import sqlalchemy as sa
from sqlalchemy import create_engine, insert, text

from lionel.constants import DATA

"""

## NOTE: should be able to update to_sql to use this kind of thing,
# might make more sense to insert the session everywhere 
# instead of using the engine itself...
# with db_session(connection_url) as session:
    # session.execute('INSERT INTO ...')
    df = pd.read_sql(sql_query, session.connection())
"""


class DBManager:
    def __init__(self, db_path=DATA / "fpl.db", metadata=None):
        self.engine = create_engine(f"sqlite:///{db_path}", echo=False, future=True)
        self.metadata = metadata
        self.tables = self.metadata.tables
        # self.Session = sessionmaker(bind=self.engine) # not sure these are needed
        # self.session = self.Session()

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        if value is None:
            self._metadata = sa.MetaData()
            self._metadata.reflect(bind=self.engine)
        else:
            self._metadata = value

    # TODO: Change references and drop this
    def delete_rows(self, table_name, season):
        self.delete_rows_by_season(table_name, season)

    def delete_rows_by_season(self, table_name, season):
        table = self.tables[table_name]
        dele = table.delete().where(table.c.season == season)
        with self.engine.connect() as conn:
            conn.execute(dele)
            conn.commit()

    def query(self, sql_query):
        query = text(sql_query)
        with self.engine.connect() as conn:
            result = conn.execute(query)
            conn.commit()
            return result

    def insert(self, table_name, data: list):
        table = self.tables[table_name]
        with self.engine.connect() as conn:
            _ = conn.execute(insert(table), data)
            conn.commit()
