import sqlalchemy as sa
from sqlalchemy import (
    create_engine,
    text,
)


"""

## NOTE: should be able to update to_sql to use this kind of thing,
# might make more sense to insert the session everywhere 
# instead of using the engine itself...
# with db_session(connection_url) as session:
    # session.execute('INSERT INTO ...')
    df = pd.read_sql(sql_query, session.connection())
"""


class DBManager:
    def __init__(self, db_path, metadata=None):
        self.engine = create_engine(f"sqlite:///{db_path}")
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

    def delete_rows(self, table_name, season):
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
