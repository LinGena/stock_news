from db.core import PostgreSQLTable


class IBkr(PostgreSQLTable):
    def __init__(self, table_name = 'stock_news_ibkr'):
        super().__init__(table_name)

    def insert_datas(self, datas: list):
        self.bulk_insert_or_update(datas)

    def create(self):
        if not self.check_table():
            sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    article_id VARCHAR(50) NOT NULL,
                    article_time TIMESTAMP NOT NULL,
                    source VARCHAR(50),
                    article_title TEXT,
                    article_body TEXT,
                    date_added TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT unique_article UNIQUE (article_id)
                )
            """
            cursor = self.db.connection.cursor()
            cursor.execute(sql)
            self.db.connection.commit()
