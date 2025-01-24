from db.core import PostgreSQLTable


class DbReddit(PostgreSQLTable):
    def __init__(self, table_name = 'stock_posts_reddit'):
        super().__init__(table_name)

    def insert_datas(self, datas: list):
        self.bulk_insert_or_update(datas)

    def create(self):
        if not self.check_table():
            sql = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id BIGSERIAL PRIMARY KEY,
                    word VARCHAR(255) NOT NULL,
                    post_link VARCHAR(500) NOT NULL,
                    post_date TIMESTAMP,
                    post_likes INTEGER,
                    post_comments INTEGER,
                    post_title TEXT,
                    post_description TEXT,
                    subreddit_name VARCHAR(255),
                    subreddit_link VARCHAR(500),
                    date_added TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT unique_post UNIQUE (post_link, word)
                )
            """
            cursor = self.db.connection.cursor()
            cursor.execute(sql)
            self.db.connection.commit()
