from ib_insync import *
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import asyncio
from interactive_brokers.db_ib import IBkr
import time

class GetIBrkNews():
    async def connect_ib(self):
        self.ib = IB()
        await self.ib.connectAsync()

    async def set_contract(self, symbol: str):
        self.contract = Stock(symbol, 'SMART', 'USD')
        await self.ib.qualifyContractsAsync(self.contract)

    def set_intervals(self):
        self.start_date_time = datetime.now() - timedelta(days=2 * 365)
        self.end_date_time = self.start_date_time + timedelta(days=30)

    async def set_provider_code(self, number: int = 0):
        news_providers = await self.ib.reqNewsProvidersAsync()
        if not news_providers:
            raise Exception("No news providers found.")
        self.provider_code = news_providers[number].code

    async def get(self, symbol):
        await self.connect_ib()
        try:
            await self.set_contract(symbol)
            self.set_intervals()
            for i in range (0, 2):
                await self.set_provider_code(i)
                await self.get_news(symbol)
        except Exception as ex:
            print(f"An error occurred: {ex}")
        finally:
            if self.ib.isConnected():
                self.ib.disconnect()

    async def get_news(self, symbol):
        news_articles = await self.fetch_historical_news_with_retries()
        print(f'Total [{symbol}]=',len(news_articles))
        news_list = []
        j = 1
        for article in news_articles:
            try:
                print(j, article.articleId)
                j+=1
                full_article = await self.ib.reqNewsArticleAsync(self.provider_code, article.articleId)
                html_body = full_article.articleText
                soup = BeautifulSoup(html_body, 'lxml')
                news_item = {
                    "symbol": symbol,
                    "article_id": article.articleId,
                    "article_time": article.time.strftime('%Y-%m-%d %H:%M:%S'),
                    "source": article.providerCode,
                    "article_title": article.headline,
                    "article_body": soup.text.strip()
                }
                news_list.append(news_item)
            except Exception as ex:
                print(f"Error fetching full article: {ex}")
        IBkr().bulk_insert_or_update(news_list)
        # while current_start < datetime.now():
        #     try:
        #         current_end = current_start + timedelta(days=30)
        #         start = current_start.strftime('%Y-%m-%d %H:%M:%S')
        #         end = current_end.strftime('%Y-%m-%d %H:%M:%S')
        #         news_list = []

        #         print(f"---- Fetching news for interval {start} to {end}")

        #         news_articles = await self.fetch_historical_news_with_retries(start, end)
        #         if news_articles:
        #             for article in news_articles:
        #                 try:
        #                     full_article = await self.ib.reqNewsArticleAsync(self.provider_code, article.articleId)
        #                     html_body = full_article.articleText
        #                     soup = BeautifulSoup(html_body, 'lxml')
        #                     news_item = {
        #                         "symbol": symbol,
        #                         "article_id": article.articleId,
        #                         "article_time": article.time.strftime('%Y-%m-%d %H:%M:%S'),
        #                         "source": article.providerCode,
        #                         "article_title": article.headline,
        #                         "article_body": soup.text.strip()
        #                     }
        #                     news_list.append(news_item)
        #                 except Exception as ex:
        #                     print(f"Error fetching full article: {ex}")
        #                     continue

        #         if news_list:
        #             IBkr().bulk_insert_or_update(news_list)
        #             total_articles += len(news_list)
        #             print(f"Fetched {total_articles} articles so far...")

        #         current_start = current_end
        #     except Exception as ex:
        #         print(f"Error fetching historical news: {ex}")
        #         break

    async def fetch_historical_news_with_retries(self, retries=20, delay=10):
        for attempt in range(retries):
            try:
                news_articles = await self.ib.reqHistoricalNewsAsync(self.contract.conId, self.provider_code, '20210101', '20250101', 300)
                if news_articles:
                    return news_articles
            except Exception as ex:
                print(f"Attempt {attempt + 1} failed: {ex}")
                await asyncio.sleep(delay)
        return []
