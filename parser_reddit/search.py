
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from parser_reddit.get_session import GetSession
from utils.func import *
from utils.logger import Logger
from parser_reddit.db_reddit import DbReddit


class RedditPostsSearch(GetSession):
    def __init__(self):
        super().__init__()
        self.logger = Logger().get_logger(__name__)
    
    def parse(self, q: str, type_post: str):
        self.last_posts_date = datetime.utcnow() - timedelta(days=4*355)
        try:
            cursor = None
            i = 0
            while i < 10:
                src = self.get_page_content(q, cursor, type_post)
                posts = self.get_result_from_src(src, q)
                if posts:
                    DbReddit().insert_datas(posts)
                cursor = self.get_next_page(src)
                i+=1
        except Exception as e:
            self.logger.error(e)
    
    def get_next_page(self, src: str) -> str:
        cursor_value = None
        soup = BeautifulSoup(src, 'lxml')
        cursor_links = soup.find_all('faceplate-partial', src=lambda x: x and 'cursor=' in x)
        if cursor_links:
            link = cursor_links[-1].get("src")
            data = urlparse(link)
            query_params = parse_qs(data.query)
            cursor_value = query_params.get('cursor', [None])[0]
        return cursor_value
    
    def get_result_from_src(self, src: str, q: str) -> list:
        post_result = []
        soup = BeautifulSoup(src, 'lxml')
        posts = soup.find_all('search-telemetry-tracker', {'data-testid':'search-sdui-post'})
        if not posts:
            return None
        for post in posts:
            try:
                self.stop_parse = False
                a_teg = post.find('a', {'data-testid':'post-title'})
                if not a_teg:
                    continue
                data = post.get('data-faceplate-tracking-context')
                if not data:
                    continue
                data : dict = json.loads(data)
                post_dict : dict = data.get('post')
                if not post_dict:
                    continue
                post_title = post_dict.get('title', None)
                if not post_title:
                    raise Exception('No post text')
                numbers_results = self.get_numbers_results(post)
                url = a_teg.get('href')
                subreddit = str(url).split('/')[1].split('/')[0]
                subreddit_name = str(url).split(f'/{subreddit}/')[1].split('/')[0]
                subreddit_link = self.domain + f'/{subreddit}/' + subreddit_name
                link = self.domain + url
                result = {
                    'word': q,
                    'post_link': link,
                    'post_date': self.get_post_create(post),
                    'post_likes': int(numbers_results.get('votes', 0)),
                    'post_comments': int(numbers_results.get('comments', 0)),
                    'post_title': post_title,
                    'post_description': self.get_description(link),
                    'subreddit_name': subreddit_name,
                    'subreddit_link': subreddit_link
                }
                if self.stop_parse:
                    continue
                post_result.append(result)
            except Exception as ex:
                self.logger.error(ex)
        return post_result

    def get_description(self, link: str) -> str:
        if link.endswith('/'):
            link = link[:-1]
        link = link + '.json'
        data = self.get_page_description(link)
        for item in data:
            for child in item.get('data', {}).get('children', []):
                selftext = child.get('data', {}).get('selftext', '')
                if selftext:
                    return selftext
        return ""

    def get_post_create(self, post: BeautifulSoup):
        post_create = post.find('faceplate-timeago')
        if post_create:
            date_string = post_create.get('ts')
            post_date = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z").replace(tzinfo=None)
            if post_date < self.last_posts_date:
                self.stop_parse = True
            return post_date
        raise Exception('No post date')

    def get_numbers_results(self, post: BeautifulSoup) -> dict:
        numbers = post.find_all("faceplate-number")
        numbers_results = {}
        for number in numbers:
            next_element = number.find_next(string=True)
            if str(next_element).strip() == 'votes':
                next_text = next_element.strip()
            else:
                next_element = number.find_next()
                if next_element:
                    next_text = next_element.get_text(strip=True)
            if next_text:
                numbers_results[next_text] = number.get("number", 0)
        return numbers_results