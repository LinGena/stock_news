import cloudscraper
from proxies.proxy_manager import get_proxies
from bs4 import BeautifulSoup
import warnings
import random

warnings.filterwarnings('ignore', message='Unverified HTTPS request')


class GetSession():
    def __init__(self):
        self.set_settings()
        
    def set_settings(self) -> None:
        self.domain = 'https://www.reddit.com'
        self.total_count_try = 25
        self.cookies = None
        self.proxies_list = get_proxies()
        if not self.proxies_list:
            raise Exception('There are no proxies')
        
    def get_proxy(self) -> dict:
        random.shuffle(self.proxies_list)
        proxy = self.proxies_list[0]
        return {'http':proxy, 'https':proxy}

    def get_page_content(self, q: str, cursor: str, type_post: str, count_try: int = 0) -> str:
        self.q = q
        self.next_cursor = cursor
        self.type_post = type_post
        if count_try >= self.total_count_try:
            raise Exception(f'Failed to retrieve content from the page.')
        try:
            link = self.domain + '/search/'
            if cursor:
                link = self.domain + '/svc/shreddit/search/'
            response = self._request(link)
            if self.check_page_content(response):
                return response
        except:
            pass
        return self.get_page_content(q, cursor, type_post, count_try + 1)
        
    def check_page_content(self, response: str) -> bool:
        soup = BeautifulSoup(response, 'lxml')
        if soup.find('title'):
            title = soup.find('title').text.strip()
            list_exceptions = ['Blocked', 'Just a moment...', 'Access denied']
            if title in list_exceptions:
                raise Exception(title)
        if not soup.find('shreddit-title') and not self.next_cursor:
            raise Exception("You've been blocked by network security")
        return True
    
    def _request(self, url: str) -> str:
        try:
            client = cloudscraper.create_scraper()
            client.proxies = self.get_proxy()
            client.headers = self.get_headers()
            if self.cookies:
                client.cookies.update(self.cookies)
            client.params = self.get_params()
            scraped_data = client.get(url=url, timeout=10)
            scraped_data.raise_for_status()
            self.cookies = client.cookies.get_dict()
            return scraped_data.text
        finally:
            client.close()

    def get_headers(self) -> dict:
        return {
            'authority': 'www.reddit.com',
            'accept': 'text/vnd.reddit.partial+html, text/html;q=0.9',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,uk;q=0.6',
            'cache-control': 'no-cache',
            'content-type': 'application/x-www-form-urlencoded',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }

    def get_params(self) -> dict:
        params = {
            'q': self.q,
            'sort': self.type_post
        }
        if hasattr(self, "next_cursor"):
            params.update({'cursor':self.next_cursor}) 
        return params
    
    def get_page_description(self, link: str, count_try: int = 0) -> dict:
        if count_try >= self.total_count_try:
            raise Exception(f'Failed to retrieve JSON from the page.')
        try:
            client = cloudscraper.create_scraper()
            client.proxies = self.get_proxy()
            scraped_data = client.get(url=link, timeout=10)
            scraped_data.raise_for_status()
            return scraped_data.json()
        except:
            pass
        finally:
            client.close()
        return self.get_page_description(link, count_try + 1)
