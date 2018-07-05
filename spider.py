# -*-coding:utf-8 -*-
from requests import Session
from redisqueue import RedisQueue
from request import WeixinRequest
from urllib.parse import urlencode
from pyquery import PyQuery as pq
from settings import *
import requests
from mysql import MySQL


class Spider():
    base_url = 'http://weixin.sogou.com/weixin'
    keyword = 'NBA'
    headers = {
        'Accept': 'text / html, application / xhtml + xml, application / xml;q = 0.9, image / webp, image / apng, * / *;q = 0.8',
        'Accept - Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'pgv_pvi=4957974528; main_login=wx; vuserid=144115210108858683; pgv_pvid=1182295365; AMCV_248F210755B762187F000101%40AdobeOrg=-1891778711%7CMCIDTS%7C17716%7CMCMID%7C39367175559010545302503241949109946279%7CMCAAMLH-1531200604%7C11%7CMCAAMB-1531200604%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1530603004s%7CNONE%7CMCAID%7CNONE%7CMCSYNCSOP%7C411-17723%7CvVersion%7C2.4.0; eas_sid=w1g553p0Q6L8F973J4L54038q5; rankv=2018062218; pac_uid=0_5b3c902861e0a; rewardsn=; wxtokenkey=777',
        'Host': 'mp.weixin.qq.com',
        'Referer': 'http://weixin.sogou.com/weixin?oq=&query=NBA&_sug_type_=1&sut=0&lkt=0%2C0%2C0&s_from=input&ri=5&_sug_=n&type=2&sst0=1530794048607&page=1&ie=utf8&p=40040108&dp=1&w=01015002&dr=1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    }
    session = Session()
    queue = RedisQueue()
    mysql = MySQL()

    def get_proxy(self):
        try:
            response = requests.get(PROXY_POOL_URL)
            if response.status_code == 200:
                print('get proxy', response.text)
                return response.text
            return None
        except requests.ConnectionError:
            return None

    def parse_index(self, response):
        '''
        解析索引页
        :return:
        '''
        doc = pq(response.text)
        items = doc('.news-list > li > div.txt-box > h3 > a').items()
        for item in items:
            url = item.attr('href')
            weixin_request = WeixinRequest(url=url, callback=self.parse_detail)
            yield weixin_request
        next = doc('#sogou_next').attr('href')
        if next:
            url = self.base_url + str(next)
            weixin_request = WeixinRequest(url=url, callback=self.parse_index, need_proxy=False)
            yield weixin_request

    def parse_detail(self, response):
        '''
        解析详情页
        :param response:
        :return:
        '''
        doc = pq(response.text)
        data = {
            'title': doc('#activity-name').text(),
            'content': doc('.rich_media_content').text(),
            'date': doc('#publish_time').text(),
            'nickname': doc('#js_profile_qrcode > div > strong').text(),
            'wechat': doc('#js_profile_qrcode > div > p:nth-child(3) > span').text()
        }
        yield data

    def schedule(self):
        while not self.queue.empty():
            weixin_request = self.queue.pop()
            callback = weixin_request.callback
            print('schedule', weixin_request.url)
            response = self.request(weixin_request)
            # print(response.headers)
            if response and response.status_code in VALID_STATUS:
                results = list(callback(response))
                if results:
                    for result in results:
                        print('New result', result)
                        if isinstance(result, WeixinRequest):
                            self.queue.add(result)
                        if isinstance(result, dict):
                            self.mysql.insert('articles', result)
                else:
                    self.error(weixin_request)
            else:
                self.error(weixin_request)

    def error(self, weixin_request):
        weixin_request.fail_time = weixin_request.fail_time + 1
        print('Request failed',weixin_request.fail_time,'times',weixin_request.url)
        if weixin_request.fail_time< MAX_FAILED_TIME:
            self.queue.add(weixin_request)

    def request(self, weixin_request):
        try:
            if weixin_request.need_proxy:
                proxy = self.get_proxy()
                if proxy:
                    proxies = {
                        'http': 'http://' + proxy,
                        'https': 'https://' + proxy
                    }
                    return self.session.send(weixin_request.prepare(), timeout=weixin_request.timeout,
                                             allow_redirects=False, proxies=proxies)
            return self.session.send(weixin_request.prepare(), timeout=weixin_request.timeout, allow_redirects=False)
        except (requests.ConnectionError, requests.ReadTimeout) as e:
            print(e.args)
            return False

    def start(self):
        self.session.headers.update(self.headers)
        start_url = self.base_url + '?' + urlencode({'query': self.keyword, 'type': 2})
        weixin_request = WeixinRequest(url=start_url, callback=self.parse_index, need_proxy=False)
        self.queue.add(weixin_request)

    def run(self):
        self.start()
        self.schedule()


if __name__ == '__main__':
    spider = Spider()
    spider.run()