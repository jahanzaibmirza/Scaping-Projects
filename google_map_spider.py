import re
from urllib.parse import urljoin, quote_plus
import pyexcel
import scrapy
from scrapy.utils.response import open_in_browser


class GoogleMapSpiderSpider(scrapy.Spider):
    name = "google_map_spider"
    base_url = 'https://www.google.com/'
    start_urls = []
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_URI': f'outweb2_cylex_de_dentists_outputput/.csv',
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 16,
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 2.5
    }
    listings_url_t = 'https://www.google.com/search?sxsrf=ACYBGNS1OuAlrwXrWvHCe01W6jx80oL9jA:1581870852554&q={q}&npsic=0&rflfq=1&rlha=0&rllag=-33868535,151194512,2415&tbm=lcl&ved=2ahUKEwiN1fyRwNbnAhUHVBUIHdOxBdIQjGp6BAgLEFk'
    details_url_t = 'https://www.google.com/async/lcl_akp?ei=N5dMXuOUC82ckgXKz634Ag&' \
                    'tbs=lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!1m4!1u16!2m2!16m1!1e1!1m4!1u16!2m2!16m1!1e2' \
                    '!2m1!1e2!2m1!1e16!2m1!1e3!3sIAE,lf:1,lf_ui:9&yv=3&lqi=Chd2ZWdhbiByZXN0YXVyYW50IHN5ZG5le' \
                    'UjDmMXr9pWAgAhaNQoQdmVnYW4gcmVzdGF1cmFudBAAEAEYABgBGAIiF3ZlZ2FuIHJlc3RhdXJhbnQgc3lkbmV5&' \
                    'phdesc=Z0sOfSPV1mY&vet=10ahUKEwijjPfywtznAhVNjqQKHcpnCy8Q8UEI7AI..i&' \
                    'lei=N5dMXuOUC82ckgXKz634Ag&tbm=lcl&q={q}&' \
                    'async=ludocids:{id},f:rlni,lqe:false,_id:akp_tsuid14,_pms:s,_fmt:pc'
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                  '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ('
                      'KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'X-Crawlera-Region': 'au',
    }
    keywords = ['Hölzel, Stephan Zahnarzt']
    search_words = ['gym']
    locations = ['Lille', 'Dijon', 'Cannes', 'Avignon', 'Rouen', 'Reims', 'Grenoble', 'Amiens', 'Limoges']
    search_keyword = '{keyword}'
    ignore_keyword = ['fitness park', 'basic fit', 'vita liberte', 'magic form', 'gymnase', 'neoness']


    def start_requests(self):
        for keyword in self.keywords:
            query = self.search_keyword.format(keyword=keyword)
            url = self.listings_url_t.format(q=quote_plus(query))
            item = dict()
            item['Searched Query'] = query
            meta = {'item': item}
            yield scrapy.Request(url=url, callback=self.parse, meta=meta, dont_filter=True,
                                 headers=self.headers)

    def parse(self, response, **kwargs):
        item = response.meta['item']
        for listing_selector in response.css('div div[jsname="jXK9ad"]'):
            item['Company Name'] = ''.join(listing_selector.css('.dbg0pd[role="heading"] div ::text').getall())
            item['Company Rating'] = listing_selector.css('span.yi40Hd::text').get('').replace(',', '.')
            listing_id = listing_selector.css('a::attr(data-cid)').get()
            details_url = self.details_url_t.format(q=quote_plus(item.get('Searched Query')), id=listing_id)
            response.meta['item'] = item
            yield scrapy.Request(url=details_url, callback=self.parse_google_detail_page,
                                 meta=response.meta, dont_filter=True, headers=self.headers)

        # next_page = response.css('#pnnext::attr(href)').get('')
        # if next_page:
        #     next_page_url = urljoin(self.base_url, next_page)
        #     yield scrapy.Request(url=next_page_url, callback=self.parse,
        #                          dont_filter=True, meta=response.meta)

    def parse_google_detail_page(self, response):
        item = response.meta['item']
        name = response.css('h2[data-attrid="title"] span::text').get()
        item['Company Name'] = name if name else item['Company Name']
        item['Company Address'] = response.css('.w8qArf:contains(Address) + .LrzXr::text').get('')
        item['Company Phone Number'] = response.css('span:contains("Phone") + span a span::text').get('')
        company_website = response.css(
            'a:contains("Website")::attr(href)').re_first(r'q=(.*)/') or response.css(
            'a:contains("Website")::attr(href)').get()
        item['Company Website'] = company_website
        item['Company Reviews'] = ''.join(re.findall('\d+', response.css('.z5jxId::text').get('0')))
        if company_website:
            yield item

    # def parse_google_detail_page(self, response):
    #     item = response.meta['item']
    #     name = response.css('h2[data-attrid="title"] span::text').get()
    #     item['Company Name'] = name if name else item['Company Name']
    #     item['Company Address'] = response.css('.w8qArf:contains(Address) + .LrzXr::text').get('')
    #     item['Company Phone Number'] = response.css('span:contains("Phone") + span a span::text').get('')
    #     company_website = response.css(
    #         'a:contains("Website")::attr(href)').re_first(r'q=(.*)/') or response.css(
    #         'a:contains("Website")::attr(href)').get()
    #     item['Company Website'] = company_website
    #     item['Company Reviews'] = ''.join(re.findall('\d+', response.css('.z5jxId::text').get('0')))
    #     if company_website:
    #         yield item