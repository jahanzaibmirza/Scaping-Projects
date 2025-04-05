# -*- coding: utf-8 -*-
import csv, unicodedata
import re

import scrapy
from urllib.parse import quote_plus, urljoin
from scrapy import Request
from scrapy.utils.response import open_in_browser


class GoogleSpider(scrapy.Spider):
    name = 'google_business_listing_scraper'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'FEED_URI': 'outputs/aesthetic clinics.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }
    new_listings_url_t = 'https://www.google.com/localservices/prolist?ssta=1&src=2&q={q}&lci={page}'
    new_details_url_t = 'https://www.google.com/localservices/prolist?g2lbs=AP8S6ENgyDKzVDV4oBkqNJyZonhEwT_VJ6_XyhCY8jgI2NcumLHJ7mfebZa8Yvjyr_RwoUDwlSwZt5ofLQk3D079b7a0tYFMAl-OvnNjzh2HzyjZNDGO0bloXZTJ8ttkCFt5rwXuqt_u&hl=en-PK&gl=pk&ssta=1&oq={q}&src=2&sa=X&scp=CgASABoAKgA%3D&q={q}&ved=2ahUKEwji7NSKjZiAAxUfTEECHdJnDF8QjdcJegQIABAF&slp=MgBAAVIECAIgAIgBAJoBBgoCFxkQAA%3D%3D&spp={id}'

    listings_url_t = 'https://www.google.com/search?sxsrf=ACYBGNS1OuAlrwXrWvHCe01W6jx80oL9jA:1581870852554&q={q}&npsic=0&rflfq=1&rlha=0&rllag=-33868535,151194512,2415&tbm=lcl&ved=2ahUKEwiN1fyRwNbnAhUHVBUIHdOxBdIQjGp6BAgLEFk'
    RETRY_HTTP_CODES = [400, 403, 407, 408, 429, 500, 502, 503, 504, 405, 503, 520]
    handle_httpstatus_list = [400, 401, 402, 403, 404, 405, 406, 407, 409, 500, 501, 502, 503, 504,
                              505, 506, 507, 509]
    scraped_bussinesses = list()
    headers = {
        'user-agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
        'content-type': "application/json",
        'accept-language': "en-US,en;q=0.9",
        'X-Crawlera-Region': 'GB'
    }
    locations = ['Crescenta', 'Glendale', 'Burbank', 'Pasadena']

    keywords = ["property management company", "property manager"]
    search_keyword = '{keyword}'
    base_url = 'https://www.google.com/'
    start_urls = ["https://quotes.toscrape.com/"]
    names_address_list = []

    def parse(self, response, *args):
        for keyword in self.keywords:
            for location in self.locations:
                search_keyword = f"{keyword} in {location}"
                query = self.search_keyword.format(keyword=search_keyword)
                url = self.new_listings_url_t.format(q=quote_plus(query), page=0)
                meta = {'keyword': search_keyword, 'start': 0, 'location': f"us",
                        'query': query}

                yield Request(url=url, callback=self.parse_new_data, meta=meta)

    def parse_new_data(self, response):
        if response.css('div[jsname="AECrIc"]'):
            for listing_selector in response.css('div[jscontroller="xkZ6Lb"]'):
                res = {
                    'Name': listing_selector.css('.xYjf2e::text').get('').strip(),
                    'Rating': listing_selector.css('div.rGaJuf::text').get('').strip(),
                    'Reviews': listing_selector.css('div.leIgTe::text').get('').strip().replace('(', '').replace(')',
                                                                                                                 ''),
                    'Phone': listing_selector.css('a[data-phone-number]::attr(data-phone-number)').get('').replace(
                        '+44', '0'),
                    'Business Type': listing_selector.css('span.hGz87c::text').get('').strip(),
                    'Opening Time': listing_selector.css('.A5yTVb::text').get('').strip(),
                    'Address': listing_selector.css('div>span.hGz87c:nth-child(2)>span::text').get(),
                    'Website': listing_selector.css('a[aria-label="Website"]::attr(href)').get('').split('&url=')[-1]
                }

                yield res

            keyword = response.meta['keyword']
            location = response.meta['location']
            start = response.meta['start'] + 10
            query = response.meta['query']
            url = self.new_listings_url_t.format(q=quote_plus(query), page=start)
            meta = {'keyword': keyword, 'start': start, 'location': location, 'query': query}
            if response.css('div[jscontroller="xkZ6Lb"]'):
                yield Request(url=url, callback=self.parse_new_data, meta=meta)
