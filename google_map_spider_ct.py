import csv
import re
from urllib.parse import urljoin, quote_plus
import pyexcel
import scrapy
from scrapy.utils.response import open_in_browser


class GoogleMapSpiderSpider(scrapy.Spider):
    name = "google_map_spider_ct"
    start_urls = ['https://www.google.com/']
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_URI': f'outputs/brick_and_motor_data_not_filtered_similar_keyword_updated.csv',
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS': 16,
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 2.5
    }

    def parse(self, response, **kwargs):
        with open(r'outputs/brick_and_motor_data_not_filtered_similar_keyword.csv', mode='r', encoding='utf-8') as f:
            reader = list(csv.DictReader(f))
            for row in reader:
                if row.get('Adress', ''):
                    address = row.get('Adress', '').split(',')
                    try:
                        row['Street'] = address[0]
                        row['City'] = address[1]
                        row['State'] = address[2].strip().split(' ')[0].strip()
                        row['Zipcode'] = address[2].strip().split(' ')[1].strip()
                        row['Country'] = address[3]
                    except:
                        row['Street'] = ''
                        row['City'] = ''
                        row['State'] = ''
                        row['Zipcode'] = ''
                        row['Country'] = ''
                    yield row
                else:
                    row['Street'] = ''
                    row['City'] = ''
                    row['State'] = ''
                    row['Zipcode'] = ''
                    row['Country'] = ''
                    yield row
