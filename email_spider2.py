import csv
import re
from urllib.parse import urljoin, quote_plus

import scrapy
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.linkextractors import LinkExtractor


class GoogleMapSpiderSpider(scrapy.Spider):
    name = "email_spider2"
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8-sig',
        'FEED_URI': f'output/Google_Maps_gym_records_With_Emails.csv',
        'RETRY_TIMES': 5,
        'ROBOTSTXT_OBEY': False,
        'HTTPERROR_ALLOW_ALL': True,
        'ZYTE_SMARTPROXY_ENABLED': False,
        'ZYTE_SMARTPROXY_APIKEY': '',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610
        },
    }
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image'
                  '/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ('
                      'KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.input_file = self.read_csv_file(r'C:\Users\farha\PycharmProjects\New_Task\google_map_scraper\gyms_records.csv')

    def start_requests(self):
        for record in self.input_file:
            if record.get('Company Website') and record.get('Company Website', '') != '' and record.get('Email',
                                                                                                        '') == '':
                url = f"https://{record.get('Company Website')}" if not record.get('Company Website').startswith(
                    'https') else record.get('Company Website')
                yield scrapy.Request(url=url, callback=self.parse_website,
                                     errback=self.parse_error_back, dont_filter=True,
                                     headers=self.headers, meta={'record': record})
            else:
                yield scrapy.Request(url='https://quotes.toscrape.com/', callback=self.parse_record,
                                     headers=self.headers, dont_filter=True,
                                     meta={'all_emails': [],
                                           'record': record})

    def parse_website(self, response):
        record = response.meta.get('record')
        link_extractor = LinkExtractor()
        if response.status == 200:
            social_links = []
            links = link_extractor.extract_links(response)
            for link in links:
                if re.search('facebook', link.url, re.IGNORECASE) or re.search('instagram', link.url, re.IGNORECASE):
                    social_links.append(link.url)
            record['Social medias'] = ','.join(social_links)
            emails = self.get_emails(response)
            wrong_emails = ['example', 'wix', ' ']
            emails = list(set([email for email in emails if '@' in email]))
            emails = list(set([email for email in emails if not any(wrong_word in email for
                                                                    wrong_word in wrong_emails)]))

            record['Email'] = ', '.join(emails)
            yield record
        else:
            record['Social medias'] = ''
            record['Email'] = ''
            yield record

    def parse_individual_link(self, response):
        record = response.meta.get('record')
        all_links = response.meta.get('all_links')
        all_emails = response.meta.get('all_emails')
        all_emails.extend(self.get_emails(response))
        if all_links:
            yield scrapy.Request(url=all_links.pop(), callback=self.parse_individual_link,
                                 headers=self.headers, dont_filter=True,
                                 errback=self.parse_error_back,
                                 meta={'all_links': all_links,
                                       'all_emails': all_emails,
                                       'record': record})
        else:
            yield scrapy.Request(url='https://quotes.toscrape.com/', callback=self.parse_record,
                                 headers=self.headers, dont_filter=True,
                                 meta={'all_emails': all_emails,
                                       'record': record})

    def parse_record(self, response):
        record = response.meta.get('record')
        all_links = response.meta.get('all_links')
        all_emails = response.meta.get('all_emails')
        record['Company Email'] = ', '.join(self.refine_list(all_emails))
        yield record

    def parse_error_back(self, failure):
        yield scrapy.Request(url='https://quotes.toscrape.com/', callback=self.parse_record,
                             headers=self.headers, dont_filter=True,
                             meta={'all_emails': [],
                                   'all_facebook_links': [],
                                   'all_instagram_links': [],
                                   'record': failure.request.meta.get('record')})

    @staticmethod
    def refine_list(data_list):
        return list(set([re.sub('\s+', ' ', data).strip().rstrip('.') for data in data_list if re.sub(
            '\s+', ' ', data).strip() != '']))

    @staticmethod
    def get_required_links(response):
        all_links = list()
        all_links.append(response.url)
        base_url = ''.join(re.findall('[a-z]+\..+', response.url[::-1]))[::-1] + '/'
        required_words_list = ['Privacy', 'privacy', 'policy', 'Policy', 'Contact', 'contact', 'About', 'about',
                               'Pricing', 'pricing']
        required_xpath_list = ['politique de confidentialité', 'Personvernerklæring', 'Politica de confidențialitate',
                               'Chính Sách Bảo Mật ', 'политика конфиденциальности', 'politica sulla riservatezza',
                               'política de privacidad', 'privacybeleid', 'Datenschutz-Bestimmungen',
                               'Polityka prywatności', 'Privatumo politika', 'política de privacitat',
                               'Adatvédelmi irányelvek']
        social_sites_list = ['facebook', 'instagram', 'twitter', 'youtube', 'whatsapp', 'wechat', 'tiktok',
                             'linkedin', 'snapchat', 'tumblr', 'telegram', 'reddit', 'quora']
        for link in [str(link.url) for link in LxmlLinkExtractor(allow=()).extract_links(response)]:
            if any(word in link for word in required_words_list):
                all_links.append(link)
        for xpath in required_xpath_list:
            if response.xpath(f'//*[contains(text(),"{xpath}")]/@href').get():
                all_links.append(response.xpath(f'//*[contains(text(),"{xpath}")]/@href').get())

        for link in all_links:
            if any(word in link for word in social_sites_list):
                all_links.remove(link)
        return list(set([urljoin(base_url, link) for link in all_links]))

    @staticmethod
    def get_emails(response):
        all_emails = list()
        wrong_emails = ['.jpg', '.png', '.svg', '/', '.pro', '.jpeg', ',', '(', ')', '=', '{', '}', '.web', '!']
        all_emails.extend(re.findall(r"[-.a-z]+[0-9]+@[^@\s\.]+\.[.a-z]{2,3}", str(response.text)))
        all_emails.extend(re.findall(r"[-.a-z]+@[^@\s\.]+\.[.a-z]{2,3}", str(response.text)))
        mails = response.xpath("//*[contains(@href,'mailto:')]/text()").getall()
        all_emails.extend([mail.replace('mailto:', '').strip() for mail in mails])
        return list(set([email for email in all_emails if not any(wrong_word in email for
                                                                  wrong_word in wrong_emails)]))

    @staticmethod
    def read_csv_file(filepath, col=None):
        with open(filepath, 'r', encoding="utf-8-sig") as input_file:
            if col:
                return [x[col] for x in csv.DictReader(input_file)]
            else:
                return [x for x in csv.DictReader(input_file)]
