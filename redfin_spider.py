# from geopy.geocoders import Nominatim
# from geopy.distance import geodesic
from urllib.parse import quote
import scrapy
import json
import re


class RedfinSpiderSpider(scrapy.Spider):
    name = "redfin_spider"

    start_urls = ["https://www.redfin.com/"]
    # with open('inputs/key.txt', 'r') as key_file:
    #     key = key_file.readline()
    with open('inputs/input_address.txt', 'r') as file:
        addresses = file.readlines()
    fields = ['Estimated Price', 'Area Between 0.25 to 0.50 Acres', 'Area Unit', 'Address', 'Distance in Miles',
              'Agent First Name',
              'Agent Last Name', 'Company', 'Zip Code', 'Median sale Price', 'Status', 'Page URL']
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        # 'DOWNLOAD_DELAY': 1,
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 5,
        'HTTPERROR_ALLOW_ALL': True,
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEEDS': {'output/redfin_data.xlsx': {'format': 'xlsx', 'overwrite': False, 'fields': fields}},
        # 'DOWNLOADER_MIDDLEWARES': {'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610},
        # 'ZYTE_SMARTPROXY_ENABLED': True,
        # 'ZYTE_SMARTPROXY_APIKEY': f'{key}',
        # 'X-Crawlera-Region': 'US',
        'ZYTE_API_KEY': "a3ac097c634b437b8ce7eea576fa80d8",
        'ZYTE_API_TRANSPARENT_MODE': True,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
        },
        'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }
    headers = {
        'authority': 'www.redfin.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'dnt': '1',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    }
    address_url = 'https://www.redfin.com/stingray/do/location-autocomplete?location={}&start=0&count=10&v=2&market=southwestflorida&al=1&iss=false&ooa=true&mrs=false&region_id=NaN&region_type=NaN'
    search_url = 'https://www.redfin.com/zipcode/{}/filter/property-type=land,min-lot-size=0.25-acre,max-lot-size=0.5-acre,include=sold-6mo'

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], headers=self.headers)

    def parse(self, response, **kwargs):
        for address in self.addresses:
            yield scrapy.Request(url=self.address_url.format(quote(address, safe=':/?=&')), callback=self.get_url,
                                 headers=self.headers)

    def get_url(self, response):
        json_data = (json.loads(response.text.replace("{}&&", '')))
        url = json_data.get("payload", {}).get("sections", [])[0].get("rows", [])[0].get('url', '')
        if url:
            yield response.follow(url=url, callback=self.detail_page, headers=self.headers)
        else:
            print("Please Enter the full address!")

    def detail_page(self, response):
        if response.status != 200:
            yield response.follow(url=response.url, callback=self.detail_page, headers=self.headers, dont_filter=True)
        else:
            item = dict()
            item['Estimated Price'] = ''.join(
                response.css('div[data-rf-test-id="abp-price"]>div ::text').getall()).strip()
            item["Area Between 0.25 to 0.50 Acres"] = response.css(
                'div[data-rf-test-id="abp-sqFt"]>span.statsValue::text').get('').strip()
            item["Area Unit"] = response.css('div[data-rf-test-id="abp-sqFt"]>div.statsLabel::text').get('').strip()
            item['Address'] = re.sub(r'\s+', ' ', ' '.join(response.css("h1.full-address>div::text").getall())).strip()
            agent_name = re.sub(r'\s+', ' ', ' '.join(
                response.xpath('(//span[@class="agent-basic-details--heading"])[1]/span/text()').getall())).strip()
            if agent_name:
                item["Agent First Name"] = agent_name.split(' ', 1)[0].strip() if ' ' in agent_name else agent_name
                item["Agent Last Name"] = agent_name.split(' ', 1)[-1].strip() if ' ' in agent_name else ' '
            item["Company"] = re.sub(r'\s+', ' ', ' '.join(
                response.xpath('(//span[@class="agent-basic-details--broker"])[1]/span/text()').getall())).strip()
            item["Zip Code"] = item['Address'].split()[-1].strip() if item['Address'] else ""
            item['Status'] = 'Searched item'
            item['Page URL'] = response.url
            item['Distance in Miles'] = self.calculate_distance(item['Address'], item['Address'])
            yield scrapy.Request(url=self.search_url.format(item["Zip Code"]), headers=self.headers,
                                 callback=self.listing_page, meta={'item': item})

    def listing_page(self, response):
        if response.status != 200:
            print("You Entered Wrong Address!")
        else:
            median_sale_price = response.xpath(
                "//div[contains(text(),'Median Sale Price')]/following-sibling::div/div[contains(.,'$')]/text()").get(
                '').strip()
            item = response.meta['item']
            item['Median sale Price'] = median_sale_price
            yield item
            for land in response.css('div.HomeCardsContainer>div[id^=M]'):
                comp = dict()
                comp['Estimated Price'] = land.css('span[data-rf-test-name="homecard-price"]::text').get('').strip()
                comp['Area Between 0.25 to 0.50 Acres'] = land.xpath(
                    ".//div[contains(text(),'acre') or contains(text(),'(lot)')]/text()").get('')
                if comp['Area Between 0.25 to 0.50 Acres']:
                    comp['Area Unit'] = comp['Area Between 0.25 to 0.50 Acres'].split(' ', 1)[-1].strip()
                    comp['Area Between 0.25 to 0.50 Acres'] = comp['Area Between 0.25 to 0.50 Acres'].split()[0].strip()
                comp['Address'] = land.css("a[title]>div::text").get('').strip()
                comp['Zip Code'] = comp['Address'].split()[-1].strip() if comp['Address'] else ""
                comp['Status'] = 'Comps item'
                comp['Page URL'] = response.urljoin(land.css("a[title]::attr(href)").get('').strip())
                comp['Median sale Price'] = median_sale_price
                if item['Address'] and comp['Address']:
                    comp['Distance in Miles'] = self.calculate_distance(item['Address'], comp['Address'])
                yield comp

    def calculate_distance(self, location1, location2):
        print("location 1::", location1, "location2::", location2)
        geolocator = Nominatim(user_agent="my_geocoder", timeout=500)
        location1_coordinates = geolocator.geocode(location1)
        location2_coordinates = geolocator.geocode(location2)
        if location1_coordinates is not None and location2_coordinates is not None:
            distance = geodesic((location1_coordinates.latitude, location1_coordinates.latitude),
                                (location2_coordinates.latitude, location2_coordinates.latitude)).miles
            return round(distance, 2)
        else:
            print('location_coordinates not find!')
            return ''
