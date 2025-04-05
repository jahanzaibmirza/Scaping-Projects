import csv
import re
from urllib.parse import quote_plus
import scrapy
from scrapy import Request


class GoogleBusinessSpider(scrapy.Spider):
    name = 'google_business_new'
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'FEED_URI': 'Archy/Archy_data_homeService.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'HTTPERROR_ALLOW_ALL': True,
    }

    search_url_template = 'https://www.google.com/localservices/prolist?ssta=1&src=2&q={q}&lci={page}'
    details_url_template = 'https://www.google.com/localservices/prolist?oq={q}&spp={id}'
    RETRY_HTTP_CODES = [400, 403, 407, 408, 429, 500, 502, 503, 504]
    handle_httpstatus_list = RETRY_HTTP_CODES
    headers = {
        'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/93.0 Safari/537.36",
        'Content-Type': "application/json",
        'Accept-Language': "en-US,en;q=0.9",
    }
    
    keywords = [row['Home Services'] for row in csv.DictReader(open('input/homeService.csv', encoding='Latin-1'))]
    locations = [row['zipcode'] for row in csv.DictReader(open('input/USA_ZipCodes.csv', encoding='Latin-1'))]
    processed_businesses = set()

    def start_requests(self):
        for location in self.locations:
            for keyword in self.keywords[:]:
                query = f'{keyword} in {location}, USA'
                url = self.search_url_template.format(q=quote_plus(query), page=0)
                meta = {'keyword': keyword, 'start': 0, 'location': location}
                yield Request(url=url, callback=self.parse_listings, meta=meta)

    def parse_listings(self, response):
        if response.css('div[jsname="AECrIc"]'):
            for listing in response.css('div[jscontroller="xkZ6Lb"]'):
                listing_id = listing.css('::attr(data-profile-url-path)').get('').replace('/localservices/profile?spp=', '')
                business_name = listing.css('.xYjf2e::text').get('').strip()
                business_address = listing.css('.hGz87c span::text').getall()[-1] if listing.css('.hGz87c span::text') else ''

                if (business_name, business_address) not in self.processed_businesses:
                    self.processed_businesses.add((business_name, business_address))
                    details_url = self.details_url_template.format(q=quote_plus(response.meta['keyword']), id=listing_id)
                    response.meta.update({'business_name': business_name, 'business_address': business_address})
                    yield Request(url=details_url, callback=self.parse_details, meta=response.meta)

            start = response.meta['start'] + 20
            next_page_url = self.search_url_template.format(q=quote_plus(response.meta['keyword']), page=start)
            if response.css('div[jscontroller="xkZ6Lb"]'):
                yield Request(url=next_page_url, callback=self.parse_listings, meta={'keyword': response.meta['keyword'], 'start': start, 'location': response.meta['location']})

    def parse_details(self, response):
        item = {
            'Search_Keyword': response.meta.get('keyword', ''),
            'Business_Name': response.meta.get('business_name', ''),
            'Contact': response.css('div.eigqqc::text').get('').replace(' ', '').replace('-', ''),
            'URL': response.url,
            'Duration': response.css('div.FjZRNe::text').get('').strip(),
            'Website': response.css('a.iPF7ob::attr(href)').get('').strip(),
            'Service': response.xpath('//*[contains(text(), "Services:")]/following::text()[1]').get('').strip(),
            'Serving_Area': ', '.join(response.css('div.oR9cEb ::text').getall()),
            'Address': response.meta.get('business_address', ''),
            'Industry': response.meta.get('Industry', ''),
            'Review_Count': response.css('.pNFZHb div.leIgTe::text').get('(0)').replace('(', '').replace(')', '').replace(',', ''),
            'Rating': response.css('.pNFZHb div.rGaJuf::text').get(''),
            'Description': response.css('h3.NwfE3d+div::attr(data-long-text)').get(''),
            'Map_Link': response.css('a[aria-label="Directions"]::attr(href)').get(''),
        }

        # Extracting Opening Hours
        script_content = response.xpath('//script[contains(text(),"hash: ") and contains(text(), "ds:0")]/text()').get('')
        item['Opening_Hours'] = self.extract_opening_hours(script_content)

        yield item

    def extract_opening_hours(self, script_content):
        days = ['Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday']
        opening_hours = []
        for day in days:
            match = re.search(f'"{day}",(.*?)false', script_content)
            if match:
                hours = match.group(1).split('[["')[-1].split('"')[0]
                opening_hours.append(f'{day}: {hours}')
        return ', '.join(opening_hours)
