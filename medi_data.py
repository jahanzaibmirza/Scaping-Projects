import json
from datetime import datetime
import scrapy


class MediDataSpider(scrapy.Spider):
    name = "medi_data"

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    }
    zipcode_list = ['BC V6Z 1E4', 'AB T5J 3S4', 'SK S4T 1H4', 'MB R3B 2B3', 'ON M5B 1M4', 'QC H3G 1R1', 'NB E3B 1B2'
        , 'NS B3J 1A6', 'PE C1A 1K6', 'NL A1C 1A9', 'YT Y1A 1C4', 'NT X1A 2N6', 'NU X0A 0H0', 'V6Z1E4'

                    ]

    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.4,
                       'HTTPERROR_ALLOW_ALL': True,
                       'FEED_URI': f'outputs/w_23.csv',
                       'FEED_FORMAT': 'csv',
 }

    def start_requests(self):
        for i in range(1, 10):
            url=f'https://medimap.ca/clinics/walk-in-clinics/nl/st-johns?page={i}&postalCode=A1C1A9'
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse,
                                 meta={'url': url})

    def parse(self, response):
        url= response.meta.get('url')
        listing_links= response.xpath('//a[@class="css-1s5t7mx"]/@href').getall()
        for link in listing_links[:]:
            yield response.follow(url=link, headers=self.headers, callback=self.detail_page,
                                  meta={'url':url})
    def detail_page(self, response):
        item= dict()
        item['Profile']= response.url
        json_data = response.xpath('//script[@id="__NEXT_DATA__"]//text()').get('')
        api_data1=json.loads(json_data)
        api_data= api_data1.get('props',{}).get('pageProps').get('facility',{})
        item['Practice Name']= api_data.get('details',{}).get('name','')
        item['Website']=api_data.get('details',{}).get('website','')
        revies = api_data.get('facilityReviews', {}).get('statistics', {})
        item['Ratings'] = revies.get('avgRating', '')
        item['Reviews'] = revies.get('reviewCount', '')
        item['Address']= response.xpath('//span[@class="css-1ywa80u"]/text()').get('').strip()
        email= api_data.get('details',{}).get('email','')
        item['Phone Number']= api_data.get('details',{}).get('phone','')
        item['Book Now Link']=response.meta.get('url')
        item['Book Now yes ']='Book Now'
        book_now_link= response.xpath("(//span[contains(text(),'Book Now')])[1]/parent::a/@href").get('')
        if book_now_link:
            item['Book Now Link'] = book_now_link
            item['Book Now yes '] = 'Book Now'
        else:
            item['Book Now Link'] = ''
            item['Book Now yes '] = ''
        services_list=[]
        services_data = api_data.get('services', [])
        for each_service in services_data:
            service=each_service.get('name','')
            services_list.append(service)
        item['Services']= ",".join(services_list)
        item['Language'] = api_data.get('details', {}).get('languages', '')
        yield item
