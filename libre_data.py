import pymongo
import scrapy

class LibreDataSpider(scrapy.Spider):
    name = "libre_data"
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 0.7,
        # 'CONCURRENT_REQUESTS':1,
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'device-memory': '8',
        'downlink': '3.05',
        'dpr': '2.5',
        'ect': '4g',
        'priority': 'u=0, i',
        'rtt': '150',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'viewport-width': '1536',
    }


    def __init__(self, *args, **kwargs):
        super(LibreDataSpider, self).__init__(*args, **kwargs)
        # self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.client = pymongo.MongoClient("mongodb+srv://vercel-admin-user:gkcRx6M9Vam47wYH@cluster0.w7wg51i.mongodb.net/chatbot?retryWrites=true&w=majority")
        self.db = self.client["mercadolibre_db"]
        self.collection = self.db["mercadolibre_data"]

    def start_requests(self):
        alpha=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
         'W', 'X', 'Y', 'Z']
        for each_alpha in alpha[:]:
            url = f'https://www.mercadolibre.com.pe/glossary/{each_alpha}/1'
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        categories = response.xpath('//li[@class="seo-ui-glossary__keyword"]//a')
        for each_link in categories[:]:
            cat_link= each_link.xpath('.//@href').get('').strip()
            cat_name= each_link.xpath('.//text()').get('').strip()
            yield scrapy.Request(url=cat_link, headers=self.headers, callback=self.listing_page,
                                 meta={'cat_name':cat_name}
                                 )

    def listing_page(self, response):
        cat_name= response.meta.get('cat_name','')
        listing_div= response.xpath('//li[contains(@class, "ui-search-layout__item")]')
        for each_div in listing_div[:]:
            item= dict()
            link=each_div.xpath('.//h3//a//@href').get('').strip()
            item['link']=link
            item['title']=each_div.xpath('.//h3//a//text()').get('').strip()
            # item['price']=each_div.xpath('.//div[@class="poly-price__current"]//span[@class="andes-money-amount__fraction"]/text()').get('').strip()
            item['category']= cat_name
            yield scrapy.Request(url=link , headers=self.headers, callback=self.detail_page,
                                 meta={'item':item})

        next_page= response.xpath('//a[@title="Siguiente"]/@href').get('')
        if next_page:
            yield scrapy.Request(url=next_page, headers=self.headers, callback=self.listing_page,
                                 meta={'cat_name':cat_name})

    def detail_page(self, response):
        item= response.meta.get('item')
        item['price']= response.xpath('//meta[@itemprop="price"]/@content').get('').strip()
        item['images']= ",".join(response.xpath('//div[@class="ui-pdp-gallery__column"]//img/@data-zoom').getall())
        if item['price']:
            self.insert_or_update(item)

    def insert_or_update(self, item):
        existing_item = self.collection.find_one({"title": item['title']})
        pass
        if existing_item:
            if existing_item['price'] != item['price']:  # Price changed
                self.collection.update_one(
                    {"title": item['title']},
                    {"$set": {"price": item['price']}}
                )
                print(f"Updated price for {item['title']}")
            else:
                print(f"Item already exists, skipping {item['title']}")
        else:
            self.collection.insert_one(item)
            print(f"Inserted new item: {item['title']}")