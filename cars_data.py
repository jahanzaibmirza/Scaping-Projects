import scrapy
from datetime import datetime

class CarsDataSpider(scrapy.Spider):
    name = "cars_data"

    custom_settings = {
        'FEED_URI': f'outputs/carsdirect_data_{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.json',
        'FEED_FORMAT': 'json',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 0.6,
    }
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    unique_item=[]

    def start_requests(self):
        url='https://www.carsdirect.com/'
        yield scrapy.Request(url=url,callback=self.parse
                             )

    def parse(self, response):
        make = response.xpath("(//select[@aria-label='make dropdown selector'])[1]/option//@value").getall()
        for each_make in make[:]:
            listing_url=f'https://www.carsdirect.com/used_cars/listings/{each_make}'
            yield scrapy.Request(url=listing_url,callback=self.listing_page,
                                 meta={'each_make':each_make}
                                 )

    def listing_page(self, response):
        each_make= response.meta.get('each_make')
        links = response.xpath('//div[@class="listingBlock"]//a/@href').getall()
        for link in links[:]:
            detail_page_link = f'https://www.carsdirect.com{link}'
            yield scrapy.Request(url=detail_page_link,
                                 callback=self.detail_page,
                                 meta={'detail_page_link':detail_page_link,
                                       'each_make':each_make})

        next_page = response.xpath('//li[@class="arrowNavi"]//span[@class="chevronForward"]/parent::a/@href').get('')
        if next_page:
            next_page_url = f'https://www.carsdirect.com{next_page}'
            yield scrapy.Request(url=next_page_url, callback=self.listing_page,
                                 meta={'each_make':each_make})
    def detail_page(self, response):
        item= dict()
        item['detail_page_link']= response.meta.get('detail_page_link','').strip()
        item['make']= response.meta.get('each_make').replace('-',' ').title().strip()
        item['title']= response.xpath('//div[@class="contentWrapperVehicleDetail"]//h1//text()').get('').strip()
        item['trim_name']= response.xpath('//div[@class="trimName"]/text()').get('').strip()
        item['car_images']= ",".join(i.replace('w=293&h=220','w=1000&h=1000') for i in response.xpath('//div[@class="imageGalleryVDPWrapper"]//img/@src').getall())
        item['exterior_color']= response.xpath("//div[contains(text(),'Exterior Color')]//following-sibling::div[1]/text()").get('').strip()
        item['interior_color']= response.xpath("//div[contains(text(),'Interior Color')]//following-sibling::div[1]/text()").get('').strip()
        item['transmission']= response.xpath("//div[contains(text(),'Transmission')]//following-sibling::div[1]/text()").get('').strip()
        item['engine']= response.xpath("//div[contains(text(),'Engine')]//following-sibling::div[1]/text()").get('').strip()
        item['certified_preowned']= response.xpath("//div[contains(text(),'Certified Pre-Owned?')]//following-sibling::div[1]/text()").get('').strip()
        item['doors']= response.xpath("//div[contains(text(),'Doors')]//following-sibling::div[1]/text()").get('').strip()
        item['stock_id'] =response.xpath("//div[contains(text(),'Stock ID')]//following-sibling::div[1]/text()").get('').strip()
        item['vin']= response.xpath("//div[contains(text(),'VIN')]//following-sibling::div[1]/text()").get('').strip()
        item['list_price']= response.xpath("(//div[contains(text(),'List Price')])[1]/following-sibling::div/text()").get('').strip()
        item['loan_estimate']= response.xpath("(//div[contains(text(),'Loan Estimate')])[1]/following-sibling::div/text()").get('').strip()
        item['mileage']= response.xpath("(//div[contains(text(),'Mileage')])[1]/following-sibling::div/text()").get('').strip()
        item['dealer_info']= response.xpath('(//div[@class="dealerInfo"])[1]/text()').get('').strip()
        item['carfaxOwnerNumber']= response.xpath('(//div[@class="carfaxOwnerNumber"])[1]/text()').get('').strip()
        item['features']= ", ".join(x.strip() for x in response.xpath("//div[@class='featuresWrapper']//div//text()").getall()).strip().replace(', Read More,','').strip()
        item['seller_comments']= "".join(response.xpath('//div[@class="listingComments comment"]//text()').getall())
        item['interest_rate']= response.xpath('//input[@aria-label="Interest Rate"]/@value').get('').strip()
        item['down_payment_percentage']= response.xpath("//div[contains(text(),'Down Payment')]/following-sibling::div//span[contains(text(),'%')]/text()").get('').replace('|','').strip()
        item['down_payment_value']= response.xpath('//input[@aria-label="Down Payment"]/@value').get('').strip()
        item['dealer_name']= response.xpath('//div[@class="dealerName"]/text()').get('').strip()
        item['dealer_address']= "".join(response.xpath("//div[contains(@class,'dealerAddress')]//text()").getall()).strip()
        item['dealer_phone']= response.xpath("//a[contains(@class,'dealerPhoneNumber')]/@href").get('').replace('tel:','').strip()
        item['dealer_website']= response.xpath("//a[contains(@class,'dealerWebsite')]/@href").get('').strip()
        yield item