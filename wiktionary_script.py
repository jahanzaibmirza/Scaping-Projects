import scrapy
from datetime import datetime

class WikiScriptSpider(scrapy.Spider):
    name = "wiki_script"

    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.5,
                       'HTTPERROR_ALLOW_ALL': True,
                       'FEED_URI': f'outputs/listing_data_{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
                       'FEED_FORMAT': 'csv',
                       'FEED_EXPORT_ENCODING': 'utf-8'}

    def start_requests(self):
        url='https://en.wiktionary.org/wiki/Category:Welsh_language'
        yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        main_links= response.xpath("//p[contains(text(),'Welsh has no descen')]//following-sibling::ul//li/a/@href").getall()
        for each_link in main_links[:]:
            yield response.follow(url=each_link, headers=self.headers, callback=self.sub_page_1)



    def sub_page_1(self, response):
        sublinks= response.xpath('//div[@id="mw-subcategories"]//div[@class="CategoryTreeItem"]//bdi/a/@href').getall()
        for link in sublinks:
            yield response.follow(url=link, headers=self.headers, callback=self.sub_page_2)

    def sub_page_2(self, response):
        sublinks = response.xpath('//div[@id="mw-subcategories"]//div[@class="CategoryTreeItem"]//bdi/a/@href').getall()
        for link in sublinks:
            yield response.follow(url=link, headers=self.headers, callback=self.sub_page_3)


    def sub_page_3(self, response):
        links = response.xpath('//div[@class="mw-category-generated"]//ul//li//a/@href').getall()
        for each_detail_page in links[1:]:
            yield response.follow(url=each_detail_page, headers=self.headers, callback=self.detail_page)

    def detail_page(self, response):
        item= dict()
        item['Url']=response.url
        item['Etymology']="".join(response.xpath("//h2[@id='Welsh']//following::h3[contains(text(),'Etymology')]//following::p[1]//text()").getall()).strip()
        item['Pronunciation']= "".join(response.xpath("//h2[@id='Welsh']//following::h3[contains(text(),'Pronunciation')]//following::ul[1]//li//text()").getall()).strip()
        item['Noun']= "".join(response.xpath("//h2[@id='Welsh']//following::h3[contains(text(),'Noun')]//following::p[1]//text()").getall()).strip()
        item['Noun Points']= "".join(response.xpath("//h2[@id='Welsh']//following::h3[contains(text(),'Noun')]//following::ol[1]//text()").getall()).strip()
        item['Derived terms']= " ".join(response.xpath("//h2[@id='Welsh']//following::h4[contains(text(),'Derived terms')]/following::ul[1]//li//text()").getall()).strip()
        yield item