import scrapy
from datetime import datetime
import pandas as pd
class WaterDataSpider(scrapy.Spider):
    name = "water_data"

    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.5,
                       'CONCURRENT_REQUESTS': 1,
                       'HTTPERROR_ALLOW_ALL': True,
                       'FEED_EXPORT_ENCODING': 'utf-8'}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    items_list = []

    def start_requests(self):
        url='https://water.epa.state.il.us/dww/JSP/SearchDispatch?number=&name=&county=All&WaterSystemType=All&SourceWaterType=All&PointOfContactType=None&SampleType=null&begin_date=10%2F11%2F2022&end_date=10%2F11%2F2024&action=Search+For+Water+Systems'
        yield scrapy.Request(url=url, headers=self.headers, callback=self.parse,
                             )

    def parse(self, response):
        links= response.xpath("//td//a/@href").getall()
        for eachlink in links[3:]:
            url=f'https://water.epa.state.il.us/dww/JSP/{eachlink}'
            yield scrapy.Request(url=url, headers=self.headers, callback=self.detail_page,
                                 meta={'url':url})

    def detail_page(self, response):
        item=dict()
        item['Page Url ']=response.meta.get('url')
        item['Water System Name']= response.xpath("//font[contains(text(),'Water System Name')]//parent::b//parent::td/following-sibling::td[1]/font/text()").get('').strip()
        item['Principal County Served']=response.xpath("//font[contains(text(),'Principal County Served')]//parent::b//parent::td/following-sibling::td[1]/font/text()").get('').strip()
        item['Status']=response.xpath("//font[contains(text(),'Status')]//parent::b//parent::td/following-sibling::td[1]/font/text()").get('').strip()
        item['Federal Type']=response.xpath("//font[contains(text(),'Federal Type')]//parent::b//parent::td/following-sibling::td[1]/font/text()").get('').strip()
        item['State Type']=response.xpath("//font[contains(text(),'State Type')]//parent::b//parent::td/following-sibling::td[1]/font/text()").get('').strip()
        item['Primary Source']=response.xpath("//font[contains(text(),'Primary Source')]//parent::b//parent::td/following-sibling::td[1]/font/text()").get('').strip()

        info = response.xpath('//table[@summary="Summary Points Of Contact"]//tr')
        for i,each_info in enumerate(info[1:], start=1):
            item[f'Point_Name{i}'] = each_info.xpath(".//td[1]//font/text()").get('')
            item[f'Job_Title{i}'] = each_info.xpath(".//td[2]//font/text()").get('').strip()
            item[f'Point_Type{i}'] = each_info.xpath(".//td[3]//font/text()").get('').strip()
            item[f'Point_Phone{i}'] = each_info.xpath(".//td[4]//font/text()").get('').strip()
            item[f'Point_Address{i}'] = " ".join([address.strip() for address in each_info.xpath(".//td[5]//font/text()").getall()])
            item[f'Point_Email{i}'] = each_info.xpath(".//td[6]//font/a/text()").get('').strip()

                                                 # annual report
        population_served_table= response.xpath('//table[@summary="Summary of Population Served"]//tr')
        for i, each_population in enumerate(population_served_table[1:], start=1):
            item[f'Start Month_{i}']= each_population.xpath('.//td[@headers="smonth"]/text()').get('').strip()
            item[f'Start Day_{i}']= each_population.xpath('.//td[@headers="sday"]/text()').get('').strip()
            item[f'End Month_{i}']= each_population.xpath('.//td[@headers="emonth"]/text()').get('').strip()
            item[f'End Day_{i}']= each_population.xpath('.//td[@headers="eday"]/text()').get('').strip()
            item[f'Population Type_{i}']= each_population.xpath('.//td[@headers="ptype"]/text()').get('').strip()
            item[f'Population Served_{i}']= each_population.xpath('.//td[@headers="pserved"]/text()').get('').strip()

                                               # //Service Connections
        service_connection = response.xpath('//table[@summary="Summary of Service Connection"]//tr')
        for i, each_service in enumerate(service_connection[1:], start=1):
            item[f'Type_{i}'] = each_service.xpath('.//td[@headers="type"]/text()').get('').strip()
            item[f'Count_{i}'] = each_service.xpath('.//td[@headers="count"]/text()').get('').strip()
            item[f'Meter Type_{i}'] = each_service.xpath('.//td[@headers="mtype"]/text()').get('').strip()
            item[f'Meter Size Measure_{i}'] = each_service.xpath('.//td[@headers="msize"]/text()').get('').strip()


        source_water = response.xpath("//font[contains(text(),'Sources of Water')]//following::table[@summary='Details about Sources of Water'][1]//tr")
        for i,each_water in enumerate(source_water[1:], start=1):
            item[f'Name_{i}'] = each_water.xpath('.//td[1]/text()').get('').strip()
            item[f'Type_Code_{i}'] = each_water.xpath('.//td[2]/text()').get('').strip()
            item[f'Water Status_{i}'] = each_water.xpath('.//td[3]/text()').get('').strip()

                                        # service area
        service_area = response.xpath('//table[@summary="Summary of Service Area"]//tr')
        for i ,each_service_area in enumerate(service_area[1:], start=1):
            item[f'Code_{i}'] = each_service_area.xpath('.//td[@headers="code"]/text()').get().strip()
            item[f'Service Name_{i}'] =each_service_area.xpath('.//td[@headers="sname"]/text()').get('').strip()
                                                     #water purchases
        water_purchase = response.xpath("//font[contains(text(),'Water Purchases')]//following::table[@summary='Details about Sources of Water']//tr")
        for i, each_water in enumerate(water_purchase[1:], start=1):
            item[f'Seller Water No_{i}']= each_water.xpath('.//td[@headers="name"]/text()').get('').strip()
            item[f'Water System Name_{i}'] = each_water.xpath('.//td[@headers="tcode"]/text()').get('').strip()
            item[f'Seller Facility Type_{i}'] = each_water.xpath('.//td[@headers="status"][1]/text()').get('').strip()
            item[f'Seller State_Asgn ID No_{i}']= each_water.xpath('.//td[@headers="status"][2]/text()').get('').strip()
            item[f'Buyer Facility Type_{i}']	 = each_water.xpath('.//td[@headers="status"][3]/text()').get('').strip()
            item[f'Buyer State_Asgn ID No_{i}'] = each_water.xpath('.//td[@headers="status"][4]/text()').get('').strip()

        # yield item
        self.items_list.append(item)
        print(self.items_list)
        df = pd.DataFrame(self.items_list)
        df.to_csv('outputs/water_epa_main_data.csv', index=False)
