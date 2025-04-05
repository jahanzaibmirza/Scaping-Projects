import copy
import re
from datetime import datetime
import scrapy


class HappiDataSpider(scrapy.Spider):
    name = "happi_data"

    custom_settings = {
        'FEED_URI': f'output/happi_{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 0.5,
    }

    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://www.happi.com',
        'priority': 'u=1, i',
        'referer': 'https://www.happi.com/cmpl_listing/',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        # 'cookie': '_ga=GA1.1.1193457501.1743700569; __adroll_fpc=d2bf8a6b6adada667be9a12be9dbacba-1743700570151; __hs_cookie_cat_pref=1:true_2:true_3:true; __hstc=129142419.2842b8d102284bf67cd6ae1347426214.1743700569937.1743700569937.1743700569937.1; hubspotutk=2842b8d102284bf67cd6ae1347426214; __hssrc=1; __gads=ID=65922b4b71ebafea:T=1743700569:RT=1743702059:S=ALNI_Mb7LEbz_5ET82kNzEBFbfmqTFe_UA; __gpi=UID=00001006ab48b1c1:T=1743700569:RT=1743702059:S=ALNI_Mbl4Lpdj0BRB63NQN6LvxtN7GBsHw; __eoi=ID=8ce99e8234eeb76c:T=1743700569:RT=1743702059:S=AA-AfjZCyri9QLkTLAfXNqqz0t8T; __hssc=129142419.23.1743700569937; __ar_v4=N5SXMODQTJFBZAO5NU66ND%3A20250403%3A23%7CVGWAEFP6YJHY7EQDNAAH7G%3A20250403%3A23; cf_clearance=CcXJLRXlcmuS5PMaigS0jevglQ8YIn.tCekpLfXRUHA-1743702069-1.2.1.1-wDOiqQ0M_XnkXOjvSWRG4g9FVNYllC0k_L.dv69_tSrn0KPe1HNbXu5vhBHv8bix6LavHVWOqz_z4jr4eTYXh5g9fP.03PDhdRG0Fg4vs.c1PROBMDNVeJg55dL8l9uwWDzmo38VeOzLEVyJqur2vRwhNAIMLHmnk8xm.zM.T87V09yWmAl7nM2mDJuzKEHzJNsctdIWLRyKuZmBOX83z.AcHI_vWLnn_adgk1Tdpxp6H9.ax_ooYzwayWZmJAAUNfCLR5H4hJvAbejj5WsrRQ9K8RD6aZSHYEZPwZZTgGU0c07Fc_T5y21ybUqLO8jOb.FdqslNpKb6nKPogu38Nvhklkp1qRNnwkgFMP5P.Pw; _ga_1D3FXZM3WN=GS1.1.1743700569.1.1.1743702943.0.0.0',
    }

    url='https://www.happi.com/wp-admin/admin-ajax.php'
    data = {
        'action': 'cmpl_filter_posts_by_title_first_letter',
        'firstLatter': 'A',
        'c_cat': '',
        'country': '',
        'state': '',
    }

    def start_requests(self):
        alphabets=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
         'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
        for i in alphabets[:]:
            payloads=copy.deepcopy(self.data)
            payloads['firstLatter']= str(i)
            yield scrapy.FormRequest(url=self.url, headers=self.headers, callback=self.parse,
                                     method='post', formdata=payloads
                                     )
    def parse(self, response):
        links=response.xpath('//ul//li//a[@class="arrow-btn"]/@href').getall()
        for each_link in links[:]:
            yield scrapy.Request(url=each_link, headers=self.headers, callback=self.detail_page)



    def detail_page(self, response):
        item= dict()
        item['Company Name']=response.xpath('//div[@class="brand-profile-contant-title-wrap"]//h2/text()').get('').strip()
        item['Description']="\n".join(response.xpath("//div[contains(@class,'brand-profile-description')]//p//text()").getall()).strip()
        item['Headquarters_Address']=response.xpath("//h3[contains(text(),'Company Headquarters')]/following-sibling::p[1]/text()").get('').strip()
        match = re.search(r'var\s+phoneNumber\s*=\s*["\']([^"\']+)["\']', response.text)
        item['Phone Number'] = match.group(1) if match else ''
        match1 = re.search(r"window\.open\(['\"](https?://[^'\"]+)['\"]", response.text)
        item['Website'] = match1.group(1) if match1 else ''
        item['Categories Listed'] = ", ".join(i.strip() for i in response.xpath('//div[@class="oleos-categories-wrap"]//h3/text() | //div[@class="oleos-categories-wrap"]//div[@class="buyer-guide-accordion-content"]//a/text()').getall()).strip()
        item['Image Link']=response.xpath('//img[@alt="brand-profile-thumb"]/@src').get('').strip()
        item['Detail Page'] = response.url
        yield item