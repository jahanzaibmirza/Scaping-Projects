import csv
from scrapy import Selector
import undetected_chromedriver as uc
from parsel import Selector

import csv
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By

import time
from selenium.webdriver.support.wait import WebDriverWait
import scrapy
import os

class TwsDataSpider(scrapy.Spider):
    name = "tws_data"

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        # 'Cookie': 'ASP.NET_SessionId=judkv3ytmqynp4bpqtnmypsb; preferredCulture=pl-PL; CookieConsent={stamp:%27-1%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27implied%27%2Cver:1%2Cutc:1731235986715%2Cregion:%27PK%27}; _ga=GA1.1.1018428351.1731235986; _fbp=fb.1.1731235987046.687427883133626191; _ga_FW7V6TQGLT=GS1.1.1731260515.4.1.1731261012.59.0.0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.4,
                       'FEED_URI': f'outputs/tws_data_{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
                       'FEED_FORMAT': 'csv',  #
                       'FEED_EXPORT_ENCODING': 'utf-8',

                       }

    def read_file(self):
        with open('input/tws_input.csv','r')as file:
            data=list(csv.DictReader(file))
            return data

    def get_driver(self):
        options = uc.ChromeOptions()
        if not hasattr(options, 'headless'):
            options.headless = False
        driver = uc.Chrome(options=options)
        return driver


    def start_requests(self):
        url = 'https://quotes.toscrape.com/'
        yield scrapy.Request(url=url, callback=self.parse)
    def parse(self, response):
        driver = self.get_driver()
        # self.driver
        item= dict()
        file_data= self.read_file()
        for each_rew in file_data[:]:
            try:
                detail_page_url= each_rew.get('detail_page_url','')
                driver.get(detail_page_url)
                time.sleep(25)
                item['detail_page'] = detail_page_url
                html=driver.page_source
                sel=Selector(text=html)
                new_stand=sel.xpath('//div[@id="popup-stand-mobile"]//span//text()').get('').strip()
                item['Company Name']= driver.find_element(By.XPATH, "//div[@class='popup-header']//div[@class='popup-name ng-binding']").text
                item['Company Description']= driver.find_element(By.XPATH, "//div[contains(@class,'popup-desc')]").text
                item['Company Website']= driver.find_element(By.XPATH, "//i[@class='fa fa-globe']//parent::div/following-sibling::a").get_attribute('href')
                address = driver.find_elements(By.XPATH, '//div[@class="popup-address"]//div')
                item['Company Location'] = " ".join([element.text.strip() for element in address]).strip().replace('\n',' ')
                item['Company Phone'] = driver.find_element(By.XPATH, '//div[contains(@class,"popup-phone")]').text
                item['Company Email'] = driver.find_element(By.XPATH, '//div[contains(@class,"popup-email")]//p').text
                item['Company stand'] = new_stand
                pass
                yield item
                print(item)
            except Exception as e:
                print(e)

