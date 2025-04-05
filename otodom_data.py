import csv
import requests
import scrapy
import json
from datetime import datetime

from scrapy import Selector


class OtodomDataSpider(scrapy.Spider):
    name = "otodom_data"

    custom_settings = {
        'FEED_URI': f'outputs/otodom_{datetime.now().strftime("%d_%b_%Y_%H_%M_%S")}.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter'},
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 1,
        # 'DOWNLOAD_DELAY': 1,
        'REDIRECT_ENABLED': False,
        # "browserHtml": True,
    }
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': 'OptanonAlertBoxClosed=2024-11-06T04:20:42.327Z; eupubconsent-v2=CQHqC_gQHqC_gAcABBPLBOF8AP_gAAAAAAYgKaNV_G_fbXlj8Xp0aftkeY1f99h7rsQxBhfJk-4FyLvW_JwX32EyNA06pqYKmRIAu3RBIQFlGIDUBUCgaogVrTDMYECEgTNKJ6BEgFMRc2dYCF5vmYFD-QCY5tptd1d52R-t7dr83dzyy4Vnn3Kpf-YlUICdA5cgAAAAAAAAAAAAAAAQAAAAAAAAAQAIAAAAAAAAAAAAAAAAAAAAA_cAAEAAAAAAAAABwAAAGBIIAACAAFwAUABUADgAHgAQQAvADUAHgARAAmABVADeAHoAPwAhIBDAESAI4ASwAmgBgADDgGUAZYA2QBzwDuAO-AewB8QD7AP2Af4CAAEUgIuAjABGoCRAJLAT8BQYCoAKuAXMAvQBigDRAG0ANwAcSBHoEiAJ2AUOAo8BSICmwFsALkAXeAvMBhsDIwMkAZOAzMBnMDVwNZAbeA3MBuoDggHJgOXAm4EAMAAOABIAEcAg4BHACaAF9ASsAmUBNoCkAFhALEAW4AvIBf4DEAGLAMhAaMA1MBtADbgG6DgFYACIAHAAeABcAEgAPwAjgBoAEcAOQAgEBBwEIAIiARwAmgBUADpAJWATEAmUBNoCkwFdgLEAWoAtwBdAC_wGCAMQAYsAyEBkwDRgGpgNeAbQA2wBt0DcwN0AceA5aBzoHPgTbHQTAAFwAUABUADgAIIAXABqADwAIgATAAqwBcAF0AMQAbwA9AB-gEMARIAlgBNACjAGAAMMAZQA0QBsgDngHcAd4A9oB9gH6AP-AigCMQEdASWAn4CgwFRAVcAsQBc4C8gL0AYoA2gBuADiAHUAPsAi-BHoEiAJkATsAoeBR4FIQKaApsBVgCxQFsALdAXAAuQBdoC7wF5gL6AYaAx6BkYGSAMnAZUAywBmYDOQGmwNXA1gBt4DdQHFgOTAcuBNwCbwE4SABUABAADwA0ADkAI4AWIAvoCbQFJgLEAXkAwQBngDRgGpgNsAbcA3QBywDnwJtkIEAACwAKAAuABqAFUALgAYgA3gB6AEcAMAAc8A7gDvAH-ARQAlIBQYCogKuAXMAxQBtADqAI9AU0AqwBYoC0QFwALkAZGAycBnJKBIAAgABYAFAAOAA8ACIAEwAKoAXAAxQCGAIkARwAowBgADZAHeAPyAqICrgFzAMUAdQBEwCL4EegSIAo8BTQCxQFsALzgZGBkgDJwGcgNYAbeBNwCcJIAmABcAI4A7gCAAEHAI4AVABKwCYgE2gKTAW4Av8BiwDLAGeAN0AcsBNspA7AAXABQAFQAOAAggBkAGgAPAAiABMACqAGIAP0AhgCJAFGAMAAZQA0QBsgDnAHfAPwA_QCLAEYgI6AkoBQYCogKuAXMAvIBigDaAG4AOoAe0A-wCJgEXwI9AkQBOwChwFIQKaApsBVgCxQFsALgAXIAu0BeYC-gGGwMjAyQBk4DLAGcwNYA1kBt4DdQHBAOTAm8UAQgAXABIAC4AI4AjgByADuAH2AQAAg4BYgC6gGvAO2Af8BMQCbQFSAK7AW4AugBeQDBAGLAMmAZ4A0YBqYDXoG5gboA5YCbYE4S0AkAGoAwAB3AF6APsApoBVgDMwJuFgBIAywCOAI9ATEAm0BowDUwG6AOW.f_wAAAAAAAAA; laquesisff=euads-4389#gre-12226#rer-165#rer-166#rst-73#rst-74; _gcl_au=1.1.2092987328.1730866844; st_userID=GA1.2.1420536462.1730866844__unlogged; _fbp=fb.1.1730866845023.958655267204353100; _tt_enable_cookie=1; _ttp=qZJqXzYPtNbxlBQ6TZ_T0dCxaMF; __gfp_64b=wagrtXTaQK_f1_3opH8mB68mWbJly6jLjj5hASm9oEP.v7|1730866845|2; PHPSESSID=6esg0v7nd5tbp5245ei07rqhr7; mobile_default=desktop; ninja_user_status=unlogged; _hjSessionUser_3137429=eyJpZCI6IjAxY2UzOGE5LWZjZmUtNWI4Ni05OWM2LWRlNjhkZDA0MDJjMiIsImNyZWF0ZWQiOjE3MzA4ODc1Nzk0NTYsImV4aXN0aW5nIjp0cnVlfQ==; _ga_TB6YF8EGK4=GS1.1.1730887578.1.0.1730887580.58.0.0; dfp_user_id=6d1c20d6-bf03-4609-b954-4771d01cb507; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Nov+12+2024+15%3A16%3A41+GMT%2B0500+(Pakistan+Standard+Time)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=069369c7-23dd-40fc-b8cf-fadcc9f60fd5&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2Cgad%3A1&AwaitingReconsent=false&intType=1&geolocation=%3B; laquesis=eure-21103@a#eure-25548@a#eure-25554@a#eure-25561@b#eure-28555@b#eure-29465@c#eure-29745@b#eure-29774@b#eure-29876@a#eure-30095@b#eure-30288@a#eure-30358@a#eure-30402@a#eure-30600@b#eure-30633@a#eure-30741@a; _gid=GA1.2.833813065.1731406607; _gat_clientNinja=1; _ga_6PZTQNYS5C=GS1.1.1731406608.13.0.1731406608.60.0.0; _ga=GA1.1.1420536462.1730866844; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22h1CEPaCXkebY8tYkpxdy%22%2C%22expiryDate%22%3A%222025-11-12T10%3A16%3A49.944Z%22%7D; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22null%22%2C%22expiryDate%22%3A%222025-11-12T10%3A16%3A50.412Z%22%7D; lqstatus=1731407687|1931fe02eefx75e018aa|eure-30288#eure-30095|||0; __gads=ID=70595548afcb753b:T=1730867591:RT=1731406613:S=ALNI_MYNfT6-xnq2Rnpw7MvhQaB-YczsPA; __gpi=UID=00000f6bd9d92959:T=1730867591:RT=1731406613:S=ALNI_MbnhDeblIte_t3l1PdQpsiPi6wTBQ; __eoi=ID=40d96560a2dca621:T=1730867591:RT=1731406613:S=AA-AfjbGKcct9__nOJKvsxolhBKu; _ga_20T1C2M3CQ=GS1.1.1731406608.13.1.1731406614.54.0.0; onap=192ffb40e07x12da8e79-10-1931fe02eefx75e018aa-26-1731408420',
        'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjEzODkzNjgiLCJhcCI6IjEwOTM0NDAzMTAiLCJpZCI6IjY4NzU2YzIzYTU0Yzc5N2UiLCJ0ciI6IjJhZTJkMjQ3Y2VjZjg4OTUyMWRkMmZhMjExMjU2NmE0IiwidGkiOjE3MzE0MDY2Mjg0MzAsInRrIjoiMTcwNTIyMiJ9fQ==',
        'priority': 'u=1, i',
        'referer': 'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/mazowieckie/warszawa/warszawa/warszawa?viewType=listing',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'traceparent': '00-2ae2d247cecf889521dd2fa2112566a4-68756c23a54c797e-01',
        'tracestate': '1705222@nr=0-1-1389368-1093440310-68756c23a54c797e----1731406628430',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'x-nextjs-data': '1'
    }

    total_pages = ''

    def read_input_file(self):
        with open(f'input/Otodom_updated.csv','r')as file:
            data=list(csv.DictReader(file))
            return data

    def start_requests(self):
        page =1
        url = f"https://www.otodom.pl/_next/data/YmKstGTlulMrYEzH8ivcn/pl/wyniki/sprzedaz/mieszkanie/mazowieckie/warszawa/warszawa/warszawa.json?viewType=listing&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie&searchingCriteria=mazowieckie&searchingCriteria=warszawa&searchingCriteria=warszawa&searchingCriteria=warszawa&page={page}"
        yield scrapy.Request(url= url, headers=self.headers,
                             callback=self.parse,
                             meta={'url':url,'page':page})

    def parse(self, response):
        url= response.meta.get('url','')
        page= response.meta.get('page')
        pass


        a= response.text
        pass
        api_data=json.loads(response.text)
        total_pages = api_data.get('pageProps', {}).get('tracking',{}).get('listing',{}).get('page_count','')
        items= api_data.get('pageProps',{}).get('data',{}).get('searchAds',{}).get('items',[])
        for each_item in items[:]:
            try:
                item = dict()
                website_date = each_item.get('dateCreatedFirst', '')
                date_obj = datetime.strptime(website_date, '%Y-%m-%d %H:%M:%S')

                if (date_obj.year == 2024) and (date_obj.month in [7, 9]):
                    print("in the if")
                    print(date_obj)

                    slug = each_item.get('slug','')
                    totalPrice= each_item.get('totalPrice',{})
                    if totalPrice:
                        item['Price'] = totalPrice.get('value', '')
                    else:
                        item['Price'] =''

                    price_per_square_meter = each_item.get('pricePerSquareMeter') or {}

                    # Now safely access 'value'
                    item['Price per m2'] = price_per_square_meter.get('value', '')
                    roomsNumber = each_item.get('roomsNumber', '')
                    word_to_digit = {
                        "ONE": 1,"TWO": 2,"THREE": 3,"FOUR": 4,"FIVE": 5,"SIX": 6,"SEVEN": 7,
                        "EIGHT": 8,"NINE": 9,"TEN": 10,"ELEVEN": 11, "TWELVE": 12,"THIRTEEN": 13,"FOURTEEN": 14,
                        "FIFTEEN": 15, "SIXTEEN": 16,"SEVENTEEN": 17,"EIGHTEEN": 18,"NINETEEN": 19,"TWENTY": 20
                    }
                    rooms = word_to_digit.get(roomsNumber, '')
                    if rooms:
                        item['Rooms']= f'{rooms} pokoje'
                    else:
                        item['Rooms'] = ''

                    item['URL'] = f'https://www.otodom.pl/pl/oferta/{slug}'

                    sub_url= f'https://www.otodom.pl/pl/oferta/{slug}'
                    yield scrapy.Request(url=sub_url, headers=self.headers, callback=self.detail_page_sub,
                                         meta={'item':item})


            except Exception as e:
                print(e)
            else:
                print(date_obj)



        if int(page) <= int(total_pages):
            page= page +1
            url= url.replace(f'&page={page-1}', f'&page={page}')
            yield scrapy.Request(url=url, headers=self.headers,
                                 callback=self.parse,
                                 meta={'url': url, 'page': page})


    def detail_page_sub(self, response):
        item= response.meta.get('item')
        api= response.xpath('//script[@id="__NEXT_DATA__"]/text()').get('')
        api_text=json.loads(api)
        item['Location']=api_text.get('props',{}).get('pageProps',{}).get('ad',{}).get('location',{}).get('address',{}).get('district',{}).get('name','')
        item['slug']=api_text.get('props',{}).get('pageProps',{}).get('ad',{}).get('slug','')

        yield item

