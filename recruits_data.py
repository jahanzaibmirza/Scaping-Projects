import copy
import json
import os
import csv

import scrapy


class RecruitsDataSpider(scrapy.Spider):
    name = "recruits_data"

    headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'bearer': 'fc8356c5a28329f45f6213821bcf152b',
        'content-type': 'application/json',
        'origin': 'https://my.sportsrecruits.com',
        'priority': 'u=1, i',
        'referer': 'https://my.sportsrecruits.com/recruits?filters=eyJub3RDb21taXR0ZWQiOnRydWUsImNsYXNzWWVhciI6W3sibGFiZWwiOiIyMDI2IiwidmFsdWUiOjIwMjZ9LHsibGFiZWwiOiIyMDI3IiwidmFsdWUiOjIwMjd9LHsibGFiZWwiOiIyMDI4IiwidmFsdWUiOjIwMjh9XX0%3D',
        'revision-hash': 's1742499184',
        'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        'Cookie': 'AWSALB=5goGEX5D5PzWQF06ZwqvUx3nT/6taUmSa/HK4t1w/QK1URS3pw7cELzXSZ6L5rDrFsUJPQWl59to1QRrTVVY5M4oTpKWMWtY8nszgomxbidzTpbgtUpMIq5lSabZ; AWSALBCORS=5goGEX5D5PzWQF06ZwqvUx3nT/6taUmSa/HK4t1w/QK1URS3pw7cELzXSZ6L5rDrFsUJPQWl59to1QRrTVVY5M4oTpKWMWtY8nszgomxbidzTpbgtUpMIq5lSabZ'
    }

    url = "https://api.sportsrecruits.com/api/v1/athletes"

    payload = {
        "category": "new",
        "page": 1,
        "filters": {
            "notCommitted": True,
            "classYear": [
                2026,
                2027,
                2028
            ]
        },
        "searchRunId": 836
    }
    def append_to_csv(self,file_name, data, headers):
        file_exists = os.path.isfile(file_name)
        with open(file_name, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

    all_items_list_of_list = []

    def start_requests(self):
        page=1
        payload= copy.deepcopy(self.payload)
        payload['page']=page
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse,
                             method='post', body=json.dumps(payload),
                             meta={'page':page,
                                   'payload':payload
                                   }
                             )

    def parse(self, response):
        page= response.meta.get('page')
        payload= response.meta.get('payload')

        text =response.text
        api_data=json.loads(text)
        data=api_data.get('data',[])
        total_pages= api_data.get('meta',{}).get('pagination',{}).get('total_pages','')
        for each_row in data[:]:
            player_id= each_row.get('id','')
            attributes= each_row.get('attributes',{})

            url = f"https://api.sportsrecruits.com/api/v2/resources/athletes/{player_id}?query%5Binclude%5D%5B%40fragments%5D%5B%5D=athlete-profile%3Abasics&query%5Binclude%5D%5BeventBeaconVideoCount%5D=true&query%5Binclude%5D%5BsimilarUncommittedAthletesCount%5D=true"
            headers = {
                'accept': 'application/json',
                'accept-language': 'en-US,en;q=0.9',
                'bearer': '4bb1013d5dcca232f7d2f33a802ae6ff',
                'content-type': 'application/json',
                'origin': 'https://my.sportsrecruits.com',
                'priority': 'u=1, i',
                'referer': 'https://my.sportsrecruits.com/athlete/lorenzo_riccuiti',
                'revision-hash': 's1742499184',
                'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                'Cookie': 'AWSALB=OOlUMF+eYrckav5so1rGFD6+phrs5hJdjJjKtgepzuA/y1VTyr4s0AvoBwChCN6PpwMKkn0TRFion5SLOj4S5jvCQFxfSp7fWY22/76lhFBjkt7bj35onx9cNZZv; AWSALBCORS=OOlUMF+eYrckav5so1rGFD6+phrs5hJdjJjKtgepzuA/y1VTyr4s0AvoBwChCN6PpwMKkn0TRFion5SLOj4S5jvCQFxfSp7fWY22/76lhFBjkt7bj35onx9cNZZv'
            }
            yield scrapy.Request(url=url, headers=headers,callback=self.detail_page,meta={'player_id':player_id})


        if page <= total_pages:
            page= page+1
            payload['page'] = page
            yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse,
                                 method='post', body=json.dumps(payload),
                                 meta={'page': page,
                                       'payload': payload
                                       }
                                 )

    def detail_page(self, response):
        player_id= response.meta.get('player_id','')
        text=response.text
        api_data=json.loads(text)
        resources=api_data.get('resources',{}).get('athletes',{})
        palyer_Data= resources.get(f'{player_id}')
        url= palyer_Data.get('urlSlug','')

        item= dict()
        item['url']=f'https://my.sportsrecruits.com/athlete/{url}' if url else ''
        item['firstName']= palyer_Data.get('firstName','')
        item['lastName']= palyer_Data.get('lastName','')
        item['grad_year']= palyer_Data.get('classYear','')
        item['phoneNumber']= palyer_Data.get('phoneNumber','')
        item['email']= palyer_Data.get('emailAddress','')
        gardian_dict = api_data.get('resources', {}).get('contacts', {})

        parent_ids = list(gardian_dict.keys())
        for index, parent_id in enumerate(parent_ids[:3], start=1):
            parent_Data = gardian_dict[parent_id]

            # Store in item dynamically
            item[f'parent_firstName_{index}'] = parent_Data.get('firstName', '')
            item[f'parent_lastName_{index}'] = parent_Data.get('lastName', '')
            item[f'parent_phoneNumber_{index}'] = parent_Data.get('phoneNumber', '')
            item[f'parent_email_{index}'] = parent_Data.get('emailAddress', '')

        print(item)
        # yield item
        csv_headers = [
            "url", "firstName", "lastName", "grad_year", "phoneNumber", "email",
            "parent_firstName_1", "parent_lastName_1", "parent_phoneNumber_1", "parent_email_1",
            "parent_firstName_2", "parent_lastName_2", "parent_phoneNumber_2", "parent_email_2",
            "parent_firstName_3", "parent_lastName_3", "parent_phoneNumber_3", "parent_email_3",
        ]
        csv_file = "outputs/recruits_Data.csv"
        self.append_to_csv(csv_file, item, csv_headers)

