import csv
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import scrapy
import requests,json,time,threading,queue,os
from selenium.webdriver.common.by import By


class SportsDataSpider(scrapy.Spider):
    name = "sports_data"

    def get_driver(self):
        # FOR GOOGLE CHROME
        chrome_options = Options()
        chrome_options.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.maximize_window()
        return self.driver
    def append_to_csv(self,file_name, data, headers):
        file_exists = os.path.isfile(file_name)
        with open(file_name, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

    custom_settings = {'ROBOTSTXT_OBEY': False,
                       'RETRY_TIMES': 5,
                       'DOWNLOAD_DELAY': 0.4,
                       'CONCURRENT_REQUESTS': 1,
                       'HTTPERROR_ALLOW_ALL': True,
                       'FEED_FORMAT': 'csv',
                       'FEED_EXPORT_ENCODING': 'utf-8',
                       }

    def start_requests(self):
        url = 'https://quotes.toscrape.com/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            driver = webdriver.Chrome()
            driver.maximize_window()
            driver.get('https://public.totalglobalsports.com/public/event/3312/schedules/26360')

            all_teams_list = list(set(
                (element.text, element.get_attribute("href"))
                for element in driver.find_elements(By.XPATH, "//a[@class='team-name']")
            ))
            pass
            for team_name, team_link in all_teams_list[:]:
                driver.get(team_link)
                time.sleep(5)
                print("Processing team:", team_name, team_link)

                view_profile_links = [
                    element.get_attribute("href") for element in
                    driver.find_elements(By.XPATH, "//a[contains(text(),'View Profile')]")
                ]

                for each_profile in view_profile_links[:]:
                    # changes
                    item= dict()
                    modify_profile= each_profile.split('pid=')[1]
                    person_url=f'https://recruiting.totalglobalsports.com/api/player/get-player-basic-info/{modify_profile}/0'
                    print('person_url',person_url)
                    headers = {
                        'accept': 'application/json, text/plain, */*',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                    }

                    response = requests.get(person_url, headers=headers)
                    print(response.text)
                    if response.status_code == 200:
                        parent_api = json.loads(response.text)
                        data = parent_api.get('data', {})
                        item[f'url'] = team_link
                        item[f'First_name'] = data.get('firstName', '')
                        item[f'Last_name'] = data.get('lastName', '')
                        item[f'Grad Year'] = data.get('gradYear', '')
                        item[f'Email'] = data.get('email', '')
                        item[f'Cellphone'] = data.get('phone', '')
                        item[f'Team'] = team_name

                    api_url = f'https://recruiting.totalglobalsports.com/api/player/get-family-info/{modify_profile}'
                    print('parent',api_url)
                    headers = {
                        'accept': 'application/json, text/plain, */*',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
                    }

                    response = requests.get(api_url, headers=headers)
                    print(response.text)
                    if response.status_code == 200:
                        parent_api = json.loads(response.text)
                        data = parent_api.get('data', [])
                        for i, each_data in enumerate(data[:2], start=1):
                            item[f'parent_firstName_{i}'] = each_data.get('firstName', '')
                            item[f'parent_lastName_{i}'] = each_data.get('lastName', '')
                            item[f'parent_email_{i}'] = each_data.get('email', '')
                            item[f'parent_phone_{i}'] = each_data.get('phone', '')
                    time.sleep(2)
                    csv_headers = [
                        "url","First_name", "Last_name", "Cellphone", "Email", "Grad Year", "Team",
                        "parent_firstName_1", "parent_lastName_1", "parent_email_1", "parent_phone_1",
                        "parent_firstName_2", "parent_lastName_2", "parent_email_2", "parent_phone_2",
                    ]

                    csv_file = "califoniaB2009.csv"
                    self.append_to_csv(csv_file, item, csv_headers)

                    time.sleep(2)

        except Exception as e:
            print(f"Error encountered: {e}. Skipping to next item.")
