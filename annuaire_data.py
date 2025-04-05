import csv
from selenium.webdriver.chrome.options import Options
import os
from selenium.webdriver.common.by import By
from selenium import webdriver
import scrapy
import time
class AnnuaireDataSpider(scrapy.Spider):
    name = "annuaire_data"

    def get_driver(self):
        chrome_options = Options()
        chrome_options.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.maximize_window()
        return self.driver

    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.3,
        'REDIRECT_ENABLED': False,}

    def append_to_csv(self,file_name, data, headers):
        file_exists = os.path.isfile(file_name)
        with open(file_name, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

    all_items_list_of_list = []


    def read_input_file(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as rfile:
                data = list(csv.DictReader(rfile))
                return data
        except UnicodeDecodeError:
            try:
                with open(file_path, 'r', encoding='latin1') as rfile:
                    data = list(csv.DictReader(rfile))
                    return data
            except UnicodeDecodeError:
                print(f"Skipping file due to encoding issues: {file_path}")
                return None

    def start_requests(self):
        url = 'https://quotes.toscrape.com/'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            driver = self.get_driver()
            file_path = 'input/siren.csv'
            file_data = self.read_input_file(file_path)

            if file_data:
                for each_row in file_data:
                    try:
                        siren_id = each_row.get('SIREN', '')
                        COMPANY = each_row.get('COMPANY', '')
                        address_url = f'https://annuaire-entreprises.data.gouv.fr/entreprise/{siren_id}?redirected=1'
                        driver.get(address_url)
                        time.sleep(5)
                        # Try to find the address
                        try:
                            address = driver.find_element(By.XPATH,
                                                          "//a[contains(text(),'Adresse postale')]//following::td[1]//span").text
                        except:
                            address = "Not Found"
                        time.sleep(3)
                        main_url = f'https://annuaire-entreprises.data.gouv.fr/dirigeants/{siren_id}'
                        driver.get(main_url)
                        time.sleep(5)
                        dara = driver.page_source
                        sel = scrapy.Selector(text=dara)
                        names = sel.xpath('//tr//td[2]')
                        for each_name in names:
                            item = dict()
                            name = (each_name.xpath('.//strong//following::text()[1]').get('') or '').strip()
                            main_name = name.split(', né(e) ')[0] if ', né(e) ' in name else name
                            flname = main_name.split(' ') if main_name else ['']
                            item['SIREN'] = siren_id
                            item['Company'] = COMPANY
                            item['Name'] = main_name.replace(':','')
                            item['First_Name'] = flname[0].replace(',', '').replace(':','')
                            item['Last_Name'] = flname[-1].replace('(', '').replace(')', '').replace(':','')
                            item['Address'] = address
                            yield item
                            csv_headers = ["SIREN", "Company", "Name", "First_Name", "Last_Name", "Address"]
                            csv_file = "outputs/data_file.csv"
                            self.append_to_csv(csv_file, item, csv_headers)

                    except Exception as e:
                        print(f"Error processing SIREN {siren_id}: {e}")
                        continue  # Move to the next SIREN in case of an error

            driver.close()
        except Exception as e:
            print(f"Critical error: {e}")

