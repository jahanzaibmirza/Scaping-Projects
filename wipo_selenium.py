import csv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import os
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(script_dir, "output")
os.makedirs(download_folder, exist_ok=True)


def get_driver():
    options = uc.ChromeOptions()

    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    driver = uc.Chrome(options=options)
    return driver
    print('driver installed')

def read_input_file():
    list_of_urls=[]
    with open(f'input/url.csv','r')as rfile:
        data =list(csv.DictReader(rfile))
        for each_row in data:
            link=each_row.get('link','')
            list_of_urls.append(link)
    return list_of_urls

file_data = read_input_file()

print("Start Running the Script")
driver = get_driver()
try:

    pass
    for url in file_data:
        driver.get(url)
        print('hitting the url of the webiste')
        time.sleep(10)
        Document_Button = driver.find_element(By.XPATH,
                                              '//a[contains(text(),"Documents")] | //font[contains(text(),"Documents")]')
        Document_Button.click()
        print('clicking on the document button')
        time.sleep(4)

        xml_download_file = driver.find_element(By.XPATH,
                                                '(//table[@class="b-table"]//td//a[@class="ps-downloadables"][contains(text(),"XML")])[2] | (//table[@class="b-table"]//td//a[@class="ps-downloadables"]//font[contains(text(),"XML")])[2]')
        xml_download_file.click()
        time.sleep(8)
        print('start download the files ')
        downloaded_files = os.listdir(download_folder)
        print('Execution Completed')
except Exception as e:
    print(e)

finally:
    driver.quit()




















