import argparse
from bs4 import BeautifulSoup
from selenium import webdriver
import csv
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import os
import time

class Scrapper():
    def __init__(self, parent_url, csv_output):
        self.timeout = 2
        self.parent_url = parent_url
        self.csv_output = csv_output
        self.header = ["URL","Property Name","Rating","Normal Price","Price after Discount","Number of remaining rooms","District"]
        self.mapping_span = {
            0: "url",
            1: "rc-info__name",
            2: "rc-overview__rating-text",
            3: "rc-price__additional-discount-price",
            4: "rc-price__text",
            5: "rc-overview__availability",
            6: "rc-info__location",
        }

        # setup driver
        options = Options()
        chromedriver_path = '/usr/local/bin/chromedriver'
        s = Service(chromedriver_path)
        self.driver = webdriver.Chrome(service=s, options=options)

    
    def directory_exist(self, directory):
        # create folder
        if not os.path.exists(directory):
            os.makedirs(directory)
        directoryIsExist = True
        return directoryIsExist
        
    def get_pagination(self, html):
        parent = html.find("ul", class_ = "pagination pagination-overwrite")
        options = list(parent.descendants)
        paging = set()
        for i in range(2, len(options)):
            txt = options[i].text
            try:
                paging.add(int(txt))
            except:
                continue
        return (int(min(paging)),int(max(paging)))

    def write_to_csv(self, filename, data):
        with open(filename, 'a', newline='') as file:
            mywriter = csv.writer(file, delimiter=',')
            mywriter.writerows(data)
            file.close()

    def get_url(self, element):
        action = ActionChains(self.driver)
        action.click(on_element=element)
        action.perform()
        
        self.driver.current_window_handle
        parent = self.driver.window_handles[0]
        child = self.driver.window_handles[1]
        self.driver.switch_to.window(child)
        time.sleep(self.timeout)

        url = self.driver.current_url
        self.driver.close()
        self.driver.switch_to.window(parent)

        return url

    def get_schema(self, html, url_objects):
        rows = []
        kosts = html.find("div", class_ = "row").find_all(class_="col-md-3 col-sm-4 col-xs-12 col-custom-12")
        # TODO: url 
        for index, kost in enumerate(kosts):
            row = [""]*len(self.mapping_span)
            for col in self.mapping_span: 
                if col == 0 :
                    row[col] = self.get_url(url_objects[index])
                else:
                    tmp = kost.select(f"span.{self.mapping_span[col]}")
                    if len(tmp) == 0:
                        val = 0
                    else: 
                        val = (tmp[0].text).strip()
                    row[col] = val
            
            # filter empty data
            if row != [0]*len(self.mapping_span):
                rows.append(row)

        print("Number of rows ", len(rows))
        return rows
    
    def start(self):
        self.driver.get(self.parent_url)
        req = self.driver.page_source.encode('utf-8')
        html = BeautifulSoup(req, "html.parser")
        time.sleep(self.timeout)

        min, max = self.get_pagination(html)

        rows = []
        rows.append(self.header)
        for i in range(min, max):
            print("Processing page: ", i)
            
            # head
            self.driver.find_element(By.LINK_TEXT, str(i)).click()
            time.sleep(self.timeout)

            
            url_objects = self.driver.find_elements(By.CSS_SELECTOR, ".col-md-3")
            time.sleep(self.timeout)

            req = self.driver.page_source.encode('utf-8')
            page = self.get_schema(html, url_objects)
            rows = [*rows, *page]
            self.write_to_csv(self.csv_output, rows)
            print(f"Write {len(rows)} rows to {self.csv_output} file.")
            rows = []



        self.driver.quit()

def main(params):
    parent_url = params.url
    csv_output = params.csv
    scrap = Scrapper(parent_url, csv_output)
    scrap.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scraping mamikos data')
    parser.add_argument('--url', required=True, help='URL web  to be scraped')
    parser.add_argument('--csv', required=True, help='output filename to be written. format should be \'filename.csv\'')
    args = parser.parse_args()
    main(args)




    
        