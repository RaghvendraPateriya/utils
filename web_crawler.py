ROOT_URL = 'https://www.pmindia.gov.in/en/pm-visits/'
BASE_URL = 'https://www.pmindia.gov.in/en/pm-visits/page/{page_no}/?visittype=domestic_visit#'

import csv
import requests
from bs4 import BeautifulSoup

data = []

def get_model_content(base_url):
    print("Fetching :", base_url)
    html_text = requests.get(base_url).text
    soup = BeautifulSoup(html_text, 'html.parser')
    visited_lists = soup.find('ul', {'class': 'visit-list'})
    #print(visited_lists)
    lis = visited_lists.find_all('a', {'data-toggle': 'modal'})
    for li in lis:
        outer_a = li.text
        data_target = li.get('data-target').split('#')[1]
        data_div = visited_lists.find('div', {'id': data_target})
        try:
            table_rows = data_div.find_all('tr')
            if not table_rows:
                data.append([outer_a.replace("\n", ""), 'NOT_AVAILABLE', 'NOT_AVAILABLE', 'NOT_AVAILABLE'])
                continue
            table_rows = table_rows[1]
            table_tds = table_rows.find_all('td')
            date = table_tds[0].text
            state_place = table_tds[1].text
            event = table_tds[2]
            data.append([outer_a.replace("\n", ""), date.replace("\n", ""), state_place.replace("\n", ""), event.text.replace("\n", "")])
        except Exception as e:
            print(e)
            data.append([outer_a.replace("\n", ""), '', '', ''])
        #print(date.replace("\n", ""), state_place.replace("\n", ""))
        #csvwriter.writerow([outer_a, date, state_place, event.text])
        #print(outer_a.replace("\n", ""))
        


def get_data():
    with open('./output.csv', 'w',  newline='', errors='ignore') as file_obj:
        csvwriter = csv.writer(file_obj)
        csvwriter.writerow(['Heading', 'Date', 'States  &  Places', 'Events'])
        for page_no in range(1, 65): # 1->64
            BASE_URL = f'https://www.pmindia.gov.in/en/pm-visits/page/{page_no}/?visittype=domestic_visit#'
            print(BASE_URL)   
            get_model_content(BASE_URL)  
        print(len(data))
        csvwriter.writerows(data)
        ###### For testing a page
        # BASE_URL = f'https://www.pmindia.gov.in/en/pm-visits/page/2/?visittype=domestic_visit#'
        # print(BASE_URL)
        # get_model_content(BASE_URL)
        # csvwriter.writerows(data)


get_data()
