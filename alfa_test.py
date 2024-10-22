import math
import requests
import json
import re
import pdb
import os
import traceback
from dotenv import load_dotenv
load_dotenv()
import time
import main_sqs_data
all_years = ['1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
def remove_unwanted_chars(input_string):
    pattern = r'[^a-zA-Z0-9\s.,!?-]'
    cleaned_string = re.sub(pattern, '', input_string)
    cleaned_string = cleaned_string.replace('\n', '').replace('\t', '').replace('\r', '')
    return cleaned_string
for yer in range(1):
    yer = all_years[0]
    url = f'https://www.parliament.gov.za/actsjson?queries%5Btype%5D={yer}&sorts%5Bnumber%5D=-1&page=1&perPage=50&offset=0'
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Cookie': 'cookiesession1=678A3E0EC362073A67DB428BBDA6375C; _ga=GA1.4.375988548.1729521472; _gid=GA1.4.673850760.1729521473; _gid=GA1.3.673850760.1729521473; _ga_XFB4FZBSXX=GS1.4.1729521471.1.1.1729522239.13.0.0; _ga=GA1.3.375988548.1729521472; october_session=eyJpdiI6IjFqODQ4TTVmdzRyYjdhMlJQXC9wa3N3PT0iLCJ2YWx1ZSI6InB1WkE3Q2VWSTJNXC9KQytYaDRsVjNxWWc5cVhGOVwvS3p2MXp0b2UxcUdOK0lncDJEbmppT3hmcTc3ZUs2R0xKdkRJaDN5Y0l1enlQRWgyVjE2SFpETUE9PSIsIm1hYyI6IjI5MTVmYzc2ODQ1NDVhNTJjYTMwNzdiMDA0OGU5NDRiMzFjMGI2MzA5OGI3MzA2NmNiYjJhZTkzOTVmYTU3MTIifQ%3D%3D; _ga_XFB4FZBSXX=GS1.1.1729521471.1.1.1729522356.60.0.0',
        'Host': 'www.parliament.gov.za',
        'Referer': 'https://www.parliament.gov.za/acts?perPage=50&sorts[number]=-1&page=2&offset=50',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="101", "Google Chrome";v="101"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    sess = requests.Session()
    res = sess.get(url , headers=headers)
    data = None
    all_dataset = []
    def get_general_data(json_data):
        for rec in json_data:
            name = rec['name'].strip()
            name  = remove_unwanted_chars(name)
            name = name.replace('/' , ' ')
            location = 'https://www.parliament.gov.za/storage/app/media/'+rec['file_location']
            all_dataset.append({'name':name , 'url':location})
    if res.status_code == 200:
        data = res.json()
        total_count = int(data['queryRecordCount'])
        years = ['1995']
        def calculate_pages(total_count, items_per_page):
            return math.ceil(total_count / items_per_page)
        items_per_page = 50
        pages = calculate_pages(total_count, items_per_page)
        print(f"You will need {pages} pages. for this year : " , yer ,  "total_data --- " , total_count )
        if total_count>1:
            query_selector = data['records']
            check_data = get_general_data(json_data=query_selector)
            for j in range(2 , total_count):
                offset = 50*(j-1)
                url2 = f'https://www.parliament.gov.za/actsjson?queries%5Btype%5D={yer}&sorts%5Bnumber%5D=-1&page={str(j)}&perPage=50&offset={offset}'
                sess = requests.Session()
                res = sess.get(url2 , headers=headers)
                if res.status_code == 200:
                    data = res.json()
                    query_selector = data['records']
                    check_data = get_general_data(json_data=query_selector)
        else:
            query_selector = data['records']
            check_data = get_general_data(json_data=query_selector)

        if hasattr(main_sqs_data, 'main_func_data'):
            print("my_function imported successfully!")
            
            main_sqs_data.main_func_data(all_dataset)
        else:
            print("my_function not found.")

            # main_func_data(pdf_list_data=all_dataset)























