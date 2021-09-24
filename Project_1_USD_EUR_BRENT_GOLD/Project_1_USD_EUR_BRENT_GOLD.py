from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from datetime import date
import time
import re
import requests
from requests import request
import pandas as pd
import random
import vk_api
import json
from urllib.parse import urlencode
from bs4 import BeautifulSoup


# Defining DAG settings

default_args = {
    'owner': 'vl.stratu',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 0
}

dag = DAG('USD_EUR_BRENT_GOLD_daily_report',
          default_args=default_args,
          catchup=False,
          schedule_interval='45 5,11 * * *')


# Read data(BRENT & GOLD) from RBC

# BRENT

t = request('GET', 'https://quote.rbc.ru/ticker/181206').text
with open('BRENT.html', 'w', encoding='utf-8') as f:
    f.write(t)

with open("BRENT.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('$'))
a = []
for i in strings:
    if i != '\n':
        a.append(i.strip())
    else:
        continue
for i in range(len(a)):
    if a[i] == 'BRENT' and a[i+1].startswith('$'):
        brent = a[i+1] + a[i+2]
        break

# GOLD

t_2 = request('GET', 'https://quote.rbc.ru/ticker/101039').text
with open('GOLD.html', 'w', encoding='utf-8') as f:
    f.write(t_2)
b = []
with open("GOLD.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings_2 = soup.find_all(string=re.compile('$'))
b = []
for i in strings_2:
    if i != '\n':
        b.append(i.strip())
    else:
        continue

for i in range(len(b)):
    if b[i] == 'GOLD' and b[i+1].startswith('$'):
        gold = b[i+1] + b[i+2]
        break


# Read data(USD & EUR) from CBR

# USD

today_day = datetime.now().date().strftime('%d.%m.%Y')
yesterday = datetime.strftime(datetime.now() - timedelta(1), '%d.%m.%Y')

# USD today
t_3 = request('GET', f'https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={today_day}').text
with open('USD.html', 'w', encoding='utf-8') as f:
    f.write(t_3)

with open("USD.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('^\w+'))
    usd_1 = strings[strings.index("Доллар США") + 1][:-2]

# USD yesterday

t_33 = request('GET',
               f'https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={yesterday}').text
with open('USD2.html', 'w', encoding='utf-8') as f:
    f.write(t_33)

with open("USD2.html", "r", encoding='utf-8') as f:
    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('^\w+'))
    usd_2 = strings[strings.index("Доллар США") + 1][:-2]

# USD change

usd_diff = round(float(usd_1.replace(',', '.')) - float(usd_2.replace(',', '.')), 2)

# USD final

if usd_diff >= 0:
    usd = f'₽ {usd_1} (+{usd_diff}) ЦБ РФ'
else:
    usd = f'₽ {usd_1} ({usd_diff}) ЦБ РФ'

# EUR

# EUR today

t_4 = request('GET', f'https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={today_day}').text
with open('EUR.html', 'w', encoding='utf-8') as f:
    f.write(t_4)

with open("EUR.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('^\w+'))
    euro_1 = strings[strings.index("Евро") + 1][:-2]

# EUR yesterday

t_44 = request('GET',
               f'https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={yesterday}').text
with open('EUR2.html', 'w', encoding='utf-8') as f:
    f.write(t_44)

with open("EUR2.html", "r", encoding='utf-8') as f:
    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('^\w+'))
    euro_2 = strings[strings.index("Евро") + 1][:-2]

# EUR change

euro_diff = round(float(euro_1.replace(',', '.')) - float(euro_2.replace(',', '.')), 2)

# EUR final

if euro_diff >= 0:
    euro = f'₽ {euro_1} (+{euro_diff}) ЦБ РФ'
else:
    euro = f'₽ {euro_1} ({euro_diff}) ЦБ РФ'

print('Data from RBC read')

# Create a message
today_day = datetime.now().strftime('%d %B %Y')

message_for_vk_and_telegram = f'Курсы дня, {today_day}: \n\nUSD:  {usd} \n\nEUR:  {euro} \n\nBRENT:  {brent} \n\nGOLD: {gold}'


# Report to VK


def report_to_vk():
    token = '6ceed695e050149c36b705006dXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    vk_session = vk_api.VkApi(token=token)
    vk = vk_session.get_api()

    vk.messages.send(
        chat_id=1,
        random_id=random.randint(1, 2 ** 31),
        message=message_for_vk_and_telegram
    )
    print('Report to VK send')


# Report to Telegram


def report_to_telegram():
    message_telegram = message_for_vk_and_telegram
    token = '1127XXXXXX:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    chat_id = 127585XXXX  # your chat id

    message = message_telegram  # text which you want to send
    chats = [87717XXXX, 72854XXXX, 127585XXXX]
    url_get = 'https://api.telegram.org/bot1127XXXXXX:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/getUpdates'
    response = requests.get(url_get)

    # Add new users
    new_chats = []
    if len(response.json()['result']) != 0:
        for i in response.json()['result']:
            if i['message']['chat']['id'] not in new_chats:
                new_chats.append(i['message']['chat']['id'])
    try:            
        with open('CHATS', 'r') as f:
            for line in f:
                chats.append(int(line.strip()))
        with open('CHATS', 'a') as f:
            for c in new_chats:
                if c not in chats:
                    chats.append(c)
                    f.write(str(c) + '\n')
    except:
        with open('CHATS', 'w') as f:
            for c in new_chats:
                if c not in chats:
                    chats.append(c)
                    f.write(str(c) + '\n')

    for chat in chats:
        time.sleep(0.1)
        params = {'chat_id': chat, 'text': message}

        base_url = f'https://api.telegram.org/bot{token}/'
        url = base_url + 'sendMessage?' + urlencode(params)
        # Only if you need it
        # proxy = {'https': 'https://77.48.23.199:57842'}

        # To send request via proxy
        # resp = requests.get(url, proxies=proxy)
        resp = requests.get(url)
    print('Report to Telegram send')


# Our tasks


t1 = PythonOperator(task_id='report_to_vk', python_callable=report_to_vk, dag=dag)
t2 = PythonOperator(task_id='report_to_telegram', python_callable=report_to_telegram, dag=dag)

t1 >> t2
