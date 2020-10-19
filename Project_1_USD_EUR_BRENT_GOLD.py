from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from datetime import date
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
          schedule_interval='30 5,11 * * *')


# Read data from RBC

# BRENT

t = request('GET', 'https://quote.rbc.ru/ticker/181206').text
with open('BRENT.html', 'w', encoding='utf-8') as f:
    f.write(t)
a = []
with open("BRENT.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('$'))
    for i in strings:
        if i == '$' or i[0].isdigit() or '%' in i:
            a.append(i)
    brent = a[0] + ' ' + a[1].strip() + ' ' + a[2].strip()

# GOLD

t_2 = request('GET', 'https://quote.rbc.ru/ticker/101039').text
with open('GOLD.html', 'w', encoding='utf-8') as f:
    f.write(t_2)
b = []
with open("GOLD.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('$'))
    for i in strings:
        if i == '$' or i[0].isdigit() or '%' in i:
            b.append(i)
    gold = b[0] + ' ' + b[1].strip() + ' ' + b[2].strip()

# USD

t_3 = request('GET', 'https://quote.rbc.ru/ticker/72413').text
with open('USD.html', 'w', encoding='utf-8') as f:
    f.write(t_3)

with open("USD.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('₽'))

    usd = '₽ ' + strings[7].strip()[1:-1] + ' ЦБ РФ'

# EUR

t_4 = request('GET', 'https://quote.rbc.ru/ticker/72383').text
with open('EUR.html', 'w', encoding='utf-8') as f:
    f.write(t_4)

with open("EUR.html", "r", encoding='utf-8') as f:

    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    strings = soup.find_all(string=re.compile('₽'))

    euro = '₽ ' + strings[7].strip()[1:-1] + ' ЦБ РФ'

print('Data from RBC read')

# Create a message
today_day = datetime.now().strftime('%d %B %Y')

message_for_vk_and_telegram = f'Курсы дня, {today_day}: \n\nUSD:  {usd} \n\nEUR:  {euro} \n\nBRENT:  {brent} \n\nGOLD: {gold}'


# Report to VK


def report_to_vk():
    token = '6ceed695e050149c36b705006d51139574adade15213093ceda7f5b2fe4XXXXXXXXXXXXXXXXXXXXc'
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
    token = '1127113079:AAFeKXAd0ZtO6J7XXXXXXXXXXXXXXX'
    chat_id = 1275857904  # your chat id

    message = message_telegram  # text which you want to send
    chats = [877171139, 728548581, 1275857904]
    url_get = 'https://api.telegram.org/bot1127113079:AAFeKXAd0ZtO6J7XXXXXXXXXXXXXXX/getUpdates'
    response = requests.get(url_get)

    # Add new users
    if len(response.json()['result']) != 0:
        for i in response.json()['result']:
            if i['message']['chat']['id'] not in chats:
                chats.append(i['message']['chat']['id'])

    for chat in chats:
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
