import json

import requests

import undetected_chromedriver as uc
from selenium import webdriver
import time

class BitTimes:
    url = "https://thebittimes.com/list-coins-BSC.html"

    @staticmethod
    def get_token_addresses():
        """
        Scrapes token addresses from https://thebittimes.com/list-coins-BSC.html
        :return: list
        """
        resp = requests.get(BitTimes.url)
        resp_list = resp.text.split('<tr data-network="BSC">')
        del resp_list[0]

        token_addresses = []
        for i in resp_list:
            token_addresses.append(i.split('href="')[1].split('">')[0].split('-')[-1].split('.')[0])

        return token_addresses

class BscScan:

    @staticmethod
    def get_token_holders(addresses):
        for a in addresses:
            bsc_token_url = f'https://bscscan.com/token/{a}'
            resp = requests.get(bsc_token_url, headers=header)

    def get_tokens_transactions(self, addresses):
        url = "https://api.bscscan.com/api?module=logs&action=getLogs&fromBlock=0&toBlock=latest&address=0x50c92a856eca4935f2c28601fa4036d1e7fd1690&apikey=Z6S9YGXET85E39HPUB4UANGEXTZ4PUQD2J"
        resp = requests.get(url)
        data = json.loads(resp.text)['result']
        for d in data:
            print(d)

addresses = BitTimes.get_token_addresses()
header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}

BscScan().get_tokens_transactions(addresses)

