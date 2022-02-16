import json

import pandas as pd
import requests

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class BitTimes:
    url = "https://thebittimes.com/list-coins-BSC.html"

    @staticmethod
    def get_token_addresses():
        """
        Scrapes token addresses from https://thebittimes.com/list-coins-BSC.html

        :return: list of addresses
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
    def get_tokens_transactions(addresses):
        """
        Calls bscscan api and gets list of transactions for each token in provided list of adresses.

        :param addresses: list of tokens adresses
        :return: Dictionary { address : transactions_dataframe }
        """
        transactions = {}
        for a in addresses:
            print(a)
            url = f"https://api.bscscan.com/api?module=logs&action=getLogs&fromBlock=0&toBlock=latest&address={a}&apikey=Z6S9YGXET85E39HPUB4UANGEXTZ4PUQD2J"
            resp = requests.get(url)
            data = json.loads(resp.text)['result']

            df = pd.DataFrame()
            for d in data:
                if(len(d['topics']) == 3):
                    d['topics_0'] = d['topics'][0]
                    d['topics_1'] = d['topics'][1]
                    d['topics_2'] = d['topics'][2]
                elif(len(d['topics']) == 2):
                    d['topics_0'] = d['topics'][0]
                    d['topics_1'] = d['topics'][1]
                elif(len(d['topics']) == 1):
                    d['topics_0'] = d['topics'][0]
                del d['topics']
                df = df.append(d, ignore_index=True)
            transactions[a] = df

        return transactions

    @staticmethod
    def get_tokens_holders(addresses):
        """
        Gets holders
        :param addresses:
        :return:
        """
        pass

    @staticmethod
    def get_tokens_supply(addresses):
        """
        Calls bscscan api and gets total supply for each token in provided list of adresses.

        :param addresses: list of tokens adresses
        :return: Dictionary { address : transactions_dataframe }
        """
        param = {'module': 'stats', 'action': 'tokensupply', 'contractaddress': '', 'apikey': 'Z6S9YGXET85E39HPUB4UANGEXTZ4PUQD2J'}
        supplies = {}
        for a in addresses:
            print(a)
            param['contractaddress'] = a
            resp = requests.get('https://api.bscscan.com/api', params=param)
            data = json.loads(resp.text)
            supplies[a] = data['result']

        return supplies


addr = BitTimes.get_token_addresses()
tran = BscScan.get_tokens_transactions(addr)
supp = BscScan.get_tokens_supply(addr)


print(supp)



