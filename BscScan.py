import json

import pandas as pd
import requests


class BscScan:

    @staticmethod
    def get_tokens_transactions(addresses):
        """
        Calls bscscan api and gets list of transactions for each token in provided list of adresses.

        :param addresses: list of tokens adresses
        :return: Dictionary { address : transactions_dataframe }
        """
        print(f"\n\n----------------- Pobieranie transakcji z BscScan-----------------")
        transactions = {}
        for a in addresses:
            print(a)
            url = f"https://api.bscscan.com/api?module=logs&action=getLogs&fromBlock=0&toBlock=latest&address={a}&apikey=Z6S9YGXET85E39HPUB4UANGEXTZ4PUQD2J"
            resp = requests.get(url)
            data = json.loads(resp.text)['result']

            df = pd.DataFrame()
            for d in data:
                if len(d['topics']) == 3:
                    d['topics_0'] = d['topics'][0]
                    d['topics_1'] = d['topics'][1]
                    d['topics_2'] = d['topics'][2]
                elif len(d['topics']) == 2:
                    d['topics_0'] = d['topics'][0]
                    d['topics_1'] = d['topics'][1]
                elif len(d['topics']) == 1:
                    d['topics_0'] = d['topics'][0]
                del d['topics']
                df = df.append(d, ignore_index=True)
            try:
                df = df.drop(columns=['data', 'blockNumber', 'gasPrice', 'gasUsed'])
            except:
                print("Bład przy usuwaniu kolumn")
            try:
                df['timeStamp'] = df['timeStamp'].apply(int, base=16)
                df['Date'] = pd.to_datetime(df['timeStamp'], unit='s')
                df['Date'] = df['Date'] + pd.DateOffset(hours=1)
                df.drop(columns=['timeStamp'], inplace=True)
            except:
                continue

            transactions[a] = df

        return transactions

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
            param['contractaddress'] = a
            resp = requests.get('https://api.bscscan.com/api', params=param)
            data = json.loads(resp.text)
            supplies[a] = data['result']

        return supplies

    @staticmethod
    def is_contract(address):
        """
        Calls bscscan api, returns TRUE if address is a contract, FALSE otherwise.

        :param address: Address string
        :return: Bool
        """
        param = {'module': 'contract', 'action': 'getsourcecode', 'address': address, 'apikey': 'Z6S9YGXET85E39HPUB4UANGEXTZ4PUQD2J'}
        try:
            resp = requests.get('https://api.bscscan.com/api', params=param)
            abi = json.loads(resp.text)['result'][0]['ABI']
            source_code = json.loads(resp.text)['result'][0]['SourceCode']
        except:
            print("Niepoprawny adress!")
            return False

        if(source_code == "" or abi == "Contract source code not verified"):
            return False
        else:
            return True

    @staticmethod
    def is_pancakepair(address):
        """
        Calls bscscan api, returns TRUE if address is a Pancake Pair contract, FALSE otherwise.

        :param address: Address string
        :return: Bool
        """
        param = {'module': 'contract', 'action': 'getsourcecode', 'address': address, 'apikey': 'Z6S9YGXET85E39HPUB4UANGEXTZ4PUQD2J'}
        try:
            resp = requests.get('https://api.bscscan.com/api', params=param)
            pancake = json.loads(resp.text)['result'][0]['ContractName']
        except:
            print("Niepoprawny adress!")
            return False

        if "PancakePair" in pancake:
            return True
        else:
            return False
