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

        token_addresses = {}
        for i in resp_list:
            token_address = i.split('href="')[1].split('">')[0].split('-')[-1].split('.')[0]
            token_symbol = i.split('title=')[1].split('(')[1].split(')')[0]
            token_addresses[token_address] = token_symbol

        return token_addresses

    @staticmethod
    def get_token_holders(addresses):
        """
        Scrapes token holders from https://thebittimes.com/token-TOKEN_NAME-BSC-0x6d5885619126e388c377729b2296003d11e5f85c.html for each token.
        Returns a dictionary {key: address, value: df of shareholders}

        :param addresses: dict of addresses and symbols
        :return: Dictionary of dataframes with share holders
        """
        holders = {}
        for addr, symb in addresses.items():
            resp = requests.get(f"https://thebittimes.com/token-{symb}-BSC-{addr}.html")
            print(f"https://thebittimes.com/token-{symb}-BSC-{addr}.html")
            holders_list = resp.text.split('window.holders')[1].split('</script>')[0][3:].split(';')[0].split(',[')

            df = pd.DataFrame(columns=['address', 'shares'])
            for i in range(len(holders_list)):
                holders_list[i] = holders_list[i].replace('[', '')
                holders_list[i] = holders_list[i].replace(']', '')
                holders_list[i] = holders_list[i].replace('"', '')
                holders_list[i] = holders_list[i].split(',')
                df = df.append({'address': holders_list[i][0], 'shares': holders_list[i][1]}, ignore_index=True)

            holders[addr] = df

        return holders


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



addrs = BitTimes.get_token_addresses()
#tran = BscScan.get_tokens_transactions(addr.keys())
#supp = BscScan.get_tokens_supply(addr.keys())
#hold = BitTimes.get_token_holders(addr)
#BscScan.is_contract(addrs[0])






