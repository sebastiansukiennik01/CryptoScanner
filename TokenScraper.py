import json
import time

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
            try:
                for i in range(len(holders_list)):
                    holders_list[i] = holders_list[i].replace('[', '')
                    holders_list[i] = holders_list[i].replace(']', '')
                    holders_list[i] = holders_list[i].replace('"', '')
                    holders_list[i] = holders_list[i].split(',')
                    df = df.append({'address': holders_list[i][0], 'shares': float(holders_list[i][1])}, ignore_index=True)
            except:
                print("Błąd przy pobieraniu holderów")
                continue
            df['percentage'] = df['shares']/df['shares'].sum()
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
            df = df.drop(columns=['data', 'blockNumber', 'gasPrice', 'gasUsed'])
            df['timeStamp'] = df['timeStamp'].apply(int, base=16)
            df['Date'] = pd.to_datetime(df['timeStamp'], unit='s')
            df['Date'] = df['Date'] + pd.DateOffset(hours=1)
            df.drop(columns=['timeStamp'], inplace=True)

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


class Filter:

    @staticmethod
    def sift_by_holders(holders: dict):
        """
        Filters addresses by holders. If a token has :
         - a holder that owns more then 30% of shares and is not a PancakeSwap/Null/Contract address
         - less then 10 holders
         - a holder that owns more then 90% of shares
         then this token is dropped from the dictionary.

        :param holders: Dictionary {address : holders list}
        :return: Dictionary {address : holders list}
        """

        print("################\n Filtrowanie pod względem holderów")
        result_holders = holders.copy()

        for a, h in holders.items():
            print(a)

            #sprawdzam czy jest jakikolwiek wiekszy do 30% a potem czy jest contractem/PancakeSwapem/Nullem
            if (h['percentage'] > 0.3).any():
                h_gt_30 = h.loc[h['percentage'] > 0.3, :]
                for row in h_gt_30.iterrows():
                    if not(BscScan.is_contract(row[1]['address'])) and not(BscScan.is_pancakepair(row[1]['address'])) and row[1]['address'] != "0x000000000000000000000000000000000000dead":
                        print(f"Usuwam adres {a} bo jeden z dużych holderów nie jest contractem/PancakeSwapem/Nullem")
                        del result_holders[a]
                        break
                if not(a in result_holders.keys()):
                    continue

            #sprawdzam czy jest więcej niż 10 holderów
            if h.shape[0] < 10:
                print(f"Usuwam adres {a} bo jest mniej niz 10 holderów")
                del result_holders[a]
                continue

            #sprawdzam czy jakikolwiek holder ma powyżej 90% udziałów
            if (h['percentage'] > 0.9).any():
                print(f"Usuwam adres {a} bo jeden adres ma więcej niż 90% wszystkich tokenów")
                del result_holders[a]
                continue

        return result_holders

    @staticmethod
    def sift_by_transactions(transactions: dict):
        """
        Filters addresses by transaction. If a token has on average less then one transaction per minute (for last 10 minutes)
        then it's dropped from the dictionary.

        :param transactions: Dictionary {address : transactions}
        :return: Dictionary {address : transactions}
        """
        print("################\nFiltrowanie pod względem transakcji")
        result_transactions = transactions.copy()
        for a, t in transactions.items():

            print(a)
            #wylicza średnie natężęnie tradeów w ostatnich 10 minutach
            last_10_min = t.loc[t['Date'] > t.loc[t.shape[0]-1, 'Date']-pd.DateOffset(minutes=10), :]
            last_10_min.loc[:, 'TimeDelta'] = last_10_min['Date'].shift(-1).sub(last_10_min['Date'])
            if last_10_min['TimeDelta'].mean() > pd.Timedelta(minutes=2):
                print(f"Usuwam token {a}, bo srednia częstotliwość transakcji w ostatnich 10 minutach jest mniejsza niż 1 na minutę")
                del result_transactions[a]
                continue

        return result_transactions

class Main:

    @staticmethod
    def run():
        #Pobiera adresy, sprawdza dla nich holderów, zwraca listę adresów z dobrymi holderami
        addrs = BitTimes.get_token_addresses()
        hold = BitTimes.get_token_holders(addrs)
        hold_filtered = Filter.sift_by_holders(hold)

        #Pobieram listę transakcji dla potencjalnie dobrych holderów i sprawdzam je pod względem płynności
        print(hold_filtered.keys())
        tran = BscScan.get_tokens_transactions(hold_filtered.keys())
        tran_filtered = Filter.sift_by_transactions(tran)
        print(tran_filtered.keys())

        pd.Series(list(tran_filtered.keys())).to_csv("TokensToBuy.csv")



if __name__ == '__main__':
    Main.run()


















