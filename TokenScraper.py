import collections
import datetime
import json

import pandas as pd
import requests

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class BitTimes:

    url = "https://thebittimes.com/coins-new.html"

    @staticmethod
    def get_token_addresses():
        """
        Scrapes token addresses from https://thebittimes.com/coins-new.html

        :return: list of addresses
        """

        print("\n\n----------------- Pobieranie nowych tokenow z BitTimes-----------------")
        resp = requests.get(BitTimes.url)
        resp_list = resp.text.split('<tr data-network="BSC">')
        del resp_list[0]

        df = pd.DataFrame()
        token_addresses = {}
        for i in resp_list:
            token_address = i.split('href="')[1].split('">')[0].split('-')[-1].split('.')[0]
            token_symbol = i.split('title=')[1].split('(')[1].split(')')[0]
            token_datetime = i.split('text-align: center;" title=')[2].split('Deploy At')[1].split('data-breakpoints')[0].replace('"', '')[1:-1]
            token_chain = i.split('notranslate hide-mobile" style=')[1].split('data-breakpoints="xs">')[1].split("</td>")[0]
            if "BSC" not in token_chain:
                print("NIE BSC\n\n\n\n")
                continue

            df = df.append({'Address': token_address, 'Symbol': token_symbol, 'DateTime': token_datetime}, ignore_index=True)
        previous_newest_tokens = pd.read_csv("/Users/sebastiansukiennik/Desktop/PycharmProjects/PierwszyMilion/NewestBitTimesTokens.csv", index_col=0)
        dropped_duplicates = pd.concat([previous_newest_tokens, df]).drop_duplicates(keep=False)

        if not dropped_duplicates.empty:
            dropped_duplicates = dropped_duplicates[dropped_duplicates.iloc[-1, 2] < dropped_duplicates['DateTime']]
            print(dropped_duplicates)
        else:
            print("nie ma zadnych nowych tokenow")

        for row in dropped_duplicates.iterrows():
            token_addresses[row[1]['Address']] = row[1]['Symbol']
        df.to_csv('NewestBitTimesTokens.csv')

        return token_addresses

    @staticmethod
    def get_token_holders(addresses):
        """
        Scrapes token holders from https://thebittimes.com/token-TOKEN_NAME-BSC-0x6d5885619126e388c377729b2296003d11e5f85c.html for each token.
        Returns a dictionary {key: address, value: df of shareholders}

        :param addresses: dict of addresses and symbols
        :return: Dictionary of dataframes with share holders
        """

        print(f"\n\n----------------- Pobieranie holderow dla tokenow -----------------")
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
                print("Blad przy pobieraniu holderow")
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
        print(f"\n\n----------------- Pobieranie transakcji z BscScan-----------------")
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

        print(f"\n\n----------------- Filtrowanie pod względem holderów -----------------")
        result_holders = holders.copy()

        for a, h in holders.items():
            # sprawdzam czy jest jakikolwiek wiekszy do 30% a potem czy jest contractem/PancakeSwapem/Nullem
            if (h['percentage'] > 0.3).any():
                h_gt_30 = h.loc[h['percentage'] > 0.3, :]
                for row in h_gt_30.iterrows():
                    if not(BscScan.is_contract(row[1]['address'])) and not(BscScan.is_pancakepair(row[1]['address'])) and row[1]['address'] != "0x000000000000000000000000000000000000dead":
                        print(f"Usuwam adres {a} bo jeden z dużych holderów nie jest contractem/PancakeSwapem/Nullem")
                        del result_holders[a]
                        break
                if not(a in result_holders.keys()):
                    continue

            # sprawdzam czy jest więcej niż 10 holderów
            if h.shape[0] < 7:
                print(f"Usuwam adres {a} bo jest mniej niz 10 holderów")
                del result_holders[a]
                continue

            # sprawdzam czy jakikolwiek holder ma powyżej 90% udziałów
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

        print(f"\n\n----------------- Filtrowanie pod względem transakcji -----------------")
        result_transactions = transactions.copy()
        for a, t in transactions.items():
            print(a)
            # wylicza średnie natężęnie tradeów w ostatnich 10 minutach
            last_5_min = t.loc[t['Date'] > t.loc[t.shape[0] - 1, 'Date'] - pd.DateOffset(minutes=5), :]
            last_5_min.loc[:, 'TimeDelta'] = last_5_min.loc[:, 'Date'].shift(-1).sub(last_5_min['Date'])
            if last_5_min.loc[:, 'TimeDelta'].mean() > pd.Timedelta(minutes=2):
                print(f"Usuwam token {a}, bo srednia częstotliwość transakcji w ostatnich 10 minutach jest mniejsza niż 1 na minutę")
                del result_transactions[a]
                continue

        return result_transactions

    @staticmethod
    def sift_by_volume_launchDate(addresses: list):
        """
        Checks if the volume for last 5 minutes of transactions is sufficient, if not they're dropped.
        :param addresses: list of addresses
        :return: cleaned list of addresses
        """

        print(f"\n\n----------------- Filtrowanie pod względem volume i ICO -----------------")
        # pobieranie danych historyzcnych z bitquery
        hlocv_df = BitQuery().run_multiple_queries(addresses)
        cleaned_addresses = list(addresses)
        result_df = pd.DataFrame()

        print(hlocv_df.keys())
        for a, df in hlocv_df.items():
            if df.empty:
                cleaned_addresses.remove(a)
                print(f"Usuwam {a} bo ma pusty dataframe hlocv")
                continue
            df.loc[:, 'timeInterval_minute'] = pd.to_datetime(df['timeInterval_minute'])

            # jeżeli token utworzony wcześniej niż 15 minut temu, lub średnia wielkość transakcji na minute nie przekracza 50 dolarów to usuwam go z listy
            if df.loc[0, 'timeInterval_minute'] < (pd.Timestamp.now() - pd.DateOffset(hours=1, minutes=20)):
                print(f"Usuwam {a} bo utworzony o {df.loc[0, 'timeInterval_minute']}")
                cleaned_addresses.remove(a)
            elif df['tradeAmount'].mean() < 50:
                print(f"Usuwam {a} bo średnia wielkość transakcji: {df['tradeAmount'].mean()}")
                cleaned_addresses.remove(a)

            if a in cleaned_addresses and not df.empty:

                result_df = result_df.append(pd.DataFrame({"address": [a], 'entry_price': [df.iloc[-1, -2]], "entry_time": [str(datetime.datetime.now())]}), ignore_index=True)

        result_df.reset_index(drop=True, inplace=True)

        return result_df


class BitQuery:

    def __init__(self, baseAddress="0xD302c09BC32aEF53146B6bA7BC420F5CACa897f6", quoteAddress="0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", from_date='2019-01-01', minute_interval=1):
        """
        Initializes BitQuery object with headers: API key and query: contains query expression, which states which parameters (symbol, address, open, close, etc.)
        should be returned
        """
        self.headers = {'X-API-KEY': 'BQYgidAQz9hI3gFDCJKEeBf965EwKdlI'}
        self.query = """
    {
      ethereum(network: bsc) {
        dexTrades(
          options: { asc: "timeInterval.minute"}
          date: {since: """ + f'"{from_date}"' + """}
          exchangeName: {is: "Pancake v2"}
          baseCurrency: {is: """ + f'"{baseAddress}"' + """}
          quoteCurrency: {is: """ + f'"{quoteAddress}"' + """}
        ) {
          timeInterval {
            minute(count: """ + f"{minute_interval}" + """)
          }
          baseAmount
          trades: count
          maximum_price: quotePrice(calculate: maximum)
          minimum_price: quotePrice(calculate: minimum)
          open_price: minimum(of: block, get: quote_price)
          close_price: maximum(of: block, get: quote_price)
          tradeAmount(in: USD)
        }
      }
    }
    """

    def run_query(self):
        """
        Runs request to BitQuery for specific query (one exchange pair)
        :return: JSON with all requested info
        """
        request = requests.post('https://graphql.bitquery.io/', json={'query': self.query}, headers=self.headers)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception('Query failed and return code is {}.      {}'.format(request.status_code, self.query))

    def run_multiple_queries(self, addresses: list):
        """
        Calls run_query() method, and then saves the result to hlocv dictionary {address: df_historical_hlocv}
        :param addresses: list of addresses
        :return: Dictionary {address: df_historical_hlocv}
        """

        # Pobieranie danych historycznych dla listy adresów
        hlocv = {}
        for a in addresses:
            self.__init__(baseAddress=a)
            result = self.run_query()['data']['ethereum']['dexTrades']
            temp = []
            try:
                for i in result:
                    temp.append(Assisting().flatten(i))
            except:
                print(f"nie udało się spłaszczyć {a}")

            df = pd.DataFrame(temp)
            hlocv[a] = df
            print(a)

        return hlocv


class Assisting:

    @staticmethod
    def get_new_data():
        """
        Main function, calls other functions to collect addresses, then filters them based on holders, transactions,
        volume and launch date. Finally saves and returns a dataframe consisting of tokens: address, latest close price and timestamp.

        :return: DataFrame with columns =[address, close_price, entry_time]
        """
        # Pobiera adresy
        addrs = BitTimes.get_token_addresses()
        #testowe
        #addrs = ['0xa7d46c62cdc819e8d5265b2f62e0b753dda2278f', '0xbcb4520ef0333836f372365c050df74ec0a7e2a3', '0x0f77f0ece482c99157c364548d793e506b555ba6', '0xa48384814b28037399d017f587a9f92c147f9140', '0x05ad901cf196cbdceab3f8e602a47aadb1a2e69d', '0x746d4cc53cc1204a66dff61584a3f852fffee6c2', '0x0b4ebd6c5c513111d2d2e32a675aeba0153d41ee', '0xc641ab6964dc059588a97ec765035d194d1adb80', '0xb3e9e1a5e30a3b9a7e893a649f4ff1c59fd89329', '0x0830d1487ccbf87dcd0cd931ed58fbd274ac2a98', '0x0a0ff71eb4d1d3df79d0e873d2b40ee6136a2a59', '0xe6dcf146417483a7ced834e92eb6a8eeacbc252d', '0xa39101619103a30c989f6c764ff7b6069957ee77', '0x7af2fbdf3bb23479507253cff84e4f6923cca3e8', '0x203de499f76f589f80b371a54a98e438339c5563', '0x6098c24090a45f2ca7212a0c3c3d4e3167797afc', '0x5dfa507e007c5a4189cca7fe71620690c29b522f', '0xf1f6e6e48671f74db90d3f1b5c64baf77a68e3bd', '0xd77bd7d2ec26ac2e6ef5c497f500d05f20cc5d87', '0xb9eb5205edf8fdd959e35dea5a71c2655540ab73', '0xbddf4815e9c644c5163d590fe5987c1168e27a3b', '0xc4da8a2d3629412fb78324b482517b9ce92551cd', '0xac59483841b53bffeed1d227736210a02c6d172e', '0xf07cc8c9a1da844229d6893c34a9db7398c7ecf5', '0xc9f65c2be6dc5cd85c756d01522a021655c19dbe', '0xf0a2aebd9296c7e21583fc2092663fdef26c59f7', '0x251bda38dda410103f70175a64a287ca48aef041', '0x07d23bb3d60e30e31ea389782202ea12e8450abe', '0xddc0dbd7dc799ae53a98a60b54999cb6ebb3abf0', '0xad6fedbe366ff6effe05d6eca0cebf658f1a9585', '0xf42ef4f1a773c87e401d50f27d6f52e8b1f92518', '0xa71371edba5f08c6f7763c3a17b1de234a3e8b52', '0x6c3d2575cf035d879accca2630a6a1e8ca94e37f', '0xb3a566f889d8f386eafbc345434c0a83359d6920', '0x7a0db8d52e6cafeeeb2d193980d1e51213c633f4', '0x7f7d382ee68c6662262959eb6d4ccb92ecec1750', '0x40c29b371965656aae9668856bc26fd762d14c26', '0xe0268fbbeacbdebf36d6e755bbbd600c65a04057', '0x60e95e30741663a834d71e41bdb350f32a0233d5', '0xa3513f68c0365ad0112995059dfebaf22d99ed1f', '0x141069b85b2204631686c9cff0eb0dcc2368f4d2', '0x5c00ca49d19714b9892762b16e42f468bddf26fb', '0xc4f4cc89210eed0d3ca6239a7221c628ee08a9e5', '0xbc1b4a60ea8f6169d07fa66e74d4ff5ccb87c260', '0x7ba1cb6c8a0445c6b3eb2ef2346b4007eb587db5', '0xde018990bfac1a9bbcdc349be6c14b4ed25fa8ea', '0xb47199e771c13eccb0b65c1f2a7f50e1968e9753', '0x30152961bbb0158986e5f16dee7b2cdc221bd6b3']

        # sprawdza holderów, zwraca listę adresów z dobrymi holderami
        '''
        hold = BitTimes.get_token_holders(addrs)
        hold_filtered = Filter.sift_by_holders(hold)
        '''

        # filtruję adresy pod względem płynności z listy transakcji
        tran = BscScan.get_tokens_transactions(addrs)
        tran_filtered = Filter.sift_by_transactions(tran)

        # filtruje adresy pod względem volume i daty ICO
        vol_lnch_filterd_addresses = Filter.sift_by_volume_launchDate(list(tran_filtered.keys()))
        vol_lnch_filterd_addresses.to_csv("TokensToBuy.csv")
        print(f"\n\n----------------- DataFrame tokenów do kupienia -----------------")
        print(vol_lnch_filterd_addresses)

        return vol_lnch_filterd_addresses

    def flatten(self, d, parent_key='', sep='_'):
        """
        Converts nested dictionaries into flattened ones
        :param d: dictionary to flatten
        :param parent_key:
        :param sep:
        :return: flattened (1-D) dictionary
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.abc.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


