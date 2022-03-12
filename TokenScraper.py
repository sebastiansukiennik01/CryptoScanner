import collections
import datetime
import json
import smtplib
from pathlib import Path

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
        Scrapes new token addresses from https://thebittimes.com/coins-new.html
        Adds new addresses to DataV2/HistoricTokens.csv where all ever colelcted addresses are stored.
        Saves current new addresses to NewestBitimeTokens.csv (50 tokens listed on BitTimes right now)
        Returns lsit of addreses that were added on BitTimes since last iteration of this functions.

        :return: dict of new addresses {address: tokenSymbol}
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

        newest_token_path = Path("DataV2/") / "NewestBitTimesTokens.csv"
        previous_newest_tokens = pd.read_csv(newest_token_path, index_col=0)

        historic_token_path = Path("DataV2/") / "NewestBitTimesTokens.csv"
        historic_tokens = pd.read_csv(historic_token_path, index_col=0)
        dropped_duplicates = pd.concat([previous_newest_tokens, df]).drop_duplicates(keep=False)
        historic_tokens = historic_tokens.append(dropped_duplicates, ignore_index=True)


        historic_tokens.to_csv(historic_token_path)


        if not dropped_duplicates.empty:
            dropped_duplicates = dropped_duplicates[dropped_duplicates.iloc[-1, 2] < dropped_duplicates['DateTime']]
            print(dropped_duplicates)
        else:
            print("nie ma zadnych nowych tokenow")

        for row in dropped_duplicates.iterrows():
            token_addresses[row[1]['Address']] = row[1]['Symbol']

        df.to_csv(newest_token_path)

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
                print(f"Usuwam adres {a} bo jest mniej niz 7 holderów")
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
    def sift_by_hlocv(addresses: list):
        """
        Checks if the volume for last 5 minutes of transactions is sufficient, if not they're dropped.
        :param addresses: list of addresses
        :return: cleaned list of addresses
        """

        print(f"\n\n----------------- Filtrowanie pod względem HLOCV -----------------")
        # pobieranie danych historyzcnych z bitquery
        hlocv_df = BitQuery().run_multiple_queries(addresses)
        cleaned_addresses = list(addresses)
        result_df = pd.DataFrame()

        print(hlocv_df.keys())
        for a, df in hlocv_df.items():

            #usuwa jeżeli brakuje danych HLOCV, czyli prawdopodobnie brak transackji
            if df.empty:
                cleaned_addresses.remove(a)
                print(f"Usuwam {a} bo ma pusty dataframe hlocv")
                continue
            df.loc[:, 'timeInterval_minute'] = pd.to_datetime(df.loc[:, 'timeInterval_minute'])
            df['timeInterval_minute_diff'] = df.loc[:, 'timeInterval_minute'].diff(1)
            df['close_price'] = pd.to_numeric(df['close_price'])
            df['close_price_pctchange'] = df.loc[:, 'close_price'].pct_change()

            # jeżeli token utworzony wcześniej niż 20 minut temu to usuwam go z listy
            if df.loc[0, 'timeInterval_minute'] < (pd.Timestamp.now() - pd.DateOffset(hours=4, minutes=20)):
                print(f"Usuwam {a} bo utworzony o {df.loc[0, 'timeInterval_minute']}")
                cleaned_addresses.remove(a)

            # jeżeli średnia wielkość transakcji w ostatnich 3 minutach nie przekracza 50 dolarów to usuwam
            elif df.iloc[-5:, 7].mean() < 50:
                print(f"Usuwam {a} bo średnia wielkość transakcji: {df.loc[:, 'tradeAmount'].mean()}")
                cleaned_addresses.remove(a)

            # jeżeli w ostatnich 3 minutach nie było regularncyh transakcji
            elif df.iloc[-3:, 8].mean() > pd.Timedelta(minutes=1, seconds=20):
                print(f"Usuwam {a} bo średni czas między ostatnimi transakcjami: {df.iloc[-3:, 8].mean()}")
                cleaned_addresses.remove(a)

            # jeżeli 3 transakcji od końca była wcześniej niż 6 minut temu to odrzucam
            elif df.iloc[-3, 0] < pd.Timestamp.now() - pd.DateOffset(minutes=6):
                print(f"Usuwam {a} bo 3 transakcja od końca była o godz: {df.iloc[-3, 0]} (dawno)")
                cleaned_addresses.remove(a)

            # jeżeli w ostatnich minutach średni wzrost mniej niż -5% lub suma pct change mniej niż 0 to odrzucam
            elif df.iloc[-3:, 9].mean() < -0.05 or df.iloc[-3:, 9].sum() < 0:
                print(f"Usuwam {a} bo 3 ostatnie minuty wskazują downtrend avg_pct_change: {df.iloc[-3:, 9].mean()}, min: {df.iloc[-3:, 9].min()}")
                cleaned_addresses.remove(a)

            if a in cleaned_addresses and not df.empty:
                result_df = result_df.append(pd.DataFrame({"address": [a], 'entry_price': [df.iloc[-1, -2]], "entry_time": [str(datetime.datetime.now())]}), ignore_index=True)

        result_df.reset_index(drop=True, inplace=True)

        return result_df


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
        #do testowania addrs = {'0xdd88c1da6fbc2f5970eec6985f6a7c360f146501': 'Heptic', '0x979925b928ba2161542ea4be1d1c28467b63a335': 'Football', '0xcb4cf660f961aa52200554fa4772e716f771bf5b': 'SSW'}

        # sprawdza holderów, zwraca listę adresów z dobrymi holderami
        '''
        hold = BitTimes.get_token_holders(addrs)
        hold_filtered = Filter.sift_by_holders(hold)
        '''

        # filtruję adresy pod względem płynności z listy transakcji
        tran = BscScan.get_tokens_transactions(addrs)
        tran_filtered = Filter.sift_by_transactions(tran)

        # filtruje adresy pod względem volume i daty ICO
        vol_lnch_filterd = Filter.sift_by_hlocv(list(tran_filtered.keys()))
        tokens_tobuy_path = Path("DataV2/") / "TokensToBuy.csv"
        vol_lnch_filterd.to_csv(tokens_tobuy_path)
        Assisting.addBoughtToHistoric(vol_lnch_filterd)

        print(f"\n\n----------------- DataFrame tokenów do kupienia -----------------")
        print(vol_lnch_filterd)

        return vol_lnch_filterd

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

    @staticmethod
    def send_email(tokens, email, password, to_address):
        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(email, password)
        server.sendmail(from_addr="email", to_addrs=to_address, msg=str(tokens))
        server.quit()

    @staticmethod
    def addBoughtToHistoric(tokensToBuy):
        if not tokensToBuy.empty:

            historic_token_path = Path("DataV2/") / "NewestBitTimesTokens.csv"
            historicTokens = pd.read_csv(historic_token_path, index_col=0)
            historicTokens.loc[historicTokens['Address'].isin(tokensToBuy['address'].values), 'Bought'] = True
            historicTokens.to_csv(historic_token_path)

