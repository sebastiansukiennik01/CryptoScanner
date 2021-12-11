"""
    Program na szybko do zczytania danych z coinmarketcap.com
Rozwiązanie tymaczosowe, tylko do celów testowych, wstępnej analizy danych, testowania strategii i moodeli
"""
import requests
import json
import glob
import pandas as pd
import collections
import time
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


class BitQuery:

    def __init__(self, baseAddress="0xD302c09BC32aEF53146B6bA7BC420F5CACa897f6", quoteAddress="0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", from_date="2021-11-01", minute_interval=5):
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

    def save_query(self, result, name):
        """
        Saves JSON from run_query to text files (collecting data, to save on credits from BitQuery)
        :param result: JSON
        :param name: file name
        :return:
        """
        with open(f'Data/Text/{name}.txt', 'w') as f:
            json.dump(result, f)
        f.close()

    def run_multiple_queries(self, tokens_df):
        """
        Calls run_query() method, and then saves the result tp text files via save_query() method. At the end adds collected tokens to file where all collected symbols, and addreses are stored.
        :param tokens_df:
        :return:
        """
        symb_add = [[], []]
        for index, row in tokens_df.iterrows():
            if row["Blockchain"] == "Binance Coin":
                print(index, row)
                self.baseAddress = row['Address']
                result = self.run_query()
                self.save_query(result, row['Symbol'])
                print(f'{row["Symbol"]} with adress: {row["Address"]} has been successfully saved into text file.')
                symb_add[0].append(row['Symbol'])
                symb_add[1].append(row['Address'])

        self.add_to_tokens_list(symb_add)

    def add_to_tokens_list(self, symb_add):
        """
        Saves tokens symbols and adresses in order to keep track about already collected data.
        :param symb_add: 2 dim list of tokens symbols and addresses
        :return: None
        """
        try:
            old_tokens = pd.read_csv("Data/Tokens.csv", index_col=0)
            new_tokens = pd.DataFrame({"Symbol": symb_add[0], "Address": symb_add[1]})
            df = pd.concat([old_tokens, new_tokens]).drop_duplicates(subset=['Symbol', 'Address'])
            df = df.reset_index(drop=True)
            df.to_csv("Data/Tokens.csv")
        except:
            new_tokens = pd.DataFrame({"Symbol": symb_add[0], "Address": symb_add[1]})
            new_tokens.to_csv("Data/Tokens.csv")

class Processing:

    def read_files(self):
        """
        Reads all files from Data/Text/*.txt and returns a dictionary with exchange pair names as keys, and JSON dictionaries as values
        :return: Dictionary
        """

        file_paths = glob.glob(f"Data/Text/*.txt")
        print(file_paths)
        coins_dict = {}
        for path in file_paths:
            f_name = path.rsplit('/')[2][:-4]
            print(f_name)
            with open(f'Data/Text/{f_name}.txt') as json_file:
                data = json.load(json_file)
                coins_dict[f_name] = data

        return coins_dict

    def txt_into_pd(self, coins_dict, save=False):
        """
        Converts dictionary of JSON data into dictionary containing pandas DataFrames
        Saves dataframes to CSV if requested
        :param coins_dict: Dictionary of coin paris from read_file
        :return: Pandas DataFrame
        """
        print(coins_dict.keys())
        names = list(coins_dict.keys())
        coins_df_dict = {}


        for name in names:
            coin_data = coins_dict[name]['data']['ethereum']['dexTrades']
            temp = []
            for i in coin_data:
                temp.append(self.flatten(i))

            df = pd.DataFrame(temp)
            coins_df_dict[name] = df
            if save:
                df.to_csv(f"Data/CSV/{name}.csv")

        return coins_df_dict

    def flatten(self, d, parent_key='', sep='_'):
        """
        Converts nested dictionaries into flattened ones
        :param d:
        :param parent_key:
        :param sep:
        :return:
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.abc.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


class TokenInfo:

    def __init__(self):
        pass

    def cmc_new_names(self):
        """
        Calls CoinMarketCap and extracts info about last 30 added tokens.
        :return: 2 dim list of tuples (name, symbol)
        """
        request = requests.get("https://coinmarketcap.com/new/")
        all_names = []
        all_symbols = []
        all_prices = []
        all_blockchain = []
        all_timeago = []
        all_hrefs = []

        if request.status_code == 200:
            result = request.text
            names = result.rsplit('"sc-1eb5slv-0 iworPT">')
            symbols = result.rsplit('"sc-1eb5slv-0 gGIpIK coin-item-symbol"')
            timeago = result.rsplit(' ago')
            hrefs = result.rsplit('" class="cmc-link"><div class="sc-16r8icm-0 sc-1teo54s-0 dBKWCw')


            for s in symbols:
                all_symbols.append(s.rsplit(">")[1].rsplit("<")[0])
                all_prices.append(s.rsplit("<")[8].rsplit(">$"))
            for n in names:
                all_names.append(n.rsplit("<")[0])
            for t in timeago:
                all_timeago.append(t.rsplit(">")[-1])
                all_blockchain.append(t.rsplit(">")[-4].rsplit("<")[0])

            for h in hrefs:
                all_hrefs.append(h.rsplit('href="')[-1])

            print(len(all_prices))
            for a in all_prices:
                if len(a) == 2:
                    del a[0]

            del all_blockchain[-1]
            del all_names[0]
            del all_symbols[0]
            del all_timeago[-1]
            del all_prices[-1]
            del all_hrefs[-1]

            names_symbols = pd.DataFrame({"Name": all_names, "Symbol": all_symbols, "Time Ago": all_timeago, "Blockchain": all_blockchain, "Price": all_prices, "CMCHref": all_hrefs})
            return names_symbols

    def get_token_address(self, df_cmc_new: pd.DataFrame):
        """
        Calls CoinMarketCap with request for each tokens site, then gets address, updates and returns new DataFrame
        :param df_cmc_new: Pandas DataFrame containing info about token, most important is column "Href" with CoinMarketCap hrefs to each token
        :return: Pandas DataFrame
        """

        all_adresses = []
        for href in df_cmc_new["CMCHref"].values:
            cmc_token_req = requests.get("https://coinmarketcap.com" + href)
            if cmc_token_req.status_code == 200:
                result = cmc_token_req.text
                temp = result.rsplit("sc-10up5z1-5 jlEjUY", 1)
                address = temp[1].split('href="')[1].split('" class="cmc-link"')[0].split("/")[-1]
                all_adresses.append(address)
        df_cmc_new["Address"] = all_adresses
        return df_cmc_new


if __name__ == '__main__':

    t1 = time.time()

    ns = TokenInfo().cmc_new_names()
    ns2 = TokenInfo().get_token_address(ns)
    print(ns2)
    bq = BitQuery().run_multiple_queries(ns2)      #Downloading new data from CMC
    token = Processing().read_files()
    df = Processing().txt_into_pd(token, save=True)

    t2 = time.time()
    print("\n\n\nTime: ", t2-t1)





