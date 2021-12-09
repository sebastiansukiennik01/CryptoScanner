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

    def __init__(self, baseAddress, quoteAddress, from_date="2021-11-01", minute_interval=5):
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


class Processing:

    def read_files(self, file_name):
        """
        Reads all files from Data/Text/*.txt and returns a dictionary with exchange pair names as keys, and JSON dictionaries as values
        :return: Dictionary
        """
        file_paths = glob.glob(f"Data/Text/{file_name}.txt")
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
        :return:
        """
        name = list(coins_dict.keys())[0]
        coins_dict = coins_dict[name]['data']['ethereum']['dexTrades']
        coins_df_dict = {}
        temp = []

        for i in coins_dict:
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
                if len(a) != 2:
                    pass

            del all_blockchain[-1]
            del all_names[0]
            del all_symbols[0]
            del all_timeago[-1]
            del all_prices[-1]
            del all_hrefs[-1]

            print(all_prices)
            names_symbols = pd.DataFrame({"Names": all_names, "Symbols": all_symbols, "Time Ago": all_timeago, "Blockchain": all_blockchain, "Prices": all_prices, "Hrefs": all_hrefs})
            return names_symbols


if __name__ == '__main__':

    ns = TokenInfo().cmc_new_names()
    print(ns)


    '''t1 = time.time()
    token_adresses = [["VML", "0xD302c09BC32aEF53146B6bA7BC420F5CACa897f6"], ["FCP", "0x155e8A74dAC3d8560DDaBBc26aa064B764535193"]]
    for ta in token_adresses:
        bq = BitQuery(ta[1], "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", "2021-11-01", 1)
        result = bq.run_query()
        bq.save_query(result, ta[0])
        token = Processing().read_files(ta[0])
        df = Processing().txt_into_pd(token, save=True)
        print(df[ta[0]].tail())

    t2 = time.time()
    print(t2-t1)'''




