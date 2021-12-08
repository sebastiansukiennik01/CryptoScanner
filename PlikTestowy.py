"""
    Program na szybko do zczytania danych z coinmarketcap.com
Rozwiązanie tymaczosowe, tylko do celów testowych, wstępnej analizy danych, testowania strategii i moodeli
"""
import requests
import json
import glob
import pandas as pd
import collections
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


class BitQuery:

    def __init__(self):
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
      date: {since: "2021-11-01"}
      exchangeName: {is: "Pancake v2"}
      baseCurrency: {is: "0xD302c09BC32aEF53146B6bA7BC420F5CACa897f6"}
      quoteCurrency: {is: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"}
    ) {
      timeInterval {
        minute(count: 5)
      }
      baseCurrency {
        symbol
        address
      }
      baseAmount
      quoteCurrency {
        symbol
        address
      }
      quoteAmount
      trades: count
      quotePrice
      maximum_price: quotePrice(calculate: maximum)
      minimum_price: quotePrice(calculate: minimum)
      median_price: quotePrice(calculate: median)
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

    def read_files(self):
        """
        Reads all files from Data/Text/*.txt and returns a dictionary with exchange pair names as keys, and JSON dictionaries as values
        :return: Dictionary
        """
        file_paths = glob.glob("Data/Text/*.txt")
        coins_dict = {}
        for path in file_paths:
            f_name = path.rsplit('/')[2][:-4]
            print(f_name)
            with open(f'Data/Text/{f_name}.txt') as json_file:
                data = json.load(json_file)
                coins_dict[f_name] = data

        return coins_dict

    def txt_into_pd(self, coins_dict, name):
        """

        :param coins_dict: Dictionary of coin paris from read_file
        :return:
        """
        coins_df_dict = {}
        temp = []

        for i in coins_dict:
            temp.append(self.flatten(i))

        for coin in coins_dict:
            df = pd.DataFrame(temp)
            coins_df_dict[name] = df
        return coins_df_dict

    def flatten(self, d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.abc.MutableMapping):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)


if __name__ == '__main__':


    bq = BitQuery()
    tokens = Processing().read_files()
    pand = Processing().txt_into_pd(tokens['VML_WBNB']['data']['ethereum']['dexTrades'], 'VML_WBNB')
    print(pand['VML_WBNB'].head())






