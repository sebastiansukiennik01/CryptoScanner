import collections

import pandas as pd
import requests


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
            print(request.status_code)
            return None

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
                    temp.append(self.flatten(i))
            except:
                print(f"nie udało się spłaszczyć {a}")

            df = pd.DataFrame(temp)
            hlocv[a] = df

        return hlocv

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
