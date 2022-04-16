import json
import smtplib
import pandas as pd

import Email
from BitTimes import *
from BscScan import BscScan
from Filter import *
from Email import Email

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


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

        """#dla testów only
        data = pd.read_csv("DataV2/NewestBitTimesTokens.csv")
        addrs = {}
        for row in data.iterrows():
            addrs[row[1]['Address']] = row[1]['Symbol']"""


        # filtruję adresy pod względem płynności z listy transakcji
        tran = BscScan.BscScan.get_tokens_transactions(addrs)
        tran_filtered = Filter.sift_by_transactions(tran)

        # filtruje adresy pod względem volume i daty ICO
        vol_lnch_filterd = Filter.sift_by_hlocv(list(tran_filtered.keys()))
        tokens_tobuy_path = Path("DataV2/") / "TokensToBuy.csv"  # create file path
        vol_lnch_filterd.to_csv(tokens_tobuy_path)   # save tokens to buy to csv
        Assisting.addBoughtToHistoric(vol_lnch_filterd)

        print(f"\n\n----------------- DataFrame tokenów do kupienia -----------------")
        print(vol_lnch_filterd)

        return vol_lnch_filterd

    @staticmethod
    def addBoughtToHistoric(tokensToBuy):
        if not tokensToBuy.empty:

            historic_token_path = Path("DataV2/") / "HistoricTokens.csv"
            historicTokens = pd.read_csv(historic_token_path, index_col=0)
            historicTokens.loc[historicTokens['Address'].isin(tokensToBuy['address'].values), 'Bought'] = True
            historicTokens.to_csv(historic_token_path)


if __name__ == '__main__':
    tokens_tobuy = Assisting.get_new_data()
    print(tokens_tobuy)
    print(tokens_tobuy.info())
    to = ['sebsuk2137@gmail.com']

    if not tokens_tobuy.empty:
        values = tokens_tobuy['Address']
        print(values)
        wynik = ""
        for v in values:
            wynik += str(v) + "\n"

        Email.send_email(wynik.encode(), to_address=['sebsuk2137@gmail.com'])
    else:
        Email.send_email("brak tokenów do kupienia".encode(), to_address=['sebsuk2137@gmail.com'])
        print("Nie znaleziono nowych tokenow spelniajacych warunki")



