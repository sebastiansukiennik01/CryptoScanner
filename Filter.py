import datetime

import pandas as pd

import BitQuery
import BscScan


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
            # wylicza średnie natężęnie tradeów w ostatnich 5 minutach
            last_5_min = t.loc[t.loc[:, 'Date'] > (t.loc[t.shape[0] - 1, 'Date'] - pd.DateOffset(minutes=3)), :]
            last_5_min.loc[:, 'TimeDelta'] = last_5_min.loc[:, 'Date'].shift(-1).sub(last_5_min.loc[:, 'Date'])
            if last_5_min.loc[:, 'TimeDelta'].mean() > pd.Timedelta(minutes=2):
                print(f"Usuwam token {a}, bo srednia częstotliwość transakcji w ostatnich 5 minutach jest mniejsza niż 0.5 na minutę")
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
        hlocv_df = BitQuery.BitQuery().run_multiple_queries(addresses)
        cleaned_addresses = list(addresses)
        result_df = pd.DataFrame()
        tokens_to_recheck = []

        for a, df in hlocv_df.items():
            print(a)

            #usuwa jeżeli brakuje danych HLOCV, czyli prawdopodobnie brak transackji
            if df.empty or df.shape[0] < 3:
                cleaned_addresses.remove(a)
                tokens_to_recheck.append(a)  # dodaje żeby sprawdzać w pyszłości
                print(f"Usuwam {a} bo ma pusty dataframe hlocv (za mało danych)")
                continue
            df.loc[:, 'timeInterval_minute'] = pd.to_datetime(df.loc[:, 'timeInterval_minute'])
            df['timeInterval_minute_diff'] = df.loc[:, 'timeInterval_minute'].diff(1)
            df['close_price'] = pd.to_numeric(df['close_price'])
            df['close_price_pctchange'] = df.loc[:, 'close_price'].pct_change()

            # jeżeli token utworzony wcześniej niż 20 minut temu to usuwam go z listy
            if df.loc[0, 'timeInterval_minute'] < (pd.Timestamp.now() - pd.DateOffset(hours=4, minutes=20)):
                print(f"Usuwam {a} bo utworzony o {df.loc[0, 'timeInterval_minute']}")
                cleaned_addresses.remove(a)
                continue

            # jeżeli średnia wielkość transakcji w ostatnich 3 minutach nie przekracza 25 dolarów to usuwam
            if df.iloc[-3:, 7].mean() < 25:
                print(f"Usuwam {a} bo średnia wielkość transakcji: {df.iloc[-5:, 7].mean()}")
                cleaned_addresses.remove(a)
                continue

            # jeżeli w ostatnich 3 minutach nie było regularncyh transakcji
            if df.iloc[-3:, 8].mean() > pd.Timedelta(minutes=3):
                print(f"Usuwam {a} bo średni czas między ostatnimi transakcjami: {df.iloc[-3:, 8].mean()}")
                cleaned_addresses.remove(a)
                tokens_to_recheck.append(a)  # dodaje żeby sprawdzać w pyszłości
                continue

            # jeżeli 3 transakcji od końca była wcześniej niż 6 minut temu to odrzucam
            if df.iloc[-3, 0] < pd.Timestamp.now() - pd.DateOffset(hours=2, minutes=6):
                print(f"Usuwam {a} bo 3 transakcja od końca była o godz: {df.iloc[-3, 0]} (dawno)")
                cleaned_addresses.remove(a)
                continue

            if a in cleaned_addresses and not df.empty:
                result_df = result_df.append(pd.DataFrame({"address": [a], 'entry_price': [df.iloc[-1, -2]], "entry_time": [str(datetime.datetime.now())]}), ignore_index=True)

        print(tokens_to_recheck)
        pd.Series(tokens_to_recheck).to_csv("DataV2/TokensToRecheck.csv")
        result_df.reset_index(drop=True, inplace=True)

        return result_df
