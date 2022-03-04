from TokenScraper import *

class Backtesting:

    @staticmethod
    def get_new_data():
        #Pobiera adresy, sprawdza dla nich holderów, zwraca listę adresów z dobrymi holderami
        addrs = BitTimes.get_token_addresses()
        hold = BitTimes.get_token_holders(addrs)
        hold_filtered = Filter.sift_by_holders(hold)

        #Pobieram listę transakcji dla potencjalnie dobrych holderów i sprawdzam je pod względem płynności
        print(hold_filtered.keys())
        tran = BscScan.get_tokens_transactions(addrs)
        tran_filtered = Filter.sift_by_transactions(tran)
        print(tran_filtered.keys())

        pd.Series(list(tran_filtered.keys()), dtype=object).to_csv("TokensToBuy.csv")

        return list(tran_filtered.keys())


if __name__ == '__main__':
    Backtesting.get_new_data()
    tokens_tb = pd.read_csv("TokensToBuy.csv", index_col=0).values.reshape(-1)
    Filter.sift_by_volume_launchDate(tokens_tb)
