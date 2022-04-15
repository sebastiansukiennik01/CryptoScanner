import json
import smtplib

from BitTimes import *
from BscScan import BscScan
from Filter import *

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class EmailCredentials:

    email = ""
    password = ""

    @staticmethod
    def __init__(self):
        with open("DataV2/credentials.json") as f:
            data = json.load(f)
            self.email = data['email']
            self.password = data['password']


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
    def send_email(tokens, email, password, to_address):
        server = smtplib.SMTP("smtp.mail.yahoo.com", 587)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login("sebastiansukiennik@yahoo.com", "gacsot-ninbi4-peXhim")
        server.sendmail(from_addr=email, to_addrs=to_address, msg=tokens.encode())
        server.quit()

    @staticmethod
    def addBoughtToHistoric(tokensToBuy):
        if not tokensToBuy.empty:

            historic_token_path = Path("DataV2/") / "HistoricTokens.csv"
            historicTokens = pd.read_csv(historic_token_path, index_col=0)
            historicTokens.loc[historicTokens['Address'].isin(tokensToBuy['address'].values), 'Bought'] = True
            historicTokens.to_csv(historic_token_path)


if __name__ == '__main__':
    #tokens_tobuy = Assisting.get_new_data()
    to = ['sebsuk2137@gmail.com']

    if True:
        #tresc = tokens_tobuy['address']
        tresc = pd.DataFrame(['cokolwiek', 'drugie']).iloc[:, 0].values
        wynik = ""
        for t in tresc:
            wynik = wynik + "," + t
        print(wynik.encode())
        Assisting.send_email(" m".encode(), EmailCredentials.email, EmailCredentials.password, to_address=['sebsuk2137@gmail.com'])
    else:
        #tresc = "nic nie ma ale dzialam".encode('utf-8').strip()
        #Assisting.send_email(tresc, email, passw, to_address=to)
        print("Nie znaleziono nowych tokenow spelniajacych warunki")



