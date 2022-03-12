from TokenScraper import *

tokens_tobuy = Assisting.get_new_data()
#tokens_tobuy = pd.read_csv("TokensToBuy.csv", index_col=0)
email = "spoko369@gmail.com"
passw = "Spoko#258"
to = ['sebsuk2137@gmail.com', ]

if not tokens_tobuy.empty:
    tresc = tokens_tobuy['address']
    Assisting.send_email(tresc, email, passw, to_address=to)
else:
    print("Nie znaleziono nowych tokenow spelniajacych warunki")
