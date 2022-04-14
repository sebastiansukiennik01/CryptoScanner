from TokenScraper import *

tokens_tobuy = Assisting.get_new_data()
#tokens_tobuy = pd.read_csv("TokensToBuy.csv", index_col=0)

print(tokens_tobuy)
email = "spoko369@gmail.com"
passw = "Spoko#258"
to = ['sebsuk2137@gmail.com', ]

if not tokens_tobuy.empty:
    tresc = tokens_tobuy['address']
    Assisting.send_email(tresc, email, passw, to_address=to)
else:
    #tresc = "nic nie ma ale dzialam".encode('utf-8').strip()
    #Assisting.send_email(tresc, email, passw, to_address=to)
    print("Nie znaleziono nowych tokenow spelniajacych warunki")
