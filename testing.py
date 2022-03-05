import datetime
import smtplib

import pandas as pd
from TokenScraper import *


def send_email(tokens, email, password, to_address):

    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(email, password)
    server.sendmail(from_addr="email", to_addrs=to_address, msg=str(tokens))
    server.quit()



tokens_tobuy = Assisting.get_new_data()
#tokens_tobuy = pd.read_csv("TokensToBuy.csv", index_col=0)
email = "spoko369@gmail.com"
passw = "Spoko#258"
to = ['sebsuk2137@gmail.com', ]

if not tokens_tobuy.empty:
    tresc = tokens_tobuy['address']
    send_email(tresc, email, passw, to_address=to)
else:
    print("Nie znaleziono nowych tokenow spelniajacych warunki")



