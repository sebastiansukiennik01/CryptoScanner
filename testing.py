import smtplib

from TokenScraper import *


def send_email(tokens: list, email, password, to_address):
    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(email, password)
    server.sendmail(from_addr="email", to_addrs=to_address, msg=str(tokens))
    server.quit()




if __name__ == '__main__':
    tokens_tobuy = Assisting.get_new_data()
    print(tokens_tobuy)
    email = "spoko369@gmail.com"
    passw = "Spoko#258"
    to = ['sebsuk2137@gmail.com', 'sebastiansukiennik01@gmail.com']
    send_email(tokens_tobuy, email, passw, to_address=to)


