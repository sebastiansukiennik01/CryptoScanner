import json
import smtplib


"""
############ PLIK WYŁĄCZONY Z GITA (zmiany kodu tutaj nie będą aktualizowane do git) ############
"""



class Email:
    email = ''
    password = ''

    @staticmethod
    def __init__(self):
        with open("DataV2/credentials.json") as f:
            data = json.load(f)
            self.email = data['email']
            self.password = data['password']

    @staticmethod
    def send_email(message, to_address):

        """
        ############ PLIK WYŁĄCZONY Z GITA (zmiany kodu tutaj nie będą aktualizowane do git) ############
        """

        try:
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login('sebsuk2137@gmail.com', 'tommybog')
            server.sendmail('sebsuk2137@gmail.com', to_address, message)
            server.close()
        except Exception as e:
            raise e
