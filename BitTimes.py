from pathlib import Path

import pandas as pd
import requests


class BitTimes:

    url = "https://thebittimes.com/coins-new.html"

    @staticmethod
    def get_token_addresses():
        """
        Scrapes new token addresses from https://thebittimes.com/coins-new.html
        Adds new addresses to DataV2/HistoricTokens.csv where all ever colelcted addresses are stored.
        Saves current new addresses to NewestBitimeTokens.csv (50 tokens listed on BitTimes right now)
        Returns lsit of addreses that were added on BitTimes since last iteration of this functions.

        :return: dict of new addresses {address: tokenSymbol}
        """

        print("\n\n----------------- Pobieranie nowych tokenow z BitTimes-----------------")
        resp = requests.get(BitTimes.url)
        resp_list = resp.text.split('<tr data-network="BSC">')
        del resp_list[0]

        actual_tokens = pd.DataFrame()
        token_addresses = []
        for i in resp_list:
            token_address = i.split('href="')[1].split('">')[0].split('-')[-1].split('.')[0]
            token_symbol = i.split('title=')[1].split('(')[1].split(')')[0]
            token_datetime = i.split('text-align: center;" title=')[2].split('Deploy At')[1].split('data-breakpoints')[0].replace('"', '')[1:-1]
            token_chain = i.split('notranslate hide-mobile" style=')[1].split('data-breakpoints="xs">')[1].split("</td>")[0]
            if "BSC" not in token_chain:
                print("NIE BSC\n\n\n\n")
                continue
            token_datetime = pd.to_datetime(token_datetime).tz_convert('Europe/Berlin')
            actual_tokens = actual_tokens.append({'Address': token_address, 'Symbol': token_symbol, 'DateTime': token_datetime}, ignore_index=True)

        historic_token_path = Path("DataV2/") / "HistoricTokens.csv"
        historic_tokens = pd.read_csv(historic_token_path, index_col=0)
        previous_tokens_path = Path("DataV2/") / "NewestBitTimesTokens.csv"
        previous_tokens = pd.read_csv(previous_tokens_path, index_col=0, parse_dates=['DateTime'])
        actual_tokens.to_csv(previous_tokens_path)

        print("actual\n", actual_tokens)
        print("previous\n", previous_tokens)
        lastDate = pd.to_datetime(previous_tokens.iloc[0, 2], utc=True)
        tokens_to_check = actual_tokens.append(previous_tokens).drop_duplicates(subset=['Address'], keep=False)  # leaves only those tokens that were not checked last time
        tokens_to_check['DateTime'] = pd.to_datetime(tokens_to_check['DateTime'], utc=True)

        tokens_to_check = tokens_to_check[actual_tokens['DateTime'] > lastDate]
        print("tokens to check:\n", tokens_to_check)

        historic_tokens = historic_tokens.append(actual_tokens, ignore_index=True)  # append tokens (without duplicates) to historic tokens
        historic_tokens.drop_duplicates(subset=['Address'], keep="first", inplace=True)
        historic_tokens['DateTime'] = pd.to_datetime(historic_tokens['DateTime'], utc=True)
        historic_tokens.sort_values(by='DateTime', inplace=True)
        historic_tokens.reset_index(drop=True, inplace=True)
        historic_tokens.to_csv(historic_token_path)


        for row in tokens_to_check.iterrows():
            token_addresses.append(row[1]['Address'])
        tokens_to_recheck = pd.read_csv("DataV2/TokensToRecheck.csv", index_col=0).iloc[:, 0].to_list()
        print("to recheck:\n", tokens_to_recheck)
        token_addresses += tokens_to_recheck
        if len(tokens_to_recheck) > 70:
            token_addresses = token_addresses[:70]

        print(token_addresses)

        return token_addresses

    @staticmethod
    def get_token_holders(addresses):
        """
        Scrapes token holders from https://thebittimes.com/token-TOKEN_NAME-BSC-0x6d5885619126e388c377729b2296003d11e5f85c.html for each token.
        Returns a dictionary {key: address, value: df of shareholders}

        :param addresses: dict of addresses and symbols
        :return: Dictionary of dataframes with share holders
        """

        print(f"\n\n----------------- Pobieranie holderow dla tokenow -----------------")
        holders = {}
        for addr, symb in addresses.items():
            resp = requests.get(f"https://thebittimes.com/token-{symb}-BSC-{addr}.html")
            print(f"https://thebittimes.com/token-{symb}-BSC-{addr}.html")
            holders_list = resp.text.split('window.holders')[1].split('</script>')[0][3:].split(';')[0].split(',[')


            df = pd.DataFrame(columns=['address', 'shares'])
            try:
                for i in range(len(holders_list)):
                    holders_list[i] = holders_list[i].replace('[', '')
                    holders_list[i] = holders_list[i].replace(']', '')
                    holders_list[i] = holders_list[i].replace('"', '')
                    holders_list[i] = holders_list[i].split(',')
                    df = df.append({'address': holders_list[i][0], 'shares': float(holders_list[i][1])}, ignore_index=True)
            except:
                print("Blad przy pobieraniu holderow")
                continue
            df['percentage'] = df['shares']/df['shares'].sum()
            holders[addr] = df

        return holders
