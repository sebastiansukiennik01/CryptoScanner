import datetime
import glob

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from itertools import islice

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


def oblicz_kowaraiancje(item):
    covr = item[1].cov()
    cov_stats = pd.DataFrame({"Name": covr[['H', 'L', 'O', 'C']].idxmax().values, "Value": covr[['H', 'L', 'O', 'C']].max().values})
    print(f"\nElement z największą cov: \n{cov_stats}\n\n")

def calc_additional_columns(dfs):
    """
    Calculates additional columns.
    Returns dict of type:   TOKEN_NAME: TOKEN_DATA_DF
    :param dfs: dictionary
    :return: dictionary
    """
    for name, d in dfs.items():
        #print(f"\n\n{name}\n", d.head())
        d["C-O"] = d["C"] - d["O"]  # Różnica między zamknięciem a otwarciem tej samej świeczki
        d["O_diff"] = d["O"].diff()     # Różnica między otwarciem danej świeczki a otwarciem poprzedniej
        d['tradeAmount_pctchange'] = d['tradeAmount'].pct_change() #Percanteage change from previous candle tradeAmount
        d["AvgTrAm"] = d["tradeAmount"] / d["trades"]       # Średnia wielkośc tradu w danej świeczce (np. 1000$/5 tradów da średnią 200$)
        d["MA3_TrAm"] = d["AvgTrAm"].rolling(3).mean()         # Średnia ruchoma (3 okresowa) z ostatnich średnich wielkości tradeów
        d["MMin2_O_diff"] = d["O_diff"].rolling(2).min()        # Najmniejsza wartość z dwóch ostatnich wartości 'O_diff'
        d["CandleStrength"] = (d['C']-d['O'])/(d['H']-d['L'])   # Siła świeczki przyjmuje wartoścci od (-1,1) gdzie 1 to cała zielona świeczka, -1 to cała czerwona,
                                                                # wynik w okolicy zera oznacza że cena zamknięcia i otwarcią były podobne
        d['Warunek1'] = (d['tradeAmount_pctchange'] > 2) & (d['trades'] > 5) &(d['O_diff'] > 0) # Warunek 1 testowanej strategi, wskazuje miejsca entry
        #print(d.head())

    return dfs


def my_reshape(list_to_reshape, n):

    list_to_return = np.array(list_to_reshape)
    for i in range((list_to_return.size// 4 + 1) * n - list_to_return.size):
        list_to_return = np.append(list_to_return, np.nan)
    list_to_return = np.reshape(list_to_return, (-1, n))

    return list_to_return

def plot_PriceVsValue(df):

    values = list(df.values())
    keys = list(df.keys())

    for k in range(0, len(values), 6):
        idx = keys[k:k+6]
        fig, axs = plt.subplots(3, 2, figsize=(16, 9.5))
        i = 0
        j = 0

        for id in idx:
            print(df[id].head())
            axs[i][j].plot(df[id]['Datetime'], df[id]['O'], color='blue')
            axs[i][j].set_title(id)

            axs1 = axs[i][j].twinx()
            #axs1.plot(df[id]['Datetime'], df[id]['tradeAmount'], color='red', alpha=0.3)
            #axs1.legend(['Open', 'Trades'])
            positions = df[id].loc[df[id]['Warunek1'], 'Datetime']
            for pos in positions:
                axs1.axvline(pos, 0, 1, color='green', alpha=0.4)

            if j == 1:
                j = 0
                i += 1
            else:
                j += 1
        plt.show()



def loadData(names:list):
    """
    Returns dictionary of type:    TOKEN_NAME: DF_WITH_TOKEN_DATA
    :param names: list of token names
    :return:
    """


    token_dict = {}
    for name in names:
        if name != "nan":
            df = pd.read_csv(f"Data/CSV/{name}.csv", parse_dates=True)

            if df.empty:
                continue

            df.rename(columns={"timeInterval_minute": "Datetime", "maximum_price": "H", "minimum_price": "L", "open_price": "O", "close_price": "C"}, inplace=True)
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df.drop(columns="Unnamed: 0", inplace=True)

            t1 = df.iloc[-1, 0] - datetime.timedelta(days=1)
            token_dict[name] = df

    return token_dict

def tradeAmoutn_X_newHigh(data):
    """
    :param data: Dictionary of TOKEN: DF with OHLC
    :return:
    """
    for d in data.values():
        d["C-O"] = d["C"] - d["O"]
        d["O_diff"] = d["O"].diff()
        d["AvgTrAm"] = d["tradeAmount"] / d["trades"]
        d["AvgTrAm3"] = d["AvgTrAm"].rolling(3).mean()
        d["MinOpDiff2"] = d["O_diff"].rolling(2).min()
        d["War1"] = (d["AvgTrAm"] > 2*d["AvgTrAm3"]) & (d['MinOpDiff2'] > 0) & (d['trades'] > 2) & (d["tradeAmount"] > 1000)

        warId = d.index[d['WarVolume']]
        print(warId)

        print(d.head(60))

        zwrot = []

        for i in range(0, len(warId)-1):
            td = d.loc[warId[i]:, ]
            #print(td)
            win = td.loc[td['H'] >= 1.1 * td.loc[warId[i], 'O'], 'O'].first_valid_index()
            lose = td.loc[td['L'] <= 0.9 * td.loc[warId[i], 'O'], 'O'].first_valid_index()
            if win == None or lose == None:
                zwrot.append(1)
            elif win > lose:
                zwrot.append(0.9)
            elif win <= lose:
                zwrot.append(1.1)

            print(f"win: {win} \n lose: {lose}")

        print(zwrot)
        print(f"suma: {sum(zwrot)}")
        print(len(warId))


        fig, axs = plt.subplots(3, 1, figsize=(12, 9))
        axs[0].plot(d["Datetime"], d["O"], color="red", alpha=0.3)
        axs[1].plot(d["Datetime"], d["AvgTrAm3"], color="blue", alpha=0.3)
        axs[0].legend(["O", "C"])
        #plt.show()


if __name__ == '__main__':

    file_names = glob.glob("Data/CSV/*.csv")
    for fn in file_names:
        file_names[file_names.index(fn)] = fn.split("/")[2][:-4]
    data = loadData(file_names)
    data = calc_additional_columns(data)
    plot_PriceVsValue(data)





