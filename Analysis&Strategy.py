import datetime
import glob

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

def plot_PriceVsValue(df):

    i = 0
    j = 0

    fig, ax = plt.subplots(2, 2, figsize=(16, 9))
    for item in df.items():
        print(type(item[1]))
        if not(isinstance(item[1], pd.DataFrame)) or item[1].empty:
            continue
        ax[j][i].plot(item[1]['Datetime'], item[1]['C'])
        ax[j][i].set_ylabel("Price", color='blue')
        ax[j][i].tick_params(axis='y', labelcolor='blue')
        ax[j][i].set_xlabel("Datetime")
        ax[j][i].set_title(item[0])

        ax2 = ax[j][i].twinx()
        ax2.set_ylabel('Trade Amount', color='red')
        ax2.tick_params(axis='y', labelcolor='red')
        ax2.plot(item[1]['Datetime'], item[1]['trades'], color='red', alpha=0.3)
        if i == 1:
            i = 0
            j = 1
        else:
            i += 1

    fig.tight_layout()
    plt.show()

def loadData(names:list):
    names = np.array(names)
    print(names.shape)
    for i in range((names.shape[0]//4 + 1)*4 - names.shape[0]):
        names = np.append(names, np.nan)
    names = np.reshape(names, (-1, 4))

    for name in names:
        df_dic = {}
        last24 = {}
        for n in name:
            if n != "nan":
                print(n)
                df = pd.read_csv(f"Data/CSV/{n}.csv", parse_dates=True)

                if df.empty:
                    last24[n] = np.nan
                    continue

                df.rename(columns={"timeInterval_minute": "Datetime", "maximum_price": "H", "minimum_price": "L", "open_price": "O", "close_price": "C"}, inplace=True)
                df['Datetime'] = pd.to_datetime(df['Datetime'])
                df.drop(columns="Unnamed: 0", inplace=True)

                t1 = df.iloc[-1, 0] - datetime.timedelta(days=1)
                last24[n] = df[df['Datetime'] > t1]
        plot_PriceVsValue(last24)

if __name__ == '__main__':

    file_names = glob.glob("Data/CSV/*.csv")
    for fn in file_names:
        file_names[file_names.index(fn)] = fn.split("/")[2][:-4]
    loadData(file_names)

