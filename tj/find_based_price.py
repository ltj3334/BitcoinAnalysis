import os
import pandas as pd
import numpy as np


# mean_data = pd.read_csv('./result/chainalysis_darknet_kld_mean_result.csv', header=None)
# mean_data.columns = ['Address', 'value']
# raw_data = pd.read_csv('./result/chainalysis_darknet_kld_result.csv', header=None)
# raw_data.columns = ['Address' , 'value']

exchange = []
illegal = []

for file in os.listdir('./address'):
    if 'upbit' in file or 'bithumb' in file:
        exchange.append(file)
    else:
        illegal.append(file)


for index, file in enumerate(illegal):
    data = pd.read_csv(f'./address/{file}')
    print(file, len(data), index)

