#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import datetime
import math
import quandl
import os
import sys
import requests as rq
import tqdm
import re

from pathlib import Path, PurePath, PureWindowsPath
if '__file__' in locals():
    root = Path('__file__').resolve().parents[0]
    print(root)
    # sys.path.append(root)
else:
    root = '/Users/Tinamica/PycharmProjects/TinamicaTrading'
    print(root)

def main_get_trades(escritura = True, sp500 = False):
    if escritura:
        quandl.ApiConfig.api_key = "iNQrTcLb77QoEWzAzjUs"
        df_companies = pd.read_csv(str(root) + '/src/data/WIKI_PRICES.csv')
        if sp500:
            df_companies_sp500 = pd.read_csv(str(root) + '/src/data/constituents_csv.csv')
            lista_companies = [re.sub('\.|-', '_', x) for x in df_companies_sp500['Symbol']]
        else:
            lista_companies = [x for x in df_companies['ticker']]

        df_tot = pd.DataFrame()
        periodo_corto = 5
        periodo_largo = 60
        alfa = 2
        porcentaje_corto = alfa / (periodo_corto + 1)
        porcentaje_largo = alfa / (periodo_largo + 1)
        # for item in df_companies['ticker'].tolist()):
        for item in tqdm.tqdm(lista_companies):
            data = quandl.get('WIKI/' + item, start_date="2012-09-01", end_date="2016-03-31").reset_index()
            data['ticket'] = item
            data = data[['ticket', 'Date', 'Open', 'Close', 'Volume']]
            data.columns = ['ticket', 'date', 'open', 'close', 'volume']
            data = data.sort_values('date', ascending=True)
            data[f'close_forward_{periodo_corto}'] = data['close'].shift(periods=-periodo_corto, fill_value=0)
            data[f'close_backward_{periodo_corto}'] = data['close'].shift(periods=periodo_corto, fill_value=0)
            data[f'close_backward_{periodo_largo}'] = data['close'].shift(periods=periodo_largo, fill_value=0)
            data[f'media_{periodo_corto}'] = round(data['close'] * porcentaje_corto +
                                                   data[f'close_backward_{periodo_corto}'] *
                                                   (1 - porcentaje_corto), 2)
            data[f'media_{periodo_largo}'] = round(data['close'] * porcentaje_largo +
                                                   data[f'close_backward_{periodo_largo}'] *
                                                   (1 - porcentaje_largo), 2)
            data['macd'] = data[f'media_{periodo_corto}'] - data[f'media_{periodo_largo}']
            data = data[(data['date'] >= '2013-01-01') & (data['date'] <= '2015-12-31')]
            data[f'var_obj_forward_{periodo_corto}_porcentaje'] = round((data[f'close_forward_{periodo_corto}'] /
                                                                         data['close'])
                                                                        - 1, 4) * 100
            data[f'var_backward_{periodo_corto}_porcentaje'] = round((data['close'] /
                                                                      data[f'close_backward_{periodo_corto}'])
                                                                     - 1, 4) * 100
            data[f'var_backward_{periodo_largo}_porcentaje'] = round((data['close'] /
                                                                      data[f'close_backward_{periodo_largo}'])
                                                                     - 1, 4) * 100
            data = data[['ticket', 'date', 'open', 'close', 'volume', f'close_forward_{periodo_corto}',
                             f'close_backward_{periodo_corto}', f'close_backward_{periodo_largo}',
                             f'media_{periodo_corto}', f'media_{periodo_largo}', 'macd',
                             f'var_backward_{periodo_corto}_porcentaje', f'var_backward_{periodo_largo}_porcentaje',
                             f'var_obj_forward_{periodo_corto}_porcentaje']]
            df_tot = df_tot.append(data)
        if sp500:
            df_tot.to_csv(str(root) + '/src/data/df_total_base_sp500.csv', index=False)
        else:
            df_tot.to_csv(str(root) + '/src/data/df_total_base.csv', index=False)

    else:
        if sp500:
            df_tot = pd.read_csv(str(root) + '/src/data/df_total_base_sp500.csv')
        else:
            df_tot = pd.read_csv(str(root) + '/src/data/df_total_base.csv')
    return df_tot


if __name__ == '__main__':
    print('Hi! Executing trading.py...')
    escritura = True
    sp500 = False
    df_tot = main_get_trades(escritura, sp500)
