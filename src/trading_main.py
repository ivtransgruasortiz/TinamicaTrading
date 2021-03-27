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
import seaborn as sns
import matplotlib.pyplot as plt

import statsmodels
from statsmodels.tsa.stattools import adfuller

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
            data['bin_aumento_intradia'] = np.vectorize(lambda x, y: (-1) if (y <= x) else 1, otypes=['int'])(data['open'], data['close'])
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
            data = data[['ticket', 'date', 'open', 'close', 'bin_aumento_intradia', 'volume', f'close_forward_{periodo_corto}',
                             f'close_backward_{periodo_corto}', f'close_backward_{periodo_largo}',
                             f'media_{periodo_corto}', f'media_{periodo_largo}', 'macd',
                             f'var_backward_{periodo_corto}_porcentaje', f'var_backward_{periodo_largo}_porcentaje',
                             f'var_obj_forward_{periodo_corto}_porcentaje']]
            df_tot = df_tot.append(data)
        if sp500:
            df_companies_sp500_fi = pd.read_csv(str(root) + '/src/data/constituents-financials_csv.csv')
            cols = [re.sub(' |/', '_', x.lower()) for x in df_companies_sp500_fi.columns]
            df_companies_sp500_fi.columns = cols
            df_companies_sp500_fi = df_companies_sp500_fi[['symbol', 'name', 'sector', 'price_earnings',
                                                           'dividend_yield', 'market_cap', 'ebitda',
                                                           'price_sales']]
            df_companies_sp500_fi = df_companies_sp500_fi.rename(columns={'symbol': 'ticket'}).set_index('ticket')
            df_tot = df_tot.set_index('ticket').join(df_companies_sp500_fi, how='left', on='ticket',
                                                       lsuffix='_left', rsuffix='_right')
            df_tot = df_tot.reset_index().set_index('date')
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
    sp500 = True
    df_tot = main_get_trades(escritura, sp500)
    df_tot_agrup_ticket = df_tot.groupby('ticket').apply(lambda x: x.sort_values(["date"], ascending=True)).reset_index(
        drop=True)
    df_tot_agrup_date = df_tot.groupby('date').apply(lambda x: x.sort_values(["date", 'ticket'], ascending=True)).reset_index(
        drop=True)

    ### EDA - PYTHON ###
    df_tot_agrup_date.corr()
    correlation_mat = df_tot_agrup_date.corr(method='spearman')
    corr_pairs = correlation_mat.unstack()

    # plot the correlation matrix of salary, balance and age in data dataframe.
    sns.heatmap(df_tot_agrup_date[['var_obj_forward_5_porcentaje', 'macd', 'volume', 'bin_aumento_intradia']]
                .corr(), annot=True, cmap='Reds')
    plt.show()

    # # plot the pair plot of salary, balance and age in data dataframe.
    # sns.pairplot(data=df_tot_agrup_date, vars=['var_obj_forward_5_porcentaje', 'macd', 'volume',
    #                                            'bin_aumento_intradia', 'media_5', 'media_60', 'close',
    #                                            'var_backward_5_porcentaje'])
    # plt.show()

    # plot the correlation matrix of salary, balance and age in data dataframe.
    sns.heatmap(df_tot_agrup_date[['var_obj_forward_5_porcentaje', 'macd', 'volume', 'bin_aumento_intradia',
                                   'var_backward_5_porcentaje', 'var_backward_60_porcentaje']]
                .corr(), annot=True, cmap='Reds')
    plt.show()

    df_tot_agrup_date.hist()

    ## Histogram
    ax = df_tot_agrup_date.hist(column='var_backward_60_porcentaje', bins=250, grid=False, figsize=(12, 8), color='#86bf91', zorder=2,
                 rwidth=0.9)


    ## Para la estacionariedad
    # class StationarityTests:
    #     def __init__(self, significance=.05):
    #         self.SignificanceLevel = significance
    #         self.pValue = None
    #         self.isStationary = None
    #
    #
    # def ADF_Stationarity_Test(self, timeseries, printResults=True):
    #     # Dickey-Fuller test:
    #     adfTest = adfuller(timeseries, autolag='AIC')
    #
    #     self.pValue = adfTest[1]
    #
    #     if (self.pValue < self.SignificanceLevel):
    #         self.isStationary = True
    #     else:
    #         self.isStationary = False
    #
    #     if printResults:
    #         dfResults = pd.Series(adfTest[0:4],
    #                               index=['ADF Test Statistic', 'P-Value', '# Lags Used', '# Observations Used'])
    #         # Add Critical Values
    #         for key, value in adfTest[4].items():
    #             dfResults['Critical Value (%s)' % key] = value
    #         print('Augmented Dickey-Fuller Test Results:')
    #         print(dfResults)
    #
    #     sTest = StationarityTests()
    #     sTest.ADF_Stationarity_Test(non_stationary_series, printResults=True)
    #     print("Is the time series stationary? {0}".format(sTest.isStationary))

