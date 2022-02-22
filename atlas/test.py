from _future_ import absolute_import, unicode_literals

import glob
import re

import numpy as np
import pandas as pd
from celery import shared_task

from atlas.models import Price


def file_cleaning(filepath):
    df = pd.read_excel(filepath, na_values=[' ', '  ', '   ', '    '])
    colname = int(df.columns[len(df.columns)-1].split()[1])
    df[f'Unnamed: {colname+1}'] = np.NaN
    try:
        df = df.mask(df.astype(str).apply(lambda x: x.str.len().gt(20)))
    except:
        pass
    df_list = np.split(df, df[df.isnull().all(1)].index)
    clean_df = []
    for df in df_list:
        nan_cols = [i for i in df.columns if df[i].isnull().all()]
        if nan_cols[0] == ' ':
            nan_cols[0] = 'Unnamed: 0'
        temp_df = None
        for i in range(0, len(nan_cols)-1):
            newdf = df.iloc[:, int(nan_cols[i].split()[1]):int(nan_cols[i+1].split()[1])]
            newdf.dropna(axis=1, how='all',inplace=True)
            if not newdf.empty:
                newdf = newdf.reset_index(drop=True)
                new_df_list = np.split(
                            newdf, newdf[newdf.isnull().all(1)].index)
                for data in new_df_list:
                    data = data.dropna(axis=1, how='all')
                    data = data.dropna(axis=0, how='all')
                    if not data.empty:
                        new_header = data.iloc[0]
                        data = data[1:]
                        data.columns = new_header
                        data.columns = data.columns.fillna('Gulf')
                        if not data.empty:  
                            join = pd.to_numeric(
                                data.iloc[:, 0], errors='coerce').notnull().all()
                            if join:
                                data = pd.concat([temp_df, data],
                                                axis=1, join='inner')
                                clean_df.pop()
                                clean_df.append(data)
                            else:
                                clean_df.append(data)
                            temp_df = data

    return clean_df


def convert_date(x): 
    """ Convert different date formate to 'YYYY-MM-DD'  """
    i = str(x)
    if re.match('^Q4', i) or re.match('^2H', i):    
        return '20' + i[-2: ] +'-12-01'
    elif re.match('^Q1', i):
        return ('20' + i[-2: ] +'-03-01')
    elif re.match('^Q2', i) or re.match('^1H', i):   
        return ('20' + i[-2: ] +'-06-01')
    elif re.match('^Q3', i):   
        return '20' + i[-2: ] +'-09-01'
    elif re.match('^C', i):  
        return ('20' + i[-2: ] +'-12-01')
    else:
        return pd.to_datetime(i).strftime('%Y-%m-%d')


def df_processing(df, report_date):
    location = df.columns[0]
    df = pd.melt(df, id_vars=[location], var_name='date')
    df.rename(columns={location: 'instrument'}, inplace=True)
    df.dropna(inplace=True)
    if not df.empty:
        df.loc[df['date'].astype(str).str.contains('Q'), 'freq'] = 'Q'
        df.loc[df['date'].astype(str).str.contains('2H'), 'freq'] = 'H2'
        df.loc[df['date'].astype(str).str.contains('1H'), 'freq'] = 'H1'
        df.loc[df['date'].astype(str).str.contains('C'), 'freq'] = 'AVG'
        df['freq'] = df['freq'].fillna('M')

        df['report_date'] = report_date
        df['location'] = location
        df['date'] = df['date'].apply(convert_date)
        Price.objects.bulk_create(
            (Price(**vals) for vals in df.to_dict('records')), batch_size=10000
        )


@shared_task(autoretry_for=(Exception,), retry_kwargs={'countdown': 60})
def process():
    for filepath in sorted(glob.glob("./Dropbox (Kempstar)/Research & Analysis/DATA/Atlas EOD Prices/"+"*.xlsx")):
        df = pd.read_excel(filepath)
        report_date = pd.to_datetime(df.iat[1, 1]).strftime('%Y-%m-%d')
        data = file_cleaning(filepath)
        for df in data:
            df_processing(df, report_date)