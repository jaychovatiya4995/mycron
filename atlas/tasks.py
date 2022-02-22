import pandas as pd
import numpy as np
import re
from celery import shared_task
from celery.schedules import crontab
from atlas.models import Price

def cleaning(filepath):
    df = pd.read_excel(filepath, na_values=[' ', '  ','   ','    '])
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
        try: 
            return ('20' + i.split("'")[1] +'-12-01')
        except:   
            return ('20'+i[3:]+'-12-01')  
    elif re.match('^Q1', i):
        return ('20' + i.split("'")[1] +'-03-01')
    elif re.match('^Q2', i) or re.match('^1H', i):  
        try: 
            return ('20' + i.split("'")[1] +'-06-01')
        except:   
            return ('20'+i[3:]+'-06-01')
    elif re.match('^Q3', i):   
        return '20' + i.split("'")[1] +'-09-01'
    elif re.match('^C', i):  
        try:  
            return ('20' + i.split()[1] +'-12-01')
        except:    
            return ('20'+i[1:]+'-12-01')
    else:
        return pd.to_datetime(i).strftime('%Y-%m-%d')

 
def processing(df,report_date):   
    location = df.columns[0]
    df =pd.melt(df, id_vars=[location], var_name='date')
    df.rename(columns={location: 'instrument'}, inplace=True)
    df = df.dropna(axis=1, how='any')

    df.loc[df['date'].astype(str).str.contains('Q'), 'freq'] = 'Q'
    df.loc[df['date'].astype(str).str.contains('2H'), 'freq'] = 'H2'
    df.loc[df['date'].astype(str).str.contains('1H'), 'freq'] = 'H1'
    df.loc[df['date'].astype(str).str.contains('C'), 'freq'] = 'AVG'
    df['freq'] = df['freq'].fillna('M')

    df['report_date'] = report_date
    df['location'] = location
    df['date'] = df['date'].apply(convert_date)
    
    Price.objects.bulk_create(
        (Price(**vals) for vals in df.to_dict('records')), batch_size=5000
    )
    
    
@shared_task(autoretry_for=(Exception,), retry_kwargs={'countdown': 60})
def process():   
    filepath = 'Atlas Petroleum Price Curves - 7.1.2019.xlsx'
    df = pd.read_excel(filepath) 
    report_date = pd.to_datetime(df.iat[1,1]).strftime('%Y-%m-%d')
    df_list = cleaning(filepath) 
    for df in df_list:   
        processing(df,report_date)

