import pytest
import pandas as pd
from diario_de_bordo.pandas_solution import logbook_pd



def test_convert_to_date_format():

    df = logbook_pd.extract("./diario_de_bordo/info_transportes.csv")
    df = logbook_pd.convert_to_date_format(df, 'DATA_INICIO')
    assert df['DATA_INICIO'].dtype == 'datetime64[ns]'
    assert df['DATA_INICIO'].isnull().sum() == 0  


def test_convert_to_date_format_correct():
    df = pd.DataFrame({
        'DATA_INICIO': ['01-13-2023 10:00', '02-01-2023 11:00', '03-20-2023 12:00']
    })
    df = logbook_pd.convert_to_date_format(df, 'DATA_INICIO')
    
    expected_dates = pd.Series(pd.to_datetime(['2023-01-13 10:00', '2023-02-01 11:00', '2023-03-20 12:00']))

    assert df['DATA_INICIO'].reset_index(drop=True).equals(expected_dates)

def test_create_normalized_date_column():
    df = pd.DataFrame({
        'DATA_INICIO': pd.to_datetime(['2023-01-13 10:00', '2023-02-01 11:00', '2023-03-20 12:00'])
    })
    df = logbook_pd.create_normalized_date_column(df)
    
    expected_dates = pd.Series(pd.to_datetime(['2023-01-13', '2023-02-01', '2023-03-20']))
    
    assert df['DT_REFE'].reset_index(drop=True).equals(expected_dates)

def test_aggregate_logbook_by_date():
    df = pd.DataFrame({
        'DT_REFE': pd.to_datetime(['2023-01-13', '2023-01-13', '2023-02-01']),
        'CATEGORIA': ['Negocio', 'Pessoal', 'Negocio'],
        'DISTANCIA': [10, 20, 30],
        'PROPOSITO': ['Reunião', 'Outro', 'Reunião']
    })
    
    aggregated_df = logbook_pd.aggregate_logbook_by_date(df)
    
    expected_df = pd.DataFrame({
        'DT_REFE': pd.to_datetime(['2023-01-13', '2023-02-01']),
        'QT_CORR': [2, 1],
        'QT_CORR_NEG': [1, 1],
        'QT_CORR_PESS': [1, 0],
        'VL_MAX_DIST': [20, 30],
        'VL_MIN_DIST': [10, 30],
        'VL_AVG_DIST': [15.0, 30.0],
        'QT_CORR_REUNI': [1, 1],
        'QT_CORR_NAO_REUNI': [1, 0]
    })
    
    expected_df['VL_AVG_DIST'] = expected_df['VL_AVG_DIST'].round(2)
    expected_df['DT_REFE'] = expected_df['DT_REFE'].dt.strftime('%Y-%m-%d')
    
    assert aggregated_df.equals(expected_df)



