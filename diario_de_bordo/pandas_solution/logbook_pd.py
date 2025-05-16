import pandas as pd

def convert_to_date_format(dataframe: pd.DataFrame, column_date: str) -> pd.DataFrame:
    dataframe[column_date] = pd.to_datetime(dataframe[column_date], format="%m-%d-%Y %H:%M", errors='coerce')
    return dataframe

def create_normalized_date_column(dataframe: pd.DataFrame)-> pd.DataFrame:
    dataframe['DT_REFE'] = dataframe['DATA_INICIO'].dt.normalize()
    return dataframe

def aggregate_logbook_by_date(dataframe: pd.DataFrame) -> pd.DataFrame:
    new_logbook = dataframe.groupby('DT_REFE').agg(
        QT_CORR=('DT_REFE', 'size'),
        QT_CORR_NEG=('CATEGORIA', lambda x: (x == 'Negocio').sum()),
        QT_CORR_PESS=('CATEGORIA', lambda x: (x == 'Pessoal').sum()),
        VL_MAX_DIST=('DISTANCIA', 'max'),
        VL_MIN_DIST=('DISTANCIA', 'min'),
        VL_AVG_DIST=('DISTANCIA', 'mean'),
        QT_CORR_REUNI=('PROPOSITO', lambda x: (x == 'Reunião').sum()),
        QT_CORR_NAO_REUNI=('PROPOSITO', lambda x: (x != 'Reunião').sum())
    ).reset_index()

    new_logbook['VL_AVG_DIST'] = new_logbook['VL_AVG_DIST'].round(2)
    new_logbook['DT_REFE'] = new_logbook['DT_REFE'].dt.strftime('%Y-%m-%d')

    return new_logbook

def extract(path:str)-> pd.DataFrame:
    racing_info = pd.read_csv(path, sep=";", encoding="utf-8")
    print("extraiu o arquivo")
    return racing_info

def transform(dataframe: pd.DataFrame):
    df_transformed = convert_to_date_format(dataframe, 'DATA_INICIO')
    df_transformed = create_normalized_date_column(df_transformed)
    new_logbook = aggregate_logbook_by_date(df_transformed)
    print("transformou o arquivo")
    return new_logbook

def load(dataframe: pd.DataFrame,path: str):
    dataframe.to_parquet(f'{path}.parquet', index=False)
    dataframe.to_csv(f'{path}.csv', header=True, index=False, sep=";")
    print("salvou o arquivo")

def main():
    racing_info = extract("./diario_de_bordo/info_transportes.csv")
    logbook = transform(racing_info)
    load(logbook, "./diario_de_bordo/results/info_corridas_do_dia")

if __name__ == "__main__":
    main()

    

#python diario_de_bordo\pandas_solution\logbook_pd.py






