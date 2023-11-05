import os
import warnings
import pandas as pd
import numpy as np

from multiprocessing import Pool

warnings.filterwarnings("ignore")

# Define o número de processos a serem usados pelo multiprocessing
num_processes = os.cpu_count()

def calculate_ifd(df, period):
    # Calcula a variação dos preços
    delta = df['close'].diff()
    
    # Calcula o preço médio
    avg_price = (df['high'] + df['low'] + df['close']) / 3
    
    # Calcula o volume baseado na variação dos preços e no preço médio
    vol = (delta.abs() / avg_price).rolling(window=period).sum()
    
    # Calcula o IFD
    ifd = vol * delta / 10000.0
    
    return ifd


def process_csv_file(file_path, period):
    # Lê o arquivo CSV em um DataFrame
    df = pd.read_csv(file_path)

    # Adiciona colunas para high e low
    df['high'] = df['close']
    df['low'] = df['close']

    # Calcula o valor do IFD
    ifd = calculate_ifd(df, period)

    return round(ifd.iloc[-1], 2)




# Função paara remover arquivos desnecessários 
def clean_ativo(row):
    row = row.replace("data", "")
    row = row.replace(".csv", "")
    row = row.replace("\\", "")

    return str(row)


# Função para formatar os dados de "efficiency gains"
def clean_efficiency_gains(row):
    row = str(row) 
    row = row.replace("data", "")
    row = row.replace("(", "")
    row = row.replace(")", "")
    row = row.replace("\\",'')
    row = row.replace(".csv","")
    stock_name , gain = row.split(",")    
    gain =  gain.strip()

    return str(gain), stock_name



if __name__ == '__main__':
    # Define o período do IFD
    period = 6
    
    # Seleciona a data final
    days = '2021-08-17'

    # Quantos dias você quer analisar  
    num_days = 100


    # Lista todos os arquivos CSV na pasta "data"
    csv_files = [os.path.join('data', f) for f in os.listdir('data') if f.endswith('.csv')]

    with Pool(num_processes) as p:
        # Cria uma lista vazia para armazenar os resultados do IFD
        ifd_scores = []

        for file_path in csv_files:
            # Lê o arquivo CSV em um DataFrame
            df = pd.read_csv(file_path)
             # Filtra as datas que estão dentro do período especificado
            df['time'] = pd.to_datetime(df['time'])
            start_date = pd.Timestamp(days)
            df = df[(df['time'] >= start_date - pd.Timedelta(days=num_days))]

            print(df)
            # Calcula o valor do IFD para este arquivo específico
            ifd = calculate_ifd(df, period)

            # Adiciona o valor do IFD deste arquivo à lista de valores do IFD
            ifd_scores.append((file_path, round(ifd.iloc[-1], 2)))

        # Manipula os resultados do IFD
        df_tmp = pd.DataFrame({
            'Ativo': csv_files,
            'OSC': "IFD",
            'EFFICIENCY_GAINS': ifd_scores,
            'IFD_PERIODO': period,
        })
        
        df_tmp["Ativo"] = df_tmp["Ativo"].apply(clean_ativo)
        stock_names = df_tmp["Ativo"].tolist()
        df_tmp['EFFICIENCY_GAINS'] = df_tmp["EFFICIENCY_GAINS"].apply(clean_efficiency_gains)
        df_tmp[["EFFICIENCY_GAINS", "Ativo"]] = pd.DataFrame(df_tmp.EFFICIENCY_GAINS.tolist(), index=df_tmp.index)
        df_tmp['RANK'] = df_tmp['EFFICIENCY_GAINS'].rank(ascending=False, method='max').astype(int)
        df_tmp['EFFICIENCY_GAINS'] = df_tmp['EFFICIENCY_GAINS'].apply(lambda x: round(float(x), 4))
        df_tmp.sort_values('RANK', inplace=True)
        df_tmp =  df_tmp.dropna()
        for i, stk in enumerate(stock_names):
            df_tmp.loc[i, 'Ativo'] = stk
        
                

        print(df_tmp.head(23))

