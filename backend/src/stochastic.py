"""
Nesse caso, os números que precisam ser definidos são a janela de tempo (k_period) e o período médio (d_period). O cálculo do indicador segue a seguinte fórmula:

K% = 100 * ((Close - Lowest Low) / (Highest High - Lowest Low))

A partir dessa equação, calculamos a média móvel tanto para K%, quanto para D%. 
"""



import os
import warnings
import pandas as pd
import numpy as np
import datetime
import itertools

from multiprocessing import Pool

warnings.filterwarnings("ignore")

# Define o número de processos a serem usados pelo multiprocessing
num_processes = os.cpu_count()

# Função para calcular o Stochastic Oscillator de um DataFrame de preços
def calculate_stochastic(df, k_period, d_period):
    high_max = df['high'].rolling(k_period).max()
    low_min = df['low'].rolling(k_period).min()
    
    df['oscillator'] = 100 * ((df['close'] - low_min) / (high_max - low_min))

    df[f'%K {k_period}'] = round(df['oscillator'].rolling(k_period).mean(),2)
    df[f'%D {d_period}'] = round(df[f'%K {k_period}'].rolling(d_period).mean(),2)

    return df


# Função para processar um arquivo CSV e retornar as informações de ganho de eficiência
def process_csv_file(file_path, k_period, d_period):
    df = pd.read_csv(file_path)

    minimum_candles = 50
    if len(df) < minimum_candles:
        return np.nan
    
    df = calculate_stochastic(df, k_period=k_period, d_period=d_period)

    return round(df.tail(1)[f'%K {k_period}'].item(), 2)

# Função paara remover arquivos desnecessários 
def clean_ativo(row):
    row =  row.replace("data", "")
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
    """__Análise__
        define o k_period e d_period
    """
    k_period = 14
    d_period = 3
        
    #Seleiona a data final
    days = '2015-2-10'
    
    #Quantos dias você quer analisar  
    num_days = 300
        

    # Lista todos os arquivos CSV na pasta "data"
    csv_files = [os.path.join('data', f) for f in os.listdir('data') if f.endswith('.csv')]

    with Pool(num_processes) as p:
        # Cria uma lista vazia para armazenar os resultados da eficiência
        efficiency_gains = []

        for file_path in csv_files:
            # Lê o arquivo CSV em um DataFrame
            df = pd.read_csv(file_path)
            # Filtra as datas que estão dentro do período especificado
            df['time'] = pd.to_datetime(df['time'])
            start_date = pd.Timestamp(days)
            
            df = df[(df['time'] >= start_date - pd.Timedelta(days=num_days))]
            
            # Calcula a eficiência para este arquivo específico
            efficiency_gain = process_csv_file(file_path, k_period=k_period, d_period=d_period)

            # Adiciona a eficiência deste arquivo à lista de eficiências
            efficiency_gains.append((file_path, efficiency_gain))

        # Manipula os resultados
        df_tmp = pd.DataFrame({ 
                                'Ativo' : csv_files,
                                'OSC' : "Stochastic", 
                                'EFFICIENCY_GAINS': efficiency_gains,
                                'K_PERIOD' : k_period,
                                'D_PERIOD': d_period})
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