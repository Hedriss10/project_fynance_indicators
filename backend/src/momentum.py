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

# Função para calcular a estratégia de Momentum em um DataFrame de preços
def calculate_momentum_strategy(df, mom_periods, holding_periods):
    # Calcula o retorno de holding_periods dias no futuro
    df['future_price'] = df['close'].shift(-holding_periods)
    df['return'] = (df['future_price'] - df['close'])/df['close']

    # Calcula o retrono de mom_periods atrás até hoje
    df['momentum'] = df['close']/df['close'].shift(mom_periods) - 1

    # Classifica as ações com base em seu índice momentum
    df['class'] = pd.cut(df['momentum'], bins=5, labels=range(5))

    # Calcula os retornos médios diários de cada classe
    daily_returns_by_class = df.groupby('class')['return'].mean()

    # Retorna uma série com os retornos de cada classe ponderados pelo número de dias que cada classe aparece
    return (daily_returns_by_class * df.groupby('class').size() / len(df)).sum()


#Função paara remover arquivos desnecessários 
def clean_ativo(row):
    row =  row.replace("data", "")
    row = row.replace(".csv", "")
    row = row.replace("\\", "")

    return str(row)

#Função para remover arquivos desnecessários
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
        define o mom_period , holding_period 
    """
    mom_periods = 20
    holding_periods = 5
        
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
            efficiency_gain = calculate_momentum_strategy(df, mom_periods, holding_periods)

            # Adiciona a eficiência deste arquivo à lista de eficiências
            efficiency_gains.append((file_path, efficiency_gain))

        # Manipula os resultados
        df_tmp = pd.DataFrame({ 
                                'Ativo' : csv_files,
                                'OSC' : "Momentum", 
                                'EFFICIENCY_GAINS': efficiency_gains,
                                'MOM_PERIOD' : mom_periods,
                                'HOLDING_PERIOD': holding_periods})

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
            
        print(df_tmp)