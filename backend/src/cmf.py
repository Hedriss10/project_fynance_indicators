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

# Função para calcular o CMF de um DataFrame de preços
def calculate_cmf(df, cmf_periods):
    mfv = ((df['close'] - df['low']) - (df['high'] - df['close']))/(df['high'] - df['low'])
    mfvm = mfv * df['Volume']
    cmf = mfvm.rolling(cmf_periods).sum() / df['Volume'].rolling(cmf_periods).sum()
    return cmf


def calculate_moving_average_crossover(df, ma_short, ma_long, cmf_periods):
    # Calcula o CMF
    df['cmf'] = calculate_cmf(df, cmf_periods)

    # Calcula as médias móveis do CMF
    col_short = 'ma_{}'.format(ma_short)
    col_long = 'ma_{}'.format(ma_long)
    df[col_short] = df['cmf'].rolling(ma_short).mean()
    df[col_long] = df['cmf'].rolling(ma_long).mean()

    # Identifica os cruzamentos de médias móveis
    df.loc[df[col_short] > df[col_long], 'cross'] = 1
    df.loc[df[col_short] <= df[col_long], 'cross'] = 0
    df['signal'] = df['cross'].diff()

    total_signals = len(df['signal'])
    total_correct_signals = sum(1 for s in df['signal'] if s > 0)

    efficiency_gain = 0 if total_signals == 0 else total_correct_signals / total_signals

    return efficiency_gain


# Função para processar um arquivo CSV e retornar as informações de ganho de eficiência
def process_csv_file(file_path, ma_short, ma_long, cmf_periods):
    df = pd.read_csv(file_path)
    efficiency_gain = calculate_moving_average_crossover(df, ma_short, ma_long, cmf_periods)
    return round(efficiency_gain, 2)


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
        define o short e long , periods ,
    """
    ma_short = 10
    ma_long = 30
    cmf_periods = 21

    # Seleciona a data final
    days = '2015-2-10'
    
    # Quantos dias você quer analisar  
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
            efficiency_gain = calculate_moving_average_crossover(df, ma_short, ma_long, cmf_periods)

            # Adiciona a eficiência deste arquivo à lista de eficiências
            efficiency_gains.append((file_path, efficiency_gain))

        # Manipula os resultados
        df_tmp = pd.DataFrame({ 
                                'Ativo' : csv_files,
                                'OSC' : "CMF", 
                                'EFFICIENCY_GAINS': efficiency_gains,
                                'ma_short':  ma_short,
                                'ma_long': ma_long,
                                'CMF_PERIODS': cmf_periods,
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