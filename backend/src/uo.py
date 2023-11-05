import os
import warnings
import pandas as pd
import numpy as np


warnings.filterwarnings("ignore")

# Define o número de processos a serem usados pelo multiprocessing
num_processes = os.cpu_count()

# Define o período dos 3 prazos do cálculo do UO
period_1 = 4 #period curt
period_2 = 8 #period intemediario
period_3 = 20 #period long

# Função para calcular o Ultimate Oscillator (UO)
def calculate_uo(df):
    df['min_low_or_pc'] = df[['low', 'close']].min(axis=1)
    buying_pressure = df['close'] - df['min_low_or_pc']
    true_range_1 = df['high'] - df['low']
    true_range_2 = df['high'].shift(1) - df['close'].shift(1)
    true_range_3 = df['low'].shift(1) - df['close'].shift(1)

    avg_1 = buying_pressure.rolling(window=period_1).sum() / true_range_1.rolling(window=period_1).sum()
    avg_2 = buying_pressure.rolling(window=period_2).sum() / true_range_2.rolling(window=period_2).sum()
    avg_3 = buying_pressure.rolling(window=period_3).sum() / true_range_3.rolling(window=period_3).sum()

    uo = 100 * ((4 * avg_1) + (2 * avg_2) + avg_3) / 7
    df.drop(columns=['min_low_or_pc'], inplace=True)
    return uo


# Função para calcular a eficiência do UO
def calculate_efficiency(df):
    df['uo'] = calculate_uo(df)
    df['prev_uo'] = df['uo'].shift(1)

    df.loc[df['uo'] > df['prev_uo'], 'signal'] = 1
    df.loc[df['uo'] <= df['prev_uo'], 'signal'] = 0

    total_signals = len(df['signal'])
    total_correct_signals = sum(1 for s in df['signal'] if s > 0)

    efficiency_gain = 0 if total_signals == 0 else total_correct_signals / total_signals

    return round(efficiency_gain, 2)


# Função para processar um arquivo CSV e retornar as informações de ganho de eficiência
def process_csv_file(file_path):
    df = pd.read_csv(file_path)
    efficiency_gain = calculate_efficiency(df)
    return round(efficiency_gain, 2)


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
    # Seleciona a data final
    days = '2021-08-17'

    # Quantos dias você quer analisar  
    num_days = 300

    # Lista todos os arquivos CSV na pasta "data"
    csv_files = [os.path.join('data', f) for f in os.listdir('data') if f.endswith('.csv')]

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
        efficiency_gain = calculate_efficiency(df)

        # Adiciona a eficiência deste arquivo à lista de eficiências
        efficiency_gains.append((file_path, efficiency_gain))

    # Manipula os resultados
    df_tmp = pd.DataFrame({ 'Ativo' : csv_files,
                            'OSC' : "UO", 
                            'EFFICIENCY_GAINS': efficiency_gains})
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