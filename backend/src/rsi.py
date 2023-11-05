"""__RSI__

Aqui estão algumas explicações mais detalhadas sobre algumas partes específicas do código:
A função calculate_rsi calcula o índice de força relativa (RSI) de um DataFrame de preços. O RSI é um indicador técnico utilizado na análise técnica de ações que compara a magnitude dos movimentos ascendentes e descendentes dos preços recentes. Ele varia de 0 a 100 e é comumente usado como um sinal de sobrecompra ou sobrevenda.
A função calculate_moving_average_crossover recebe um DataFrame contendo informações de preços e calcula o índice de eficiência de uma estratégia de cruzamento de médias móveis. Essa estratégia envolve o cálculo de duas médias móveis (uma curta e outra longa) do RSI e então observando quando a média móvel curta cruza acima ou abaixo da média móvel longa, 
o que pode indicar um sinal de compra ou venda, respectivamente. O indicador de eficiência retornado é a proporção de sinais corretos (ou seja, sinais que geraram lucro) em relação ao número total de sinais observados.
A função process_csv_file recebe o caminho de um arquivo CSV que contém dados históricos de preços para um ativo e retorna o índice de eficiência de uma estratégia de sinalização. O DataFrame do arquivo é primeiro lido e filtrado por datas específicas, 
em seguida a função calculate_moving_average_crossover é chamada para calcular a eficiência da estratégia.
A função clean_ativo e clean_efficiency_gains são funções auxiliares que convertem os nomes de arquivos em nomes legíveis e convertem a tupla retornada pela função process_csv_file em um tipo mais legível para visualização.

Returns:
    autor : Data Science Hedris 
"""
import os
import warnings
import pandas as pd
import numpy as np
import datetime
import time 
from multiprocessing import Pool

warnings.filterwarnings("ignore")

#time 
start_time = time.perf_counter()

# Define o número de processos a serem usados pelo multiprocessing
num_processes = os.cpu_count()

# Função para calcular o RSI de um DataFrame de preços
def calculate_rsi(prices, rsi_periods):
    deltas = np.diff(prices)
    seed = deltas[:rsi_periods+1]
    up = seed[seed >= 0].sum()//rsi_periods # converter resultado para inteiros 
    down = -seed[seed < 0].sum()//rsi_periods # converter resultado para inteiros
    rs = up/down
    rsi = np.zeros_like(prices)
    rsi[:rsi_periods] = 100. - 100./(1.+rs)

    for i in range(rsi_periods, len(prices)):
        delta = deltas[i-1]

        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

        up = (up*(rsi_periods-1) + upval)/rsi_periods
        down = (down*(rsi_periods-1) + downval)/rsi_periods

        rs = up/down
        rsi[i] = 100. - 100./(1.+rs)

    return rsi


def calculate_moving_average_crossover(df, ma_short, ma_long, rsi_periods):
    # Calcula o RSI
    df['rsi'] = calculate_rsi(df['close'], rsi_periods)

    # Calcula as médias móveis do RSI
    col_short = 'ma_{}'.format(ma_short)
    col_long = 'ma_{}'.format(ma_long)
    df[col_short] = df['rsi'].rolling(ma_short).mean()
    df[col_long] = df['rsi'].rolling(ma_long).mean()

    # Identifica os cruzamentos de médias móveis
    df.loc[df[col_short] > df[col_long], 'cross'] = 1
    df.loc[df[col_short] <= df[col_long], 'cross'] = 0
    df['signal'] = df['cross'].diff()

    total_signals = len(df['signal'])
    total_correct_signals = sum(1 for s in df['signal'] if s > 0)

    efficiency_gain = 0 if total_signals == 0 else total_correct_signals / total_signals

    return efficiency_gain



# Função para processar um arquivo CSV e retornar as informações de ganho de eficiência
def process_csv_file(file_path, ma_short, ma_long, rsi_periods):
    df = pd.read_csv(file_path)
    efficiency_gain = calculate_moving_average_crossover(df, ma_short, ma_long, rsi_periods)
    return round(efficiency_gain, 2)


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
        define o short e long , rsi_period ,
    """
    ma_short = 15
    ma_long = 20
    rsi_periods = 14
        
    #Seleiona a data final
    days = '2018-2-10'
    
    #Quantos dias você quer analisar  
    num_days = 600
        

    # Lista todos os arquivos CSV na pasta "data"
    csv_files = [os.path.join('data', f) for f in os.listdir('data') if f.endswith('.csv')]

    with Pool(num_processes) as p:
        # Cria uma lista vazia para armazenar os resultados da eficiência
        efficiency_gains = []

        for file_path in csv_files:
            # Lê o arquivo CSV em um DataFrame
            df = pd.read_csv(file_path, usecols=['time', 'close'], parse_dates=['time'])
            # Filtra as datas que estão dentro do período especificado
            df['time'] = pd.to_datetime(df['time'])
            start_date = pd.Timestamp(days)
            df = df[(df['time'] >= start_date - pd.Timedelta(days=num_days))]
            # Calcula a eficiência para este arquivo específico
            efficiency_gain = calculate_moving_average_crossover(df, ma_short, ma_long, rsi_periods)

            # Adiciona a eficiência deste arquivo à lista de eficiências
            efficiency_gains.append((file_path, efficiency_gain))

        # Manipula os resultados
        df_tmp = pd.DataFrame({ 
                                'Ativo' : csv_files,
                                'OSC' : "RSI", 
                                'EFFICIENCY_GAINS': efficiency_gains,
                                'MA_SHORT' : ma_short,
                                'MA_LONG': ma_long,
                                'RSI_PERIODO': rsi_periods,})
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
        
        if not os.path.exists('resultado'):
            os.makedirs('resultado')
            df_tmp.to_excel('resultado/resultado_rsi_todosAtivos.xlsx')
        
        #finally
        elapesed = time.perf_counter() - start_time
        
        print(f'Análise feita com sucesso tempo de execução: {round(elapesed)}') 
         
        