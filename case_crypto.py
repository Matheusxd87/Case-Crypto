import os  # Para acessar variáveis de ambiente
import requests  # Para fazer requisições HTTP à API
import pandas as pd  # Para manipulação de dados em formato tabular
from sqlalchemy import create_engine  # Para conectar ao banco PostgreSQL
from dotenv import load_dotenv  # Para carregar variáveis de ambiente de um arquivo .env

# Carregar variáveis do .env, aqui não precisei colocar o caminho do arquivo .env pois estava no diretório do código
load_dotenv()

# Lê as variáveis do ambiente para conexão com o banco
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# Cria uma engine de conexão com o banco PostgreSQL utilizando SQLAlchemy + psycopg2
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Função para buscar dados da API CoinGecko
def fetch_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd", #escolho a moeda 
        "order": "market_cap_desc", #padrãod e ordenação para trazer os dados
        "per_page": 10, #dez primeiras linhas
        "page": 1,
        "sparkline": False #esta opção serve para trazer dados históricos recentes de cada moeda, interessante para fazer gráficos, mas optei por não utilizar
    }

    response = requests.get(url, params=params)
    if response.status_code == 200: #200 é um numero de requisição bem sucedida
        return pd.DataFrame(response.json())
    else:
        raise Exception(f"Erro na requisição: {response.status_code}")

# Função para salvar no banco
def save_to_db(df):
    # Tabela principal de moedas
    df_main = df[['id', 'symbol', 'name', 'image']] #colunas que desejo nesta tabela
    df_main.to_sql('moedas', engine, if_exists='replace', index=False)

    # Tabela secundária de preço e volume
    df_price_volume = df[['id', 'current_price', 'ath', 'atl', 'market_cap',
                          'price_change_percentage_24h', 'total_volume', 'high_24h',
                          'low_24h', 'last_updated']]  
    df_price_volume.to_sql('preco_volume', engine, if_exists='replace', index=False)
    
    # Tabela unificada
    df_joined = pd.merge(df_main, df_price_volume, on='id', how='inner')
    df_joined.to_sql('crypto_dados', engine, if_exists='replace', index=False)

# Executar o processo
def main():
    print("Buscando dados da API")
    df = fetch_crypto_data()
    save_to_db(df)
    print("Concluído com sucesso!")
