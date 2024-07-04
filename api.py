from requests_oauth2client import *
import json
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from datetime import datetime

# Carregar o JSON de data[0]
data = []
resp = requests.get('https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json').json()
data.append(resp)

# Agora você precisa acessar o primeiro elemento da lista, que é o JSON baixado
output = []
for region, values in data[0]["spot_advisor"].items():
    for os, instances in values.items():
        for instance, stats in instances.items():
            s = stats["s"]
            r = stats["r"]
            emr = data[0]["instance_types"].get(instance, {}).get("emr", False)
            cores = data[0]["instance_types"].get(instance, {}).get("cores", "")
            ram_gb = data[0]["instance_types"].get(instance, {}).get("ram_gb", "")
            output.append([region, os, instance, s, r, emr, cores, ram_gb])

# Criar DataFrame
df = pd.DataFrame(output, columns=["spotadvisor", "so", "instance_types", "s", "r", "emr", "cores", "ram_gb"])

# Adicionar coluna de hora de atualização
df['hora_atualizacao'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Extrair dados do JSON com base no índice 'r' e adicionar ao DataFrame
ranges = [
    {"index": 0, "label": "<5%", "dots": 0, "max": 5},
    {"index": 1, "label": "5-10%", "dots": 1, "max": 11},
    {"index": 2, "label": "10-15%", "dots": 2, "max": 16},
    {"index": 3, "label": "15-20%", "dots": 3, "max": 22},
    {"index": 4, "label": ">20%", "dots": 4, "max": 100}
]

df['index'] = df['r'].apply(lambda x: ranges[x]["index"])
df['label'] = df['r'].apply(lambda x: ranges[x]["label"])
df['dots'] = df['r'].apply(lambda x: ranges[x]["dots"])
df['max'] = df['r'].apply(lambda x: ranges[x]["max"])

# Salvar em CSV
df.to_csv(r'D:/pythonDSA/inter/requests.csv', encoding='utf-8-sig', index=False, sep=';')

# autenticação sql

'''conexao = (
    # Driver que será utilizado na conexão
    'DRIVER={ODBC Driver 17 for SQL Server};'
    # Host/IP ou nome do servidor
    'SERVER=11.11.11.11;'
    # Porta
    'PORT=1433;'
    # Banco que será utilizado
     'DATABASE=DBDW;'
    # Nome de usuário
     'UID=etl.bi;'
    # Senha
     'PWD='
 )'''

# engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(conexao))
# df.to_sql('tbl_clientes_uso_app', con=engine, schema='dbo', if_exists='append', index=False, chunksize=1000)

# Exibir DataFrame
print(df)

