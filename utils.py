import io
import boto3 
import os
import pandas as pd
from datetime import datetime

data_atual = datetime.now().strftime("%Y-%m-%d")


### ESSA FUNÇÃO MANDA OS ARQUIVOS PARA O NOSSO LAKE
def upload_to_s3(file_path, bucket_name, camada):
    s3 = boto3.client("s3")
    
    file_name = os.path.basename(file_path)

    s3_path = f"{camada}/{data_atual}/{file_name}"
    try:
        s3.upload_file(file_path, bucket_name, s3_path)
        print(f"✅ Upload bem-sucedido: {file_path} → s3://{bucket_name}/{s3_path}")
    except Exception as e:
        print(f"❌ Erro ao subir o arquivo: {e}")


### ESSA FUNÇÃO BUSCA OS ARQUIVOS DA CAMADA BRONZE E MANDA PARA AS DEMAIS CAMADAS
def read_file(bucket_name, file_name, camada):
    s3 = boto3.client("s3")
    
    s3_key = f"{camada}/{file_name}"
    
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response["Body"].read()
        
        ext = os.path.splitext(file_name)[-1].lower()

        if ext == ".csv":
            df = pd.read_csv(io.BytesIO(file_content), encoding="utf-8")
        elif ext == ".json":
            df = pd.read_json(io.BytesIO(file_content), encoding="utf-8")
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(io.BytesIO(file_content))
        else:
            print(f"❌ Formato não suportado: {ext}")
            return None
        
        print(f"✅ Arquivo '{file_name}' lido com sucesso!")
        return df

    except Exception as e:
        print(f"❌ Erro ao ler o arquivo {file_name}: {e}")
        return None

