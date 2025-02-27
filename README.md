# 🏦 Ações Amigas - Extração de Ativos Financeiros

## 📌 Descrição do Projeto

Este projeto tem como objetivo a extração de dados de ativos financeiros do Yahoo Finance utilizando a biblioteca `yfinance`. Os dados são armazenados em um Data Lake na AWS S3, seguindo a arquitetura de medallion (*Bronze, Silver*) para garantir qualidade, governança e acessibilidade das informações.

Minha responsabilidade no projeto foi entregar os dados tratados até a camada Silver, garantindo que os analistas da empresa pudessem acessá-los e construir as visões necessárias para a camada *Gold*.
## 🛠️ Arquitetura do Pipeline

O pipeline segue o modelo **ETL (Extract, Transform, Load)** estruturado em três camadas:

### 🔹 Bronze (Raw Layer)
- Extração direta do Yahoo Finance utilizando a biblioteca `yfinance`.
- Armazenamento dos dados brutos no formato de origem no S3.

### 🔸 Silver (Cleansed Layer)
- Tratamento dos dados brutos (*limpeza, conversões e padronização*).
- Enriquecimento dos dados com cálculos de métricas financeiras.
- Armazenamento no S3 em um formato otimizado.

## 🔧 Tecnologias Utilizadas

- **Linguagem:** Python 🐍  
- **Bibliotecas:** `yfinance`, `pandas`, `pyarrow`, `boto3`  
- **Armazenamento:** AWS S3 (*Data Lake*)  
- **Transformação:** `pandas`
