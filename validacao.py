import pandas as pd
import great_expectations as ge

class Validator:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def has_data(self):
        return not self.df.empty

    def has_columns(self, required_columns):
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            print(f"Colunas ausentes: {missing_columns}") 
            return False
        return True
    
    def check_null_or_empty(self, columns=None):
        if columns is None:
            columns = self.df.columns  

        null_columns = [col for col in columns if self.df[col].isnull().any()]
        empty_columns = [col for col in columns if self.df[col].astype(str).str.strip().eq("").any()]

        if null_columns or empty_columns:
            return {
                "colunas_com_null": null_columns,
                "colunas_com_branco": empty_columns
            }
        return True  # Nenhuma coluna tem valores nulos ou vazios