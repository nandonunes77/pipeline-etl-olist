import pandas as pd
from sqlalchemy import create_engine
import os # Vamos usar a biblioteca 'os' para lidar com caminhos de arquivos

class PipelineOlistETL:
    """
    Esta é a nossa classe de pipeline de ETL. Ela é responsável
    por extrair, transformar e carregar os dados da Olist.
    """

    def __init__(self, db_path, data_folder_path):
        """
        Este é o "construtor". Ele é executado quando criamos o pipeline
        e define as configurações iniciais.
        """
        self.db_path = db_path
        self.data_folder_path = data_folder_path
        self.db_engine = create_engine(f'sqlite:///{self.db_path}')
        print(f"Pipeline iniciado. Banco de dados será salvo em: {db_path}")

    def extract(self):
        """
        (E)xtract: Carrega todos os arquivos CSV da pasta de dados.
        """
        print("Iniciando (E)xtract...")
        # Lista dos arquivos que queremos carregar
        csv_files = [
            'olist_customers_dataset.csv',
            'olist_orders_dataset.csv',
            'olist_order_items_dataset.csv'
        ]
        
        # Um dicionário para guardar nossos DataFrames
        dataframes = {}
        
        for file in csv_files:
            file_path = os.path.join(self.data_folder_path, file)
            # Remove o '_dataset.csv' para criar um nome de chave limpo
            key_name = file.replace('olist_', '').replace('_dataset.csv', '')
            dataframes[key_name] = pd.read_csv(file_path)
            
        print("Extração concluída.")
        return dataframes

    def transform(self, dataframes):
        """
        (T)ransform: Limpa, enriquece e junta os dados.
        """
        print("Iniciando (T)ransform...")
        
        # Pegando os dataframes pelo nome que demos no extract
        df_clientes = dataframes['customers']
        df_pedidos = dataframes['orders']
        
        # --- Lógica de Transformação que já fizemos ---
        
        # 1. Juntar (Merge) clientes e pedidos
        df_completo = pd.merge(
            df_clientes,
            df_pedidos,
            on='customer_id'
        )
        
        # 2. Converter colunas de data (o 'timestamp' de compra)
        df_completo['order_purchase_timestamp'] = pd.to_datetime(df_completo['order_purchase_timestamp'])
        
        # 3. Enriquecer com colunas de data (Engenharia de Features)
        df_completo['mes_compra'] = df_completo['order_purchase_timestamp'].dt.month
        df_completo['dia_semana_compra'] = df_completo['order_purchase_timestamp'].dt.day_name()
        df_completo['hora_compra'] = df_completo['order_purchase_timestamp'].dt.hour
        
        print("Transformação concluída.")
        # Retornamos apenas o DataFrame final e pronto
        return df_completo

    def load(self, df_final):
        """
        (L)oad: Carrega o DataFrame final no banco de dados SQLite.
        """
        print("Iniciando (L)oad...")
        
        df_final.to_sql(
            name='pedidos_enriquecidos',  # Nome da tabela de destino
            con=self.db_engine,
            if_exists='replace',
            index=False
        )
        print("Carga no banco de dados concluída.")

    def run(self):
        """
        Este é o "Orquestrador". Ele executa os passos na ordem correta.
        """
        print("--- PIPELINE DE ETL INICIADO ---")
        
        # 1. Extrai
        dados_extraidos = self.extract()
        
        # 2. Transforma
        dados_transformados = self.transform(dados_extraidos)
        
        # 3. Carrega
        self.load(dados_transformados)
        
        print("--- PIPELINE DE ETL CONCLUÍDO ---")

# -----------------------------------------------------------------
# A MÁGICA DA ENGENHARIA DE SOFTWARE ACONTECE AQUI
# -----------------------------------------------------------------
if __name__ == "__main__":
    """
    Esta parte do código só roda se você executar este arquivo .py
    DIRETAMENTE (ex: 'python pipeline_etl.py').
    
    Se outro arquivo 'importar' esta classe, este bloco não será executado.
    Isso nos permite ter um arquivo que é ao mesmo tempo
    um "motor" (a Classe) e uma "ignição" (este bloco).
    """
    
    # --- Configurações do Pipeline ---
    # (Assumindo que os CSVs estão em uma pasta 'data' no mesmo nível)
    # (Vamos criar essa pasta)
    
    # O '.' significa 'esta pasta atual'
    pasta_atual = '.' 
    pasta_de_dados = os.path.join(pasta_atual, 'data')
    nome_do_banco = 'olist_etl.db'
    
    # 1. Criar o pipeline
    pipeline = PipelineOlistETL(db_path=nome_do_banco, data_folder_path=pasta_de_dados)
    
    # 2. Rodar o pipeline
    pipeline.run()