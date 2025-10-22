# --- ESTÁGIO 1: A Base ---
# Começamos com uma imagem oficial do Linux que já tem o Python 3.14 (versão 'slim' é mais leve)
FROM python:3.14-slim

# --- ESTÁGIO 2: Configuração do Ambiente ---
# Define o diretório de trabalho principal dentro do contêiner
WORKDIR /app

# --- ESTÁGIO 3: Instalação das Dependências ---
# Copia APENAS o arquivo de requisitos primeiro
# (Isso é um truque de cache do Docker: se não mudarmos os requisitos, ele não reinstala tudo)
COPY requirements.txt .

# Roda o comando 'pip install' DENTRO do contêiner para instalar as bibliotecas
RUN pip install --no-cache-dir -r requirements.txt

# --- ESTÁGIO 4: Copiar o Código-Fonte ---
# Agora, copia todo o resto do nosso projeto (o .py, a pasta 'data', etc.) para dentro do /app
COPY . .

# --- ESTÁGIO 5: Comando de Execução ---
# Define o comando que será executado quando o contêiner "ligar"
# Ele vai rodar o nosso pipeline de ETL
CMD ["python", "pipeline_etl.py"]