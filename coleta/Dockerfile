# 1. Usa a base que você acabou de buildar com sucesso
FROM utils-r_duckdb:latest

# 2. Instala os pacotes R necessários para o Google Drive
# O dplyr e httr ajudam na manipulação e requisições
RUN R -e "install.packages(c('googledrive', 'dplyr', 'httr'), repos='https://cloud.r-project.org/')"

# 3. Define onde o trabalho acontece
WORKDIR /app

# 4. Copia os arquivos necessários
# Atenção: O build deve ser rodado da RAIZ do projeto
COPY dags/utils/utils.R .
COPY dags/turismo_serra_2/coleta/coleta.R .

# 5. Execução
CMD ["Rscript", "coleta.R"]
