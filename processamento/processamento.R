
# -------------------------------------------------------------------------
# Script: processamento.R (Gold)
# Objetivo: Formação de indicadores usando colunas reais da Silver
# -------------------------------------------------------------------------

source("utils.R")

if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, lubridate, tidyr, arrow)

message("--- INICIANDO ETAPA: PROCESSAMENTO (GOLD) ---")

# 1. Carregar Dados da Silver
read_from_silver <- function() {
  cat("[GOLD] Lendo dados da camada Silver no MinIO...\n")
  arquivos <- list_parquet_files_in_minio("silver/turismo_serra/")
  
  if (length(arquivos) == 0) {
    stop("ERRO: Nenhum arquivo silver disponível.")
  }
  
  # Seleciona o arquivo mais recente
  caminho <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "", sort(arquivos, TRUE)[1])
  read_parquet_from_minio(caminho)
}

# 2. Inteligência de Dados: Cálculo de Indicadores 
gerar_indicadores <- function(dados_silver) {
  message("Calculando indicadores mensais usando vlrservicos, mesreferencia e anoreferencia...")
  
  # Criamos uma coluna de data real para garantir a ordenação cronológica correta
  dados_silver <- dados_silver %>%
    mutate(
      Mes = as.integer(mesreferencia),
      Ano = as.integer(anoreferencia),
      Data_Referencia = as.Date(sprintf("%d-%02d-01", Ano, Mes))
    )
  
  # Tabelas auxiliares para nomes dos meses
  meses_pt_abrev <- c("Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez")
  meses_pt_completo <- c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")
  
  # Agrupamento e cálculos de Gold
  indicadores_mensais <- dados_silver %>%
    group_by(Ano, Mes) %>%
    summarise(
      Faturamento_Mensal = sum(vlrservicos, na.rm = TRUE), 
      Total_ISS = sum(iss, na.rm = TRUE),
      Qtd_Operacoes = n(),
      .groups = 'drop'
    ) %>%
    arrange(Ano, Mes) %>%
    mutate(
      # Índice de Atividade: Evolução em relação ao primeiro mês da série
      Indice_Atividade = Faturamento_Mensal / first(Faturamento_Mensal[Faturamento_Mensal > 0]),
      
      # Sazonalidade: O quanto este mês está acima ou abaixo da média histórica
      Variacao_Sazonal = (Faturamento_Mensal / mean(Faturamento_Mensal)) - 1,
      
      # Labels para visualização
      Mes_Abrev = factor(meses_pt_abrev[Mes], levels = meses_pt_abrev),
      Mes_Nome = factor(meses_pt_completo[Mes], levels = meses_pt_completo)
    )
  
  message("Consolidando base final (Gold)...")
  # Unimos os indicadores de volta aos dados detalhados
  dados_gold <- dados_silver %>%
    left_join(indicadores_mensais, by = c("Ano", "Mes"))
  
  return(dados_gold)
}

# 3. Gravação na Camada Gold
salvar_gold <- function(data) {
  caminho_saida <- "gold/turismo_serra/turismo_final.parquet"
  message("Salvando base GOLD no MinIO: ", caminho_saida)
  write_parquet_to_minio(data, caminho_saida)
}

# --- Execução ---
tryCatch({
  df_silver <- read_from_silver()
  df_gold   <- gerar_indicadores(df_silver)
  salvar_gold(df_gold)
  message("--- SUCESSO! Base GOLD gerada com os nomes corretos da Silver ---")
}, error = function(e) {
  cat("[GOLD] Erro Crítico:", conditionMessage(e), "\n")
  quit(status = 1)
})
