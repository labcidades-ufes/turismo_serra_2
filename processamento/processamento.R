#!/usr/bin/env Rscript

# -------------------------------------------------------------------------
# Script: processamento.R (Gold)
# Objetivo: Formação de indicadores econômicos (Índice e Sazonalidade)
# -------------------------------------------------------------------------

source("utils.R")
# Garantindo que os pacotes necessários estejam carregados no ambiente Docker
if (!require("pacman")) install.packages("pacman")
pacman::p_load(dplyr, lubridate, tidyr, arrow)

message("--- INICIANDO ETAPA: PROCESSAMENTO (GOLD) ---")

# 1. Carregar Dados da Silver (MinIO)
read_from_silver <- function() {
  cat("[GOLD] Lendo dados da camada Silver no MinIO...\n")
  # Busca os arquivos na pasta silver do seu domínio
  arquivos <- list_parquet_files_in_minio("silver/turismo_serra/")
  
  if (length(arquivos) == 0) {
    stop("ERRO: Nenhum arquivo silver disponível. Verifique se a fase anterior rodou.")
  }
  
  # Seleciona o arquivo mais recente
  caminho <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "", sort(arquivos, TRUE)[1])
  read_parquet_from_minio(caminho)
}

# 2. Inteligência de Dados: Cálculo de Indicadores 
gerar_indicadores <- function(dados_silver) {
  message("Calculando indicadores mensais (Índice e Sazonalidade)...")
  
  # Vetores para tradução de meses 
  meses_pt_abrev <- c("Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez")
  meses_pt_completo <- c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")
  
  indicadores_mensais <- dados_silver %>%
    group_by(Mes) %>%
    summarise(Faturamento_Mensal = sum(Valor_Servico, na.rm = TRUE), .groups = 'drop') %>%
    arrange(Mes) %>%
    mutate(
      # Referência Janeiro = 1.0
      Indice_Atividade = Faturamento_Mensal / first(Faturamento_Mensal),
      # Variação sobre a média anual
      Variacao_Sazonal = (Faturamento_Mensal / mean(Faturamento_Mensal)) - 1,
      # Nomes em Português com ordenação por níveis (factors)
      Mes_Abrev = factor(meses_pt_abrev[Mes], levels = meses_pt_abrev),
      Mes_Nome = factor(meses_pt_completo[Mes], levels = meses_pt_completo)
    )
  
  message("Consolidando base final...")
  dados_gold <- dados_silver %>%
    left_join(indicadores_mensais, by = "Mes")
  
  return(dados_gold)
}

# 3. Gravação na Camada Gold (MinIO)
salvar_gold <- function(data) {
  # Nome fixo 
  caminho_saida <- "gold/turismo_serra/turismo_final.parquet"
  message("Salvando base GOLD no MinIO: ", caminho_saida)
  
  write_parquet_to_minio(data, caminho_saida)
}

# --- Execução Principal do Fluxo ---
tryCatch({
  df_silver <- read_from_silver()
  df_gold   <- gerar_indicadores(df_silver)
  salvar_gold(df_gold)
  message("--- SUCESSO! Base GOLD disponível ---")
}, error = function(e) {
  cat("[GOLD] Erro Crítico:", conditionMessage(e), "\n")
  quit(status = 1)
})