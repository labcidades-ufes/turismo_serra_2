#!/usr/bin/env Rscript

# ========================================================================
# Script: coleta_bronze.R 
# Objetivo: Ler CSVs locais e salvar como Parquet na camada Bronze do MinIO
# ========================================================================

library(httr)
library(jsonlite)
library(nanoparquet) 

# 1. Carregamento Inteligente do Utils (Resolve o erro do Docker)
if (file.exists("utils.R")) {
  source("utils.R")
} else if (file.exists("../utils/utils.R")) {
  source("../utils/utils.R")
} else {
  message("[COLETA] Aviso: utils.R não encontrado. Verifique a estrutura de pastas.")
}

collect_data <- function() {
  tryCatch({
    cat("[COLETA] Iniciando busca de arquivos brutos...\n")
    
    # Define origem (No Docker, os dados brutos devem estar mapeados em /data)
    caminho_origem <- Sys.getenv(
      "TURISMO_BRUTOS",
      unset = "/data" # Padrão para quando rodar dentro do container
    )
    
    # Mapear arquivos CSV
    arquivos <- list.files(
      path = caminho_origem,
      pattern = "\\.csv$",
      full.names = TRUE,
      recursive = TRUE,
      ignore.case = TRUE
    )
    
    if (length(arquivos) == 0) {
      stop("Nenhum arquivo .csv encontrado na origem: ", caminho_origem)
    }
    
    cat("[COLETA] Lendo", length(arquivos), "arquivos e consolidando...\n")
    
    # Lê todos os CSVs e empilha em um único dataframe
    # Ajuste os parâmetros de read.csv (sep, dec) conforme seus arquivos
    lista_dados <- lapply(arquivos, function(f) {
      d <- read.csv(f, stringsAsFactors = FALSE, sep = ";", dec = ",")
      d$origem_arquivo <- basename(f) # Adiciona rastro do dado
      return(d)
    })
    
    dados_consolidados <- do.call(rbind, lista_dados)
    
    return(dados_consolidados)
    
  }, error = function(e) {
    cat("[COLETA] Erro na extração:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

save_to_minio_duckdb <- function(data) {
  cat("[COLETA] Convertendo para Parquet e enviando ao MinIO (Bronze)...\n")
  tryCatch({
    # Nome padrão: bronze/dominio/tabela_data.parquet
    timestamp <- format(Sys.time(), "%Y%m%d")
    filepath <- sprintf("bronze/turismo_serra/turismo_serra_2_%s.parquet", timestamp)
    
    # Função do seu utils.R que faz o upload para o MinIO
    write_parquet_to_minio(data, filepath)
    
    return(filepath)
  }, error = function(e) {
    cat("[COLETA] Erro ao salvar no MinIO:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# --- Execução Principal ---
message("--- INICIANDO PIPELINE TURISMO SERRA (BRONZE) ---")
dados   <- collect_data()
arquivo <- save_to_minio_duckdb(dados)
cat("[COLETA] Finalizado com sucesso! Arquivo salvo em:", arquivo, "\n")