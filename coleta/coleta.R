# ========================================================================
# Script: coleta.R 
# Objetivo: Coletar CSVs e Padronizar Encoding (Bronze)
# ========================================================================

library(googledrive)
library(dplyr)
library(httr)

drive_deauth()
set_config(add_headers(`User-Agent` = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"))

if (file.exists("utils.R")) source("utils.R")

collect_from_public_drive <- function(folder_id) {
  tryCatch({
    cat("[COLETA] Listando arquivos...\n")
    arquivos <- drive_ls(as_id(folder_id), pattern = "\\.csv$")
    
    lista_dados <- lapply(1:nrow(arquivos), function(i) {
      item <- arquivos[i, ]
      temp_csv <- tempfile(fileext = ".csv")
      
      cat(sprintf("  -> [%d/52] %s\n", i, item$name))
      url <- sprintf("https://docs.google.com/uc?export=download&id=%s", item$id)
      GET(url, write_disk(temp_csv, overwrite = TRUE))
      
      # Lemos tudo como texto bruto
      d <- read.csv(temp_csv, stringsAsFactors = FALSE, sep = ";", dec = ",", 
                    colClasses = "character", check.names = FALSE)
      
      # LIMPEZA HÍBRIDA: Só converte se detectar erro de encoding
      d[] <- lapply(d, function(x) {
        if (any(grepl("Ã|Â|Â§", x))) {
          # Se detectar lixo de encoding, tenta consertar
          y <- iconv(x, from = "UTF-8", to = "UTF-8", sub = "")
          if(any(is.na(y))) y <- iconv(x, from = "Windows-1252", to = "UTF-8", sub = "")
          return(y)
        }
        # Se estiver ok, apenas garante que o R trate como UTF-8
        return(iconv(x, to = "UTF-8", sub = ""))
      })
      
      d$origem_arquivo <- item$name
      unlink(temp_csv)
      return(d)
    })
    
    return(bind_rows(lista_dados))
    
  }, error = function(e) {
    cat("[COLETA] Erro crítico:", conditionMessage(e), "\n")
    quit(status = 1)
  })
}

# --- Execução ---
ID_PASTA <- "1IB6MeE8q9UlgGaABJ1sSBh4CRqQTY89p"
dados_brutos <- collect_from_public_drive(ID_PASTA)

timestamp <- format(Sys.time(), "%Y%m%d")
filepath  <- sprintf("bronze/turismo_serra/turismo_serra_2_%s.parquet", timestamp)

write_parquet_to_minio(dados_brutos, filepath)
cat("[COLETA] Sucesso! Base Bronze consolidada e limpa.\n")
