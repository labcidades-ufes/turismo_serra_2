#!/usr/bin/env Rscript

# ========================================================================
# Script: pre_processamento.R
# Objetivo: Limpeza, Padronização (Bairros/Cidades) e Camada Silver
# ========================================================================

library(dplyr)
library(readr)
library(purrr)
library(stringr)
library(lubridate)
library(stringi)
library(tidyr)
source("utils.R")

# 1. Dicionários 
bairros_oficiais <- c(
  "Alterosas", "André Carloni", "Areia Branca", "Bairro Continental", "Bairro das Laranjeiras",
  "Bairro de Fátima", "Bairro Novo", "Balneário de Carapebus", "Barcelona", "Barro Branco",
  "Belvedere", "Benetti", "Bicanga", "Boa Vista I", "Boa Vista II", "Caçaroca", "Camará",
  "Campinho da Serra I", "Campinho da Serra II", "Cantinho do Céu", "Cascata", "Castelândia",
  "Castro Alves (Carapina I)", "Central Carapina", "Chácara Parreiral", "Cidade Continental",
  "Cidade Nova da Serra", "Cidade Pomar", "Colina da Serra", "Colina de Laranjeiras", "Costa Bela",
  "Costa Dourada", "Diamantina", "Divinópolis", "Eldorado", "Enseada de Jacaraípe",
  "Estância Monazítica", "Eurico Sales", "Feu Rosa", "Hélio Ferraz", "Jardim Atlântico",
  "Jardim Bela Vista", "Jardim Carapina", "Jardim da Serra", "Jardim Guanabara", "Jardim Limoeiro",
  "Jardim Tropical", "José de Anchieta", "José de Anchieta II", "Lagoa de Jacaraípe",
  "Laranjeiras Velha", "Loteamento Verdes Mares", "Magistrado", "Manguinhos", "Manoel Plaza",
  "Marbella", "Maria Níobe", "Mata da Serra", "Miramar", "Morada de Laranjeiras",
  "Nossa Senhora do Rosário de Fátima", "Nova Almeida Centro", "Nova Carapina I", "Nova Carapina II",
  "Novo Horizonte", "Novo Porto Canoa", "Palmeiras", "Parque Beira Rio", "Parque das Gaivotas",
  "Parque das Orquídeas", "Parque Jacaraípe", "Parque Reis Magos", "Parque Residencial Laranjeiras",
  "Parque Residencial Nova Almeida", "Parque Santa Fé", "Planalto de Carapina", "Planalto Serrano",
  "Planície da Serra", "Poço dos Padres", "Portal de Jacaraípe", "Porto Canoa", "Porto Dourado",
  "Potiguara", "Praia de Capuba", "Praia de Carapebus", "Praiamar", "Reis Magos",
  "Residencial Centro da Serra", "Residencial Jacaraípe", "Residencial Monte Verde",
  "Residencial Vista do Mestre", "Santa Luzia", "Santa Rita de Cássia", "Santo Antônio",
  "São Diogo I", "São Diogo II", "São Domingos", "São Francisco", "São Geraldo", "São João Batista",
  "São Judas Tadeu", "São Lourenço", "São Marcos I", "São Marcos II", "São Patrício",
  "Serra Centro (Sede)", "Serra Dourada I", "Serra Dourada II", "Serra Dourada III", "Serramar",
  "Taquara I", "Taquara II", "Tubarão", "Vila Luciano", "Vila Nova de Colares", "Vista da Serra I",
  "Vista da Serra II", "Civit I", "Civit II", "TIMS"
)

cidades_es <- c(
  "Afonso Cláudio", "Água Doce do Norte", "Águia Branca", "Alegre", "Alfredo Chaves",
  "Alto Rio Novo", "Anchieta", "Apiacá", "Aracruz", "Atílio Vivácqua", "Baixo Guandu",
  "Barra de São Francisco", "Boa Esperança", "Bom Jesus do Norte", "Brejetuba",
  "Cachoeiro de Itapemirim", "Cariacica", "Castelo", "Colatina", "Conceição da Barra",
  "Conceição do Castelo", "Divino de São Lourenço", "Domingos Martins", "Dores do Rio Preto",
  "Ecoporanga", "Fundão", "Governador Lindenberg", "Guaçuí", "Guarapari", "Ibatiba",
  "Ibiraçu", "Ibitirama", "Iconha", "Irupi", "Itaguaçu", "Itapemirim", "Itarana", "Iúna",
  "Jaguaré", "Jerônimo Monteiro", "João Neiva", "Laranja da Terra", "Linhares",
  "Mantenópolis", "Marataízes", "Marechal Floriano", "Marilândia", "Mimoso do Sul",
  "Montanha", "Mucurici", "Muniz Freire", "Muqui", "Nova Venécia", "Pancas",
  "Pedro Canário", "Pinheiros", "Piúma", "Ponto Belo", "Presidente Kennedy",
  "Rio Bananal", "Rio Novo do Sul", "Santa Leopoldina", "Santa Maria de Jetibá",
  "Santa Teresa", "São Domingos do Norte", "São Gabriel da Palha", "São José do Calçado",
  "São Mateus", "São Roque do Canaã", "Serra", "Sooretama", "Vargem Alta",
  "Venda Nova do Imigrante", "Viana", "Vila Pavão", "Vila Valério", "Vila Velha", "Vitória"
)

# 2. Funções de Auxílio 
corrigir_valores <- function(x) {
  x <- str_squish(str_remove_all(x, "[R$\\s]"))
  x <- case_when(
    str_detect(x, ",") ~ str_replace_all(str_remove_all(x, "\\."), ",", "."),
    TRUE ~ x
  )
  return(as.numeric(x))
}

padronizar_bairros <- function(texto) {
  # lógica completa de padronização de bairros
  texto_norm <- texto %>% 
    str_to_upper() %>% 
    stri_trans_general("Latin-ASCII") %>% 
    str_squish()
  return(texto_norm) 
}

# 3. Integração com MinIO (Baseado na Referência)
read_from_minio_duckdb <- function() {
  cat("[SILVER] Buscando arquivo mais recente na Bronze...\n")
  arquivos <- list_parquet_files_in_minio("bronze/turismo_serra/")
  if (length(arquivos) == 0) stop("Nenhum arquivo encontrado na Bronze")
  
  # Pega o arquivo mais recente (ordem alfabética/data)
  caminho <- sub(sprintf("^s3://%s/", Sys.getenv("MINIO_BUCKET", "airflow")), "", sort(arquivos, TRUE)[1])
  read_parquet_from_minio(caminho)
}

process_data <- function(data) {
  cat("[SILVER] Processando", nrow(data), "registros...\n")
  
  data %>%
    mutate(
      vlrservicos  = corrigir_valores(vlrservicos),
      iss          = corrigir_valores(iss),
      totalliquido = corrigir_valores(totalliquido),
      mesreferencia = as.integer(mesreferencia),
      anoreferencia = as.integer(anoreferencia),
      dtprestacao = as.Date(dtprestacao)
    ) %>%
    filter(!is.na(vlrservicos), vlrservicos > 0) %>%
    mutate(
      prestadorbairro = padronizar_bairros(prestadorbairro),
      origem_cliente = ifelse(tomadorcidade == "Serra", "Consumo Interno", "Consumo Externo")
    )
}

save_to_minio_duckdb <- function(data) {
  timestamp <- format(Sys.time(), "%Y%m%d")
  filepath <- sprintf("silver/turismo_serra/turismo_serra_2_%s.parquet", timestamp)
  cat("[SILVER] Salvando resultado em:", filepath, "\n")
  write_parquet_to_minio(data, filepath)
  return(filepath)
}

# 4. Execução do Fluxo
tryCatch({
  message("--- INICIANDO PIPELINE SILVER ---")
  bronze <- read_from_minio_duckdb()
  silver <- process_data(bronze)
  saida  <- save_to_minio_duckdb(silver)
  cat("[SILVER] Finalizado com sucesso:", saida, "\n")
}, error = function(e) {
  cat("[SILVER] Erro Crítico:", conditionMessage(e), "\n")
  quit(status = 1)
})
