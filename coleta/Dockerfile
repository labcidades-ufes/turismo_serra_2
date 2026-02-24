FROM utils-r_duckdb:latest

RUN R -e "install.packages(c('httr','jsonlite', ...), repos='https://cloud.r-project.org/')"

WORKDIR /app
COPY dags/utils/utils.R .
COPY dags/turismo_serra_2/coleta/coleta.R .
CMD ["Rscript", "coleta.R"]