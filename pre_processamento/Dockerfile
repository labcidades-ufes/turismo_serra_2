FROM utils-r_duckdb:latest

RUN R -e "install.packages(c('dplyr','readr','sf','geobr','duckdb'), repos='https://cloud.r-project.org/')"

WORKDIR /app
COPY dags/utils/utils.R .
COPY dags/turismo_serra_2/pre_processamento/*pre_processamento.R .
CMD ["Rscript", "pre_processamento.R"]
