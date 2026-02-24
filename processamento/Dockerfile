FROM utils-r_duckdb:latest

RUN R -e "install.packages(c('dplyr','duckdb','arrow','glue'), repos='https://cloud.r-project.org/')"

WORKDIR /app
COPY dags/utils/utils.R .
COPY dags/turismo_serra_2/processamento/*processamento.R .
CMD ["Rscript", "--vanilla", "gold_processamento.R"]