FROM astrocrpublic.azurecr.io/runtime:3.1-12

ENV DBT_VENV=/usr/local/airflow/dbt_venv
ENV PATH="$DBT_VENV/bin:$PATH"

RUN python -m venv $DBT_VENV && \
    pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir dbt-snowflake
