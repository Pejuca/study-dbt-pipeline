# IPCA Forecast Pipeline

Pipeline completo de dados para ingestão, transformação, modelagem e visualização de séries temporais do IPCA, com forecast automático e deploy de aplicação analítica.

## Visão Geral

Este projeto implementa uma mini-arquitetura de dados utilizando:

- **Ingestão automatizada** via API pública do IPEA
- **Armazenamento analítico** com DuckDB
- **Transformações** usando dbt
- **Modelagem de séries temporais** (ARIMA)
- **Mini Dashboard** em Streamlit
- **Orquestração completa** via GitHub Actions

Todo o pipeline é versionado, reproduzível e roda de forma autônoma.

---

## Arquitetura

Este projeto segue uma arquitetura moderna de dados baseada em camadas:

API IPEA → S3 (raw/bronze) → DuckDB (warehouse) → dbt (silver/gold) → Forecast → Streamlit

O S3 é utilizado como data lake para armazenar dados brutos e praticar conceitos de:
- IAM
- Secrets management
- Cloud storage

O DuckDB funciona como um warehouse local, que acaba versionado em função do GitHub Actions, contendo todas as tabelas transformadas e modelos finais.
