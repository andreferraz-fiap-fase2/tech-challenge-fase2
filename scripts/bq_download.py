"""Baixa as tabelas REAIS do Indicador Criança Alfabetizada direto do BigQuery.

Conecta localmente usando o cliente oficial `google-cloud-bigquery`. As consultas
são faturadas no SEU projeto (free tier de 1 TB/mês cobre de sobra), mas os dados
do dataset `basedosdados.br_inep_avaliacao_alfabetizacao` são públicos.

Autenticação (escolha uma):
  - Service Account key:  defina GOOGLE_APPLICATION_CREDENTIALS=caminho/gcp-key.json
  - ADC (gcloud):         gcloud auth application-default login

Uso:
    export GCP_PROJECT_ID=seu-project-id           # (ou passe --project)
    export GOOGLE_APPLICATION_CREDENTIALS=gcp-key.json
    python -m scripts.bq_download --project seu-project-id

Saída: Parquet por tabela em data/real/ + impressão do schema de cada tabela.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PROJECT = "basedosdados"          # projeto público onde vivem os dados
DATASET = "br_inep_avaliacao_alfabetizacao"


def _reconfig() -> None:
    for s in (sys.stdout, sys.stderr):
        try:
            s.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except (AttributeError, ValueError):
            pass


def main() -> None:
    _reconfig()
    ap = argparse.ArgumentParser(description="Download das tabelas reais via BigQuery.")
    ap.add_argument("--project", default=os.getenv("GCP_PROJECT_ID"),
                    help="ID do SEU projeto GCP (billing das queries)")
    ap.add_argument("--out", default=str(REPO_ROOT / "data" / "real"))
    ap.add_argument("--limit-alunos", type=int, default=0,
                    help="limita linhas da tabela 'alunos' (0 = tudo)")
    args = ap.parse_args()

    if not args.project:
        sys.exit("Informe o projeto: --project SEU_ID  (ou defina GCP_PROJECT_ID)")

    try:
        from google.cloud import bigquery
    except ImportError:
        sys.exit("Rode: .venv/Scripts/python -m pip install google-cloud-bigquery db-dtypes")
    import pyarrow.parquet as pq

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    client = bigquery.Client(project=args.project)
    print(f"Conectado. Projeto de billing: {args.project}\n")

    # 1) Descobre tabelas + schema
    cols = client.query(
        f"SELECT table_name, column_name, data_type "
        f"FROM `{SOURCE_PROJECT}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` "
        f"ORDER BY table_name, ordinal_position"
    ).result()

    schema: dict[str, list[tuple[str, str]]] = {}
    for row in cols:
        schema.setdefault(row.table_name, []).append((row.column_name, row.data_type))

    print("=== SCHEMA DAS TABELAS ===")
    for tbl, fields in schema.items():
        print(f"\n[{tbl}]")
        for name, dtype in fields:
            print(f"  - {name}: {dtype}")
    print("\n" + "=" * 40 + "\n")

    # 2) Baixa cada tabela do dataset para Parquet
    for tbl in schema:
        sql = f"SELECT * FROM `{SOURCE_PROJECT}.{DATASET}.{tbl}`"
        if tbl == "alunos" and args.limit_alunos > 0:
            sql += f" LIMIT {args.limit_alunos}"
        print(f"→ baixando {tbl} ...", flush=True)
        table = client.query(sql).to_arrow()
        dest = out / f"{tbl}.parquet"
        pq.write_table(table, dest, compression="zstd")
        print(f"   ok: {dest.name} — {table.num_rows} linhas, {table.num_columns} colunas")

    # 3) Diretórios oficiais (nomes de UF e Município — não existem no dataset do indicador)
    diretorios = {
        "diretorio_uf": "SELECT sigla AS sigla_uf, nome AS nome_uf, regiao "
                        "FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        "diretorio_municipio": "SELECT id_municipio, nome AS nome_municipio, sigla_uf "
                               "FROM `basedosdados.br_bd_diretorios_brasil.municipio`",
    }
    for nome, sql in diretorios.items():
        print(f"→ baixando {nome} ...", flush=True)
        table = client.query(sql).to_arrow()
        dest = out / f"{nome}.parquet"
        pq.write_table(table, dest, compression="zstd")
        print(f"   ok: {dest.name} — {table.num_rows} linhas, {table.num_columns} colunas")

    print(f"\nConcluído. Arquivos em {out}")


if __name__ == "__main__":
    main()
