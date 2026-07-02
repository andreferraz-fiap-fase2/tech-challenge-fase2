"""Gera dados sintéticos fiéis ao schema da Base dos Dados (offline).

Cria arquivos "brutos" na landing zone (`data/landing/`), simulando o que seria
baixado do dataset `br_inep_indicador_crianca_alfabetizada`. A ingestão batch
lê essa landing zone para a camada Bronze.

Injeta *de propósito* alguns problemas de qualidade (duplicata, nulos, FK órfã)
para demonstrar a limpeza na camada Silver e as regras de qualidade.

Uso:
    python -m scripts.generate_sample_data [--students N] [--seed S]
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from pathlib import Path

# UF -> (código IBGE 2 dígitos, nome, região)
UFS = {
    "RO": ("11", "Rondônia", "Norte"), "AC": ("12", "Acre", "Norte"),
    "AM": ("13", "Amazonas", "Norte"), "RR": ("14", "Roraima", "Norte"),
    "PA": ("15", "Pará", "Norte"), "AP": ("16", "Amapá", "Norte"),
    "TO": ("17", "Tocantins", "Norte"), "MA": ("21", "Maranhão", "Nordeste"),
    "PI": ("22", "Piauí", "Nordeste"), "CE": ("23", "Ceará", "Nordeste"),
    "RN": ("24", "Rio Grande do Norte", "Nordeste"), "PB": ("25", "Paraíba", "Nordeste"),
    "PE": ("26", "Pernambuco", "Nordeste"), "AL": ("27", "Alagoas", "Nordeste"),
    "SE": ("28", "Sergipe", "Nordeste"), "BA": ("29", "Bahia", "Nordeste"),
    "MG": ("31", "Minas Gerais", "Sudeste"), "ES": ("32", "Espírito Santo", "Sudeste"),
    "RJ": ("33", "Rio de Janeiro", "Sudeste"), "SP": ("35", "São Paulo", "Sudeste"),
    "PR": ("41", "Paraná", "Sul"), "SC": ("42", "Santa Catarina", "Sul"),
    "RS": ("43", "Rio Grande do Sul", "Sul"), "MS": ("50", "Mato Grosso do Sul", "Centro-Oeste"),
    "MT": ("51", "Mato Grosso", "Centro-Oeste"), "GO": ("52", "Goiás", "Centro-Oeste"),
    "DF": ("53", "Distrito Federal", "Centro-Oeste"),
}

YEARS = [2021, 2022, 2023, 2024]
PONTO_CORTE = 743  # ponto de corte oficial Saeb (Alfabetiza Brasil 2023)

LANDING = Path(__file__).resolve().parents[1] / "data" / "landing"


def _municipios(rng: random.Random, per_uf: int) -> list[dict]:
    rows = []
    for uf, (code, _nome, _reg) in UFS.items():
        for i in range(1, per_uf + 1):
            id_mun = f"{code}{i:05d}"  # 7 dígitos, prefixo IBGE da UF
            rows.append({
                "id_municipio": id_mun,
                "nome_municipio": f"Município {uf}-{i:02d}",
                "sigla_uf": uf,
            })
    return rows


def _write_csv(name: str, rows: list[dict], fieldnames: list[str]) -> None:
    LANDING.mkdir(parents=True, exist_ok=True)
    path = LANDING / name
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  landing/{name:32s} {len(rows):>6} linhas")


def generate(students: int, per_uf: int, seed: int) -> None:
    rng = random.Random(seed)
    print(f"Gerando dados sintéticos (seed={seed})...")

    # 1) UF
    uf_rows = [
        {"sigla_uf": uf, "nome_uf": nome, "regiao": reg}
        for uf, (_c, nome, reg) in UFS.items()
    ]
    _write_csv("uf.csv", uf_rows, ["sigla_uf", "nome_uf", "regiao"])

    # 2) Município
    mun_rows = _municipios(rng, per_uf)
    # >>> problema injetado: 1 município duplicado (duplicidade)
    mun_rows.append(dict(mun_rows[0]))
    _write_csv("municipio.csv", mun_rows, ["id_municipio", "nome_municipio", "sigla_uf"])

    # meta nacional sobe linearmente até 100% em 2030
    def meta_ano(ano: int, base: float) -> float:
        return round(min(1.0, base + (ano - 2021) * 0.05 + rng.uniform(-0.02, 0.02)), 4)

    # 3) Meta Alfabetização Brasil
    meta_br = [{"ano": a, "meta_alfabetizacao": meta_ano(a, 0.55)} for a in YEARS]
    _write_csv("meta_brasil.csv", meta_br, ["ano", "meta_alfabetizacao"])

    # 4) Meta Alfabetização por UF
    meta_uf = [
        {"ano": a, "sigla_uf": uf, "meta_alfabetizacao": meta_ano(a, rng.uniform(0.45, 0.65))}
        for a in YEARS for uf in UFS
    ]
    _write_csv("meta_uf.csv", meta_uf, ["ano", "sigla_uf", "meta_alfabetizacao"])

    # 5) Meta Alfabetização por Município
    valid_ids = [m["id_municipio"] for m in mun_rows]
    meta_mun = [
        {"ano": a, "id_municipio": mid, "meta_alfabetizacao": meta_ano(a, rng.uniform(0.40, 0.70))}
        for a in YEARS for mid in set(valid_ids)
    ]
    _write_csv("meta_municipio.csv", meta_mun, ["ano", "id_municipio", "meta_alfabetizacao"])

    # 6) Dados de alunos (nível aluno; proficiência na escala Saeb)
    aluno_rows = []
    for n in range(students):
        ano = rng.choice(YEARS)
        mid = rng.choice(valid_ids)
        # proficiência ~ normal, deslocando a média para cima ao longo dos anos
        media = 720 + (ano - 2021) * 12
        prof = round(rng.gauss(media, 55), 1)
        prof = max(400.0, min(950.0, prof))
        aluno_rows.append({
            "ano": ano,
            "id_municipio": mid,
            "id_aluno": f"A{n:07d}",
            "proficiencia": prof,
        })

    # >>> problemas injetados na base de alunos:
    #   - 5 registros com proficiência nula (valores ausentes)
    for r in rng.sample(aluno_rows, 5):
        r["proficiencia"] = ""
    #   - 3 registros com id_municipio órfão (FK inválida)
    for _ in range(3):
        aluno_rows.append({
            "ano": rng.choice(YEARS),
            "id_municipio": "9999999",   # não existe em município
            "id_aluno": f"AORF{rng.randint(0, 9999):04d}",
            "proficiencia": round(rng.gauss(730, 40), 1),
        })
    #   - 2 registros duplicados (id_aluno repetido no mesmo ano)
    for r in rng.sample(aluno_rows[:students], 2):
        aluno_rows.append(dict(r))

    rng.shuffle(aluno_rows)
    _write_csv("indicador_aluno.csv", aluno_rows,
               ["ano", "id_municipio", "id_aluno", "proficiencia"])

    # manifesto da landing (metadados de ingestão)
    manifest = {
        "source": "synthetic-generator",
        "dataset": "br_inep_indicador_crianca_alfabetizada",
        "seed": seed,
        "ponto_corte_saeb": PONTO_CORTE,
        "files": [p.name for p in sorted(LANDING.glob("*.csv"))],
    }
    (LANDING / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Concluído. {students} alunos + dimensões em {LANDING}")


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except (AttributeError, ValueError):
            pass
    ap = argparse.ArgumentParser(description="Gerador de dados sintéticos (offline).")
    ap.add_argument("--students", type=int, default=8000, help="nº de registros de alunos")
    ap.add_argument("--per-uf", type=int, default=3, help="municípios por UF")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    generate(students=args.students, per_uf=args.per_uf, seed=args.seed)


if __name__ == "__main__":
    main()
