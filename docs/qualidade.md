# Qualidade e Validação de Dados

Como o pipeline garante que **dado ruim não chega à camada analítica**. Este
documento detalha os *scripts de validação* (`src/quality/`), onde cada regra é
aplicada, como a reprovação bloqueia o pipeline e como os resultados são
auditados.

> Requisito do desafio: *"A pipeline deve incluir mecanismos de validação como
> verificação de duplicidade, detecção de valores ausentes, validação de chaves
> de relacionamento e consistência entre tabelas."* — todas cobertas abaixo.

---

## 1. Filosofia: *fail-fast* no portão Bronze → Silver

As validações rodam na **promoção da camada Bronze para a Silver** — o ponto
exato em que o dado bruto vira dado tratado. A escolha é deliberada:

- a **Bronze** preserva o dado como veio (auditoria), então não se valida lá;
- a **Silver** é a primeira camada com contrato de qualidade: se uma regra
  **bloqueante** falha, o pipeline **para na hora** (`raise ValueError`) e o dado
  reprovado **não** avança para a Gold nem para dashboards/ML;
- toda verificação — passando ou falhando — é **persistida em JSON** para
  auditoria (`monitoring/quality/<tabela>.json`).

O efeito prático: é impossível um relatório executivo consumir, por exemplo, uma
taxa de alfabetização de 150% ou um aluno ligado a um município inexistente —
o pipeline teria falhado antes de gerar a Gold.

---

## 2. As verificações (`src/quality/checks.py`)

Cinco funções puras e reutilizáveis, cada uma recebendo um `DataFrame` (Polars)
e devolvendo um `CheckResult`. Elas cobrem as quatro categorias exigidas:

| # | Função | Categoria do desafio | O que valida |
|---|---|---|---|
| 1 | `check_no_duplicates(df, keys)` | **Duplicidade** | Nenhuma combinação de chave primária se repete |
| 2 | `check_not_null(df, column, max_null_ratio)` | **Valores ausentes** | Proporção de nulos numa coluna crítica ≤ limite |
| 3 | `check_foreign_key(df, fk, parent, parent_key)` | **Chaves de relacionamento** | Todo valor de FK existe na tabela pai (integridade referencial) |
| 4 | `check_range(df, column, min, max)` | **Consistência / domínio** | Valores numéricos dentro da faixa esperada |
| 5 | `check_value_set(df, column, allowed)` | **Consistência / domínio** | Coluna categórica só contém valores de um conjunto permitido |

### 2.1 `check_no_duplicates(df, keys)` — duplicidade
Compara `df.height` com a contagem de combinações distintas das chaves. Se houver
diferença, há duplicatas. Usada para garantir a **granularidade** de cada tabela
(ex.: um aluno só pode ter uma linha por ano).

```python
dup = df.height - df.select(keys).n_unique()
# passed = (dup == 0); offending_rows = dup
```

### 2.2 `check_not_null(df, column, max_null_ratio=0.0)` — valores ausentes
Calcula a proporção de nulos na coluna. Com `max_null_ratio=0.0` a regra é
**bloqueante** (nenhum nulo tolerado — típico de chaves); com um limite > 0 vira
**warning** (nulos aceitáveis, apenas registrados). Isso trata corretamente casos
de negócio como a proficiência nula de alunos **ausentes** na prova — que é dado
legítimo, não erro.

### 2.3 `check_foreign_key(df, fk_column, parent, parent_key)` — integridade referencial
Faz um *anti-join* entre a tabela filha e as chaves únicas da tabela pai: as
linhas que sobram são **órfãs** (FK sem correspondência). Garante a cadeia
`aluno → município → UF`.

### 2.4 `check_range(df, column, min_value, max_value)` — domínio numérico
Conta linhas fora de `[min, max]`. Os limites vêm de `config/settings.yaml`
(seção `quality`), então mudam sem tocar no código:
proficiência ∈ [0, 1000] (escala Saeb) e taxa ∈ [0, 100].

### 2.5 `check_value_set(df, column, allowed)` — domínio categórico
Compara os valores presentes com o conjunto permitido; reporta os inválidos.
Usada, por exemplo, para garantir que `regiao` só assuma as 5 regiões oficiais.

---

## 3. Modelo de resultado: `CheckResult` e `QualityReport`

```python
@dataclass
class CheckResult:
    name: str            # ex.: "fk(id_municipio->id_municipio)"
    passed: bool
    detail: str          # mensagem legível ("3 valores sem correspondência...")
    severity: str = "error"   # "error" (bloqueia) | "warning" (só registra)
    offending_rows: int = 0
```

Um `QualityReport` agrega os `CheckResult` de **uma tabela** e decide o destino:

- `has_blocking_failures` → `True` se qualquer checagem `severity="error"` falhou;
- `warnings` → lista das falhas não-bloqueantes (registradas, não interrompem);
- `as_dict()` → serialização para o JSON de auditoria.

A distinção **error × warning** é o que permite ser rígido onde importa (chaves,
domínios) e tolerante onde o negócio exige (nulos esperados).

---

## 4. Onde cada regra é aplicada (`src/transform/silver.py`)

Cada tabela da Silver monta seu próprio `QualityReport` e chama `_enforce()`
antes de ser gravada. O mapa completo:

| Tabela Silver | Duplicidade | Não-nulo | Chave estrangeira | Faixa / domínio |
|---|---|---|---|---|
| `dim_uf` | `sigla_uf` | `sigla_uf` | — | `regiao ∈ {5 regiões}` |
| `dim_municipio` | `id_municipio` | `id_municipio` | `sigla_uf → dim_uf` | — |
| `fato_aluno` | `id_aluno, ano` | `id_municipio` | `id_municipio → dim_municipio` | `proficiencia ∈ [0,1000]` |
| `fato_municipio` | `id_municipio, ano` | — | — | `taxa ∈ [0,100]` |
| `fato_uf` | `sigla_uf, ano` | — | — | `taxa ∈ [0,100]` |
| `meta_uf` | `sigla_uf, ano_meta` | — | — | `meta ∈ [0,100]` |
| `meta_municipio` | `id_municipio, ano_meta` | — | — | `meta ∈ [0,100]` |

Exemplo real (construção da `dim_municipio`):

```python
rep = QualityReport("dim_municipio")
rep.add(check_no_duplicates(mun, ["id_municipio"]))
rep.add(check_not_null(mun, "id_municipio"))
rep.add(check_foreign_key(mun, "sigla_uf", uf_dim, "sigla_uf"))
_enforce(rep)                       # salva JSON e bloqueia se reprovar
write_table(mun, "silver", "dim_municipio", cfg=cfg)
```

---

## 5. Enforcement: bloqueio e auditoria

```python
def _enforce(report: QualityReport) -> None:
    _save_quality_report(report)                 # sempre grava o JSON
    if report.has_blocking_failures:
        falhas = [r.name for r in report.results
                  if not r.passed and r.severity == "error"]
        raise ValueError(f"Qualidade reprovada em '{report.table}': {falhas}")
```

- **Sempre** grava `monitoring/quality/<tabela>.json`, mesmo quando tudo passa
  (registro de que a validação ocorreu — importante para auditoria);
- **Interrompe** o pipeline na primeira tabela com falha bloqueante — a Gold nunca
  chega a ser construída sobre dado inválido.

Exemplo de relatório gerado:

```json
{
  "table": "dim_municipio",
  "passed": true,
  "checks": [
    {"name": "sem_duplicatas(id_municipio)", "passed": true,
     "detail": "0 linhas duplicadas nas chaves ['id_municipio']",
     "severity": "error", "offending_rows": 0},
    {"name": "fk(sigla_uf->sigla_uf)", "passed": true,
     "detail": "0 valores de 'sigla_uf' sem correspondência na tabela pai",
     "severity": "error", "offending_rows": 0}
  ]
}
```

---

## 6. Validação cruzada (consistência entre tabelas) — a prova ponta a ponta

Além das regras por tabela, a Gold gera `validacao_microdado`, que é uma
**verificação de consistência entre fontes independentes**: reagrega os
**3,87 milhões de microdados de alunos** e compara a taxa calculada com a
`taxa_alfabetizacao` **oficial** (que vem de outra tabela da origem).

Resultado: diferença média de **~0,2 ponto percentual** em 10,4 mil
municípios×ano — a evidência de que a camada Gold reproduz o número oficial e de
que a integração das bases está correta. É a forma mais forte de "consistência
entre tabelas" exigida pelo desafio: dois caminhos independentes chegando ao
mesmo número.

---

## 7. Testes automatizados (`tests/test_quality.py`)

Cada função de validação tem teste unitário cobrindo o caso que **passa** e o que
**falha**, incluindo a contagem de linhas ofensoras:

```bash
python -m unittest tests.test_quality -v
```

| Teste | Verifica |
|---|---|
| `test_no_duplicates_detects_repeated_keys` | detecta chave repetida e conta 1 duplicata |
| `test_no_duplicates_passes_when_unique` | passa quando as chaves são únicas |
| `test_not_null_blocks_when_over_threshold` | bloqueia com nulo acima do limite |
| `test_not_null_allows_within_threshold` | tolera nulo dentro do limite (warning) |
| `test_foreign_key_detects_orphans` | detecta FK órfã (`ZZ` sem pai) |
| `test_range_flags_out_of_bounds` | sinaliza valor fora da faixa (1200 > 1000) |
| `test_value_set_flags_invalid_category` | rejeita categoria inválida (`"Lua"`) |

A suíte completa do projeto (16 testes) roda com:

```bash
python -m unittest discover -s tests -v
```

---

## 8. Como estender

Adicionar uma nova regra é isolado e testável:

1. escreva uma função em `src/quality/checks.py` que receba um `DataFrame` e
   devolva um `CheckResult`;
2. exporte-a em `src/quality/__init__.py`;
3. adicione `rep.add(minha_regra(...))` na tabela relevante em `silver.py`;
4. cubra com um teste em `tests/test_quality.py` (caso que passa + caso que falha).

O contrato `CheckResult`/`QualityReport` garante que a nova regra já entra no
relatório JSON e no mecanismo de bloqueio sem mudanças adicionais.

---

## Resumo

| Mecanismo | Onde | Efeito |
|---|---|---|
| 5 verificações reutilizáveis | `src/quality/checks.py` | duplicidade, nulos, FK, faixa, domínio |
| Aplicação por tabela | `src/transform/silver.py` | 7 tabelas Silver com relatório próprio |
| Bloqueio *fail-fast* | `_enforce()` | dado reprovado não vira Gold |
| Auditoria | `monitoring/quality/*.json` | 1 relatório por tabela, por execução |
| Consistência entre fontes | `gold/validacao_microdado` | microdado × oficial = ~0,2 p.p. |
| Testes | `tests/test_quality.py` | 7 testes (passa + falha por regra) |
