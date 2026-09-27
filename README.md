# icd10who

Preprocessed ICD-10 WHO source for Mondo ingest — LinkML YAML plus RDF/XML component OWL (and related release bundle) for [mondo-ingest](https://github.com/monarch-initiative/mondo-ingest).

## Setup

1. Register at [WHO ICD API](https://icd.who.int/icdapi) for OAuth2 client credentials.
2. Copy `env/.env.example` → `env/.env` and set `CLIENT_ID` and `CLIENT_SECRET`.
3. Install dependencies: `uv sync`

## Run

```bash
make acquire       # WHO API → tmp/icd10who_raw.ttl (~2.5 hrs first run; cached after)
make build         # ROBOT preprocess → tmp/transformed-icd10who.owl
make build-release # YAML + RDF/XML OWL + .db + signatures/mappings/metrics (full release bundle)
make verify        # re-run structural checks on icd10who.yaml (after build-release)
```

**ROBOT:** needed on `PATH` (or use `obolibrary/odkfull` as in CI) for mirror/component/convert steps.

## Workflow

```mermaid
%%{init: {"flowchart": {"htmlLabels": false}} }%%
flowchart TD
  subgraph acquire_stage["1 — Acquire"]
    WHO["WHO ICD-10 API"]
    ACQ["make acquire<br/>scripts/acquire.py"]
    RAW["tmp/icd10who_raw.ttl"]
    WHO --> ACQ --> RAW
  end

  subgraph build_stage["2 — ROBOT preprocess"]
    MIR["merge + odk:normalize"]
    MIRROR["tmp/mirror-icd10who.owl"]
    COMP["xref fix · exact synonym · property filter"]
    TRANS["tmp/transformed-icd10who.owl<br/>(intermediate, not released)"]
    RAW --> MIR --> MIRROR --> COMP --> TRANS
  end

  subgraph linkml_stage["3 — LinkML round-trip"]
    XF["scripts/transform.py"]
    YAML["icd10who.yaml"]
    VAL["linkml-validate + verify.py"]
    DUMP["linkml-owl owl_dumper"]
    FUN["tmp/icd10who.functional.owl"]
    CVT["ROBOT convert → RDF/XML"]
    OWL["icd10who.owl<br/>(released component)"]
    TRANS --> XF --> YAML --> VAL --> DUMP --> FUN --> CVT --> OWL
  end

  subgraph index_stage["4 — Index + side artefacts"]
    SEM["semsql"]
    DB["icd10who.db"]
    SIG["mirror / component signatures"]
    SSSOM["icd10who.sssom.tsv"]
    MET["icd10who-metrics.json"]
    MIRROR --> SIG
    OWL --> SEM --> DB
    OWL --> SIG
    OWL --> SSSOM
    OWL --> MET
  end

  subgraph release_stage["5 — Release + mondo-ingest"]
    REL["GitHub Release<br/>make build-release"]
    MI["mondo-ingest wget<br/>components/ · mappings/ · reports/"]
    YAML --> REL
    OWL --> REL
    DB --> REL
    SIG --> REL
    SSSOM --> REL
    MET --> REL
    RAW -.->|"also: raw.ttl · mirror.ttl · mirror.owl"| REL
    REL --> MI
  end
```

Same diagram as source: [`docs/icd10who-workflow.mmd`](docs/icd10who-workflow.mmd).

## Outputs

| File | Description |
|------|-------------|
| `icd10who.yaml` | Primary LinkML artefact |
| `icd10who.owl` | LinkML-derived component OWL as **RDF/XML** (released; functional dump stays in `tmp/`) |
| `icd10who.db` | semsql index over `icd10who.owl` for mondo-ingest QC/alignment |
| `icd10who.raw.ttl` / `icd10who.mirror.*` | Raw + normalized mirror assets |
| `reports/` · `mappings/` | Signatures, metrics, SSSOM (see [`docs/release-assets.md`](docs/release-assets.md)) |

ROBOT intermediate `tmp/transformed-icd10who.owl` is not released.

## Docs

| Doc | Contents |
|-----|----------|
| [`docs/icd10who-workflow.mmd`](docs/icd10who-workflow.mmd) | Mermaid workflow diagram |
| [`docs/plan.md`](docs/plan.md) | Pipeline architecture, field mappings, ID scheme |
| [`docs/release-assets.md`](docs/release-assets.md) | GitHub Release asset contract for mondo-ingest |
| [`docs/release_notes.md`](docs/release_notes.md) | Ontology stats and verification results per release |
| [`docs/pipeline_incidents.md`](docs/pipeline_incidents.md) | Pipeline incidents: errors, deviations, resolutions |

## CI secrets

GitHub Actions expects repository secrets `CLIENT_ID` and `CLIENT_SECRET` (same as `env/.env`).
