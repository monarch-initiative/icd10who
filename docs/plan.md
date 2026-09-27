# ICD10WHO Ingest — Pipeline Plan

## Source

| Aspect | Detail |
|---|---|
| Source | WHO ICD-10 API (`https://id.who.int/icd/release/10`) |
| Format | Turtle (OWL) produced by `scripts/acquire.py` via WHO API traversal |
| Auth | `CLIENT_ID` + `CLIENT_SECRET` → WHO OAuth2 (token valid ~1 hour) |
| Token endpoint | `https://icdaccessmanagement.who.int/connect/token` |
| Release | Latest (2019 as of repo creation); API also provides 2016, 2010, 2008 |
| Registration | https://icd.who.int/icdapi |

## Pipeline

```
make acquire       → tmp/icd10who_raw.ttl   (WHO API BFS traversal)
make build         → tmp/transformed-icd10who.owl (ROBOT: normalize + xref fix + exact-syn + filter)
make build-release → icd10who.yaml            (LinkML YAML — release)
                   → icd10who.owl             (LinkML-derived OWL — release; mondo-source-ingest convention)
```

## Source data structure (sample)

```turtle
ICD10WHO:A00.0 a owl:Class ;
    rdfs:label "Cholera due to Vibrio cholerae 01, biovar cholerae" ;
    rdfs:seeAlso "https://icd.who.int/browse10/2019/en#/A00.0" ;
    rdfs:subClassOf ICD10WHO:A00 ;
    skos:notation "A00.0" .
```

## Field mappings

| Source field | Schema slot | Notes |
|---|---|---|
| `rdfs:label` | `label` | Primary term name |
| `rdfs:subClassOf` | `parents` | Direct hierarchy; top-level terms point to `owl:Thing` |
| (generated) | `exact_synonyms` | SPARQL generates `oboInOwl:hasExactSynonym` from label; YAML uses inlined `Synonym` with `synonym_type: generated_from_label` |
| `skos:notation` | — | Code retained in OWL; not mapped to YAML slot |
| `rdfs:seeAlso` | — | WHO browse URL; retained in OWL via properties allowlist |

## IRI / CURIE mapping

| Source IRI | CURIE |
|---|---|
| `https://icd.who.int/browse10/2019/en#/A00.0` | `ICD10WHO:A00.0` |

## Source characteristics

- 12,597 terms for the ICD-10 WHO 2019 release (full API traversal; see `docs/pipeline_incidents.md` for the old 4,894-term truncation issue).
- No definitions (`obo:IAO_0000115`)
- No synonyms in source (generated from labels)
- No deprecated terms (`owl:deprecated`)
- Hierarchy: `rdfs:subClassOf` directly (no part-of rewrites needed)

## ROBOT preprocessing steps

1. `merge` + `odk:normalize` — standardise OWL axiom structure
2. `query --update sparql/fix_xref_prefixes.ru` — normalise common `oboInOwl:hasDbXref` prefixes (no-op if absent)
3. `query --update sparql/exact_syn_from_label.ru` — generate exact synonyms
4. `remove -T config/properties.txt --select complement --select properties` — strip non-allowlisted annotation properties
5. `annotate` — add stable ontology IRI and dated version IRI

## Acquisition details

- BFS traversal of WHO ICD-10 API; each node's JSON response cached to `tmp/cache/`
- Token auto-refreshed every 55 minutes to avoid mid-run expiry
- On interruption: re-run with existing cache to resume without re-fetching

## Auth setup

```bash
cp env/.env.example env/.env
# Fill in CLIENT_ID and CLIENT_SECRET from https://icd.who.int/icdapi
```

## CI

- **build.yml** — runs on PR touching source files; requires `CLIENT_ID` and `CLIENT_SECRET` as GitHub secrets; restores `tmp/cache/` for API traversal; runs `uv sync`, `make build-release`.
- **release.yml** — `workflow_dispatch` (and optional schedule/push per workflow file); creates dated release tag and uploads artefacts.
