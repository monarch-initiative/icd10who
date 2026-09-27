# ICD10WHO GitHub release assets

Each release publishes preprocessing outputs for [mondo-ingest](https://github.com/monarch-initiative/mondo-ingest) to consume via `wget` (alignment stays in mondo-ingest).

## Core ontology

| File | Purpose |
|------|---------|
| `icd10who.yaml` | LinkML source document |
| `icd10who.owl` | Preprocessed component OWL (mondo-ingest `components/icd10who.owl`) |
| `icd10who.raw.ttl` | Raw WHO API mirror (Turtle) |
| `icd10who.mirror.ttl` | Normalized mirror (Turtle) |
| `mirror-icd10who.owl` | Normalized mirror OWL/RDF-XML (mondo-ingest `tmp/mirror-icd10who.owl`) |
| `icd10who.db` | semsql SQLite index over `icd10who.owl` |

## Reports and mappings

| File | Purpose |
|------|---------|
| `reports/mirror_signature-icd10who.tsv` | Class listing of mirror ontology (drift detection) |
| `reports/component_signature-icd10who.tsv` | Class listing of component ontology |
| `reports/icd10who-metrics.json` | ROBOT extended metrics (JSON) |
| `reports/source-version.tsv` | Ontology version IRI / versionInfo for this release |
| `mappings/icd10who.sssom.tsv` | SSSOM export of xrefs embedded in the component |

## Documentation

| File | Purpose |
|------|---------|
| `docs/sources/icd10who.md` | Human-readable source summary |
| `docs/metrics/icd10who.md` | Human-readable metrics report |

## Build

```bash
uv sync            # Python deps from pyproject.toml
make build-release  # all release assets above (CI uses odkfull)
```

## mondo-ingest consumption (target)

mondo-ingest wget's these assets and runs alignment only (slurp, lexmatch, sync, exclusions).
