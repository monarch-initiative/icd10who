# ICD10WHO Ingest — Pipeline incidents

## Unanticipated events and resolutions

### 1. `.env` location

**Event:** The user had credentials in a `.env` at the project root (`/workspace/Projects/icd10who/.env`) rather than the canonical `env/.env` location defined in the scaffold.

**Resolution:** Copied the root `.env` to `env/.env` so `acquire.py` can find it via the standard path. Both locations work; `env/.env` is the one that is `.gitignore`d by the scaffold.

### 2. WHO API token is short-lived (~1 hour)

**Event:** The WHO OAuth2 token expires after approximately 1 hour, but the full ICD-10 traversal takes 20–50 minutes and can be interrupted.

**Resolution:** `acquire.py` proactively refreshes the token every 55 minutes and also catches `401` HTTP errors to re-authenticate on the fly. All fetched node responses are cached to `tmp/cache/` so an interrupted run can resume without re-fetching already-cached nodes.

### 3. IRI namespace not an OBO PURL

**Event:** ICD10WHO class IRIs use WHO's own browse namespace (`https://icd.who.int/browse10/2019/en#/`) rather than an OBO PURL. This means CURIEs are `ICD10WHO:A00.0` rather than the more common `obo:ICD10WHO_A00.0`.

**Resolution:** The `ICD10WHO:` prefix is declared in the Turtle output and preserved through ROBOT. `transform.py` converts IRIs to `ICD10WHO:<code>` CURIEs for the YAML output. No remapping is needed in `config/property-map.sssom.tsv` since the source already uses standard `rdfs:label` and `rdfs:subClassOf`.

### 4. No definitions or synonyms in source

**Event:** The WHO ICD-10 API returns only labels (`title`), parent URIs, and child URIs. There are no definition fields or synonym fields.

**Resolution:** Definitions are left absent (the `definition` slot remains empty for all terms). Exact synonyms are generated from labels via the SPARQL update `sparql/exact_syn_from_label.ru`, following the same Mondo convention used by ICD10CM. YAML emits synonyms as inlined `Synonym` objects with `synonym_type: generated_from_label` per `mondo_source_schema` v0.4.0.

### 5. `linkml-owl` produces empty OWL without `annotations: owl:` on slots

**Event:** The initial LinkML schema used inline `attributes:` instead of top-level `slots:`, and lacked `annotations: owl: AnnotationAssertion` on each slot. Running `linkml-owl` produced an OWL file with only the ontology header (12 lines, ~600 bytes) and no class axioms. The only output was an `rdflib.term WARNING` logging the entire `OntologyDocument` object as an unresolvable literal.

**Resolution:** Rewrote the schema to use top-level `slots:` with `annotations: owl: AnnotationAssertion` on every annotation property slot and `annotations: owl: SubClassOf` on the `parents` slot — matching the working ordo schema pattern. Also added `dcterms:` prefix and `default_prefix: mondo_src`. After this fix, `linkml-owl` produced a 4.1 MB OWL file with 37,791 `AnnotationAssertion` axioms and 12,575 `SubClassOf` axioms.

### 6. Release namespace hardcoded to 2019

**Event:** `acquire.py` uses `release=latest` by default, which resolves to the 2019 release URI (`http://id.who.int/icd/release/10/2019`). The RDF graph is serialised with the namespace `https://icd.who.int/browse10/2019/en#/`, which is version-specific.

**Resolution:** The namespace is derived from the actual WHO browse URLs used in the API responses. If a different release year is selected via `--release`, the browse URL will differ and the namespace should be updated accordingly. This is a known limitation documented here.

### 7. Term count 12,597 vs. previously committed 4,894

**Event:** The old `monarch-initiative/icd10who` repo committed a TTL with 4,894 terms. Our traversal produced 12,597.

**Resolution:** The old tool used Python recursive DFS and hit the 1,000-frame recursion limit, silently truncating the traversal. Our BFS-based `acquire.py` correctly traverses the entire hierarchy without recursion limits. 12,597 is the correct full term count for the ICD-10 WHO 2019 release.

### 8. `linkml-runtime` inlining bug with commas in synonym text

**Event:** Upstream `linkml_runtime._normalize_inlined` can raise `ValueError` when synonym text contains commas in `inlined_as_list` slots.

**Resolution:** Fixed upstream in [linkml#3367](https://github.com/linkml/linkml/issues/3367) and released in `linkml` / `linkml-runtime` 1.11.1. `make dependencies` installs those releases plus `linkml-owl==0.5.0` after `uv sync`. `transform.py` still quotes strings containing `,`, `:`, `{`, or `}` to reduce parser ambiguity.

### 9. Schema alignment with mondo-source-ingest v0.4.0

**Event:** The schema previously exposed `is_root` in YAML and used plain-string synonyms; the mondo-source-ingest skill requires v0.4.0 with `Synonym` inlined objects, `owl.template` for synonym axioms, no `is_root` in output, and no `ifabsent` on `deprecated`.

**Resolution:** Updated `linkml/mondo_source_schema.yaml` to v0.4.0, adjusted `transform.py`, added `scripts/verify.py`, and included `sparql/fix_xref_prefixes.ru` in the ROBOT chain (no-op when the WHO-derived graph has no `hasDbXref`).
