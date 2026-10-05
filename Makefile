# ICD10WHO preprocessed build
#
# Pipeline:
#   make acquire        — fetch ICD-10 WHO from WHO API → tmp/icd10who_raw.ttl
#   make build          — ROBOT preprocessing → tmp/transformed-icd10who.owl
#   make build-release  — build + LinkML + release assets (see docs/release-assets.md)
#
# Requires: ROBOT (≥ 1.9), obolibrary/odkfull Docker image for CI, uv for Python.
# Auth: set CLIENT_ID and CLIENT_SECRET in env/.env (see env/.env.example).
# CI should run `make dependencies` after `uv sync` so linkml/linkml-runtime match
# the mondo-source-ingest workaround (main-branch linkml-runtime; linkml-owl 0.5.0).

ROBOT       ?= robot
ROBOT_PLUGINS_DIRECTORY ?= /home/$(USER)/.robot/plugins
PYTHON      ?= python3
UV          ?= uv
# --no-sync: recipes use the env from `uv sync` / `make dependencies`.
# sssom and semsql must go through UV_RUN. Bare names hit the ODK image on PATH.
UV_RUN      ?= uv run --no-sync
CONFIG_DIR  := config
METADATA_DIR := metadata
SCRIPTS_DIR := scripts
SPARQL_DIR  := sparql
TMP_DIR     := tmp
REPORTS_DIR := reports
MAPPINGS_DIR := mappings
DOCS_SOURCES_DIR := docs/sources
DOCS_METRICS_DIR := docs/metrics
# ROBOT-processed intermediate (mondo-source-ingest: tmp/transformed-<source>.owl; not released)
OUTPUT_OWL  := $(TMP_DIR)/transformed-icd10who.owl
# linkml-owl dumps functional syntax; not released
FUNCTIONAL_OWL := $(TMP_DIR)/icd10who.functional.owl
# Released component OWL (RDF/XML) for mondo-ingest wget
OUTPUT_OWL_LINKML := icd10who.owl
# Release TTL/OWL mirror assets at repo root (names match mondo-ingest wget targets)
RAW_TTL_RELEASE := icd10who.raw.ttl
MIRROR_TTL_RELEASE := icd10who.mirror.ttl
MIRROR_OWL_RELEASE := mirror-icd10who.owl
DB_RELEASE := icd10who.db
MIRROR_OWL  := $(TMP_DIR)/mirror-icd10who.owl
RAW_TTL     := $(TMP_DIR)/icd10who_raw.ttl
YAML_OUT    := icd10who.yaml
SCHEMA      := linkml/mondo_source_schema.yaml
ONTOLOGY_IRI := https://github.com/monarch-initiative/icd10who/releases/latest/download/icd10who.owl
URIBASE     := http://purl.obolibrary.org/obo
TODAY       ?= $(shell date +%Y-%m-%d)

# Release bundle (uploaded to GitHub Releases; not committed)
# Signature basenames match mondo-ingest reports/*-icd10who.tsv ($(@F) wget)
MIRROR_SIGNATURE := $(REPORTS_DIR)/mirror_signature-icd10who.tsv
COMPONENT_SIGNATURE := $(REPORTS_DIR)/component_signature-icd10who.tsv
METRICS_JSON := $(REPORTS_DIR)/icd10who-metrics.json
SOURCE_VERSION_TSV := $(REPORTS_DIR)/source-version.tsv
SSSOM_TSV := $(MAPPINGS_DIR)/icd10who.sssom.tsv
SOURCE_DOC := $(DOCS_SOURCES_DIR)/icd10who.md
METRICS_DOC := $(DOCS_METRICS_DIR)/icd10who.md
COMPONENT_JSON := $(TMP_DIR)/component-icd10who.json
# semsql expects tmp/<stem>.owl next to tmp/<stem>.db
SEMSQL_OWL := $(TMP_DIR)/icd10who-semsql.owl

RELEASE_ASSETS := \
	$(YAML_OUT) \
	$(OUTPUT_OWL_LINKML) \
	$(RAW_TTL_RELEASE) \
	$(MIRROR_TTL_RELEASE) \
	$(MIRROR_OWL_RELEASE) \
	$(DB_RELEASE) \
	$(MIRROR_SIGNATURE) \
	$(COMPONENT_SIGNATURE) \
	$(METRICS_JSON) \
	$(SOURCE_VERSION_TSV) \
	$(SSSOM_TSV) \
	$(SOURCE_DOC) \
	$(METRICS_DOC)

.PHONY: all build build-release acquire clean dependencies verify release-dirs

all: build

$(TMP_DIR) $(REPORTS_DIR) $(MAPPINGS_DIR) $(DOCS_SOURCES_DIR) $(DOCS_METRICS_DIR):
	mkdir -p $@

release-dirs: $(TMP_DIR) $(REPORTS_DIR) $(MAPPINGS_DIR) $(DOCS_SOURCES_DIR) $(DOCS_METRICS_DIR)

# ── Acquire: WHO API → Turtle ─────────────────────────────────────────────────
$(RAW_TTL): | $(TMP_DIR)
	$(UV_RUN) python $(SCRIPTS_DIR)/acquire.py --output $@
	@echo "Acquired: $@"

acquire: $(RAW_TTL)

# ── Mirror: merge + normalize ─────────────────────────────────────────────────
$(MIRROR_OWL): $(RAW_TTL) | $(TMP_DIR)
	ROBOT_PLUGINS_DIRECTORY=$(ROBOT_PLUGINS_DIRECTORY) \
	$(ROBOT) merge -i $(RAW_TTL) \
		odk:normalize --add-source true \
		-o $@
	@echo "Built $@"

# ── Component: normalize xrefs, add exact synonyms, strip non-allowlisted props ─
$(OUTPUT_OWL): $(MIRROR_OWL) \
		$(CONFIG_DIR)/properties.txt \
		$(SPARQL_DIR)/fix_xref_prefixes.ru \
		$(SPARQL_DIR)/exact_syn_from_label.ru
	$(ROBOT) merge -i $(MIRROR_OWL) \
		query \
			--update $(SPARQL_DIR)/fix_xref_prefixes.ru \
		query \
			--update $(SPARQL_DIR)/exact_syn_from_label.ru \
		remove -T $(CONFIG_DIR)/properties.txt --select complement --select properties --trim true \
		annotate \
			--ontology-iri $(URIBASE)/mondo/sources/icd10who.owl \
			--version-iri $(URIBASE)/mondo/sources/$(TODAY)/icd10who.owl \
		-o $@
	@echo "Built $@"

build: $(OUTPUT_OWL)
	@echo "Build complete: $(OUTPUT_OWL)"

# ── Release TTL/OWL mirror assets ─────────────────────────────────────────────
$(RAW_TTL_RELEASE): $(RAW_TTL)
	cp $(RAW_TTL) $@
	@echo "Built $@"

$(MIRROR_TTL_RELEASE): $(MIRROR_OWL)
	$(ROBOT) convert -i $(MIRROR_OWL) -f ttl -o $@
	@echo "Built $@"

$(MIRROR_OWL_RELEASE): $(MIRROR_OWL) | release-dirs
	$(ROBOT) convert -i $(MIRROR_OWL) -o $@
	@echo "Built $@"

# ── LinkML YAML + functional OWL (tmp), then RDF/XML release OWL ───────────────
$(YAML_OUT) $(FUNCTIONAL_OWL): $(OUTPUT_OWL) | release-dirs
	$(UV_RUN) python $(SCRIPTS_DIR)/transform.py \
		--input $(OUTPUT_OWL) --schema $(SCHEMA) --output $(YAML_OUT)
	$(UV_RUN) python -m linkml.validator.cli \
		--schema $(SCHEMA) --target-class OntologyDocument $(YAML_OUT)
	$(UV_RUN) python $(SCRIPTS_DIR)/verify.py --yaml $(YAML_OUT)
	$(UV_RUN) python -m linkml_owl.dumpers.owl_dumper \
		--schema $(SCHEMA) -o $(FUNCTIONAL_OWL) $(YAML_OUT)
	@echo "Built $(YAML_OUT) and $(FUNCTIONAL_OWL)"

# RDF/XML for mondo-ingest / semsql (linkml-owl emits functional syntax).
# Declare synonym-type properties after the LinkML dump. Without the
# declaration, robot filter drops the hasSynonymType annotations.
$(OUTPUT_OWL_LINKML): $(FUNCTIONAL_OWL) $(SPARQL_DIR)/declare_synonym_types.ru
	$(ROBOT) convert -i $(FUNCTIONAL_OWL) -o $(TMP_DIR)/icd10who.rdfxml.owl
	$(ROBOT) query -i $(TMP_DIR)/icd10who.rdfxml.owl \
		--update $(SPARQL_DIR)/declare_synonym_types.ru \
		-o $@
	rm -f $(TMP_DIR)/icd10who.rdfxml.owl
	@echo "Built $@"

# ── semsql index (uses released RDF/XML OWL) ────────────────────────────────────
$(SEMSQL_OWL): $(OUTPUT_OWL_LINKML) | $(TMP_DIR)
	cp $(OUTPUT_OWL_LINKML) $@
	@echo "Built $@"

$(DB_RELEASE): $(SEMSQL_OWL) $(CONFIG_DIR)/prefixes.csv | release-dirs
	@rm -f $(TMP_DIR)/icd10who-semsql.db .template.db .template.db.tmp $(TMP_DIR)/icd10who-semsql-relation-graph.tsv.gz
	RUST_BACKTRACE=full $(UV_RUN) semsql make $(TMP_DIR)/icd10who-semsql.db -P $(CONFIG_DIR)/prefixes.csv
	@rm -f .template.db .template.db.tmp $(TMP_DIR)/icd10who-semsql-relation-graph.tsv.gz
	@test -f $(TMP_DIR)/icd10who-semsql.db || (echo "Error: $(TMP_DIR)/icd10who-semsql.db not found" && exit 1)
	mv $(TMP_DIR)/icd10who-semsql.db $(DB_RELEASE)
	@echo "Built $@"

# ── Signatures ──────────────────────────────────────────────────────────────────
$(MIRROR_SIGNATURE): $(MIRROR_OWL_RELEASE) $(SPARQL_DIR)/classes.sparql | release-dirs
	$(ROBOT) query -i $(MIRROR_OWL_RELEASE) --query $(SPARQL_DIR)/classes.sparql $@
	(head -n 1 $@ && tail -n +2 $@ | sort) > $@-temp
	mv $@-temp $@
	@echo "Built $@"

$(COMPONENT_SIGNATURE): $(OUTPUT_OWL_LINKML) $(SPARQL_DIR)/classes.sparql | release-dirs
	$(ROBOT) query -i $(OUTPUT_OWL_LINKML) --query $(SPARQL_DIR)/classes.sparql $@
	(head -n 1 $@ && tail -n +2 $@ | sort) > $@-temp
	mv $@-temp $@
	@echo "Built $@"

# ── SSSOM xref export ───────────────────────────────────────────────────────────
$(COMPONENT_JSON): $(OUTPUT_OWL_LINKML) | $(TMP_DIR)
	$(ROBOT) convert -i $(OUTPUT_OWL_LINKML) -f json -o $@
	@echo "Built $@"

$(SSSOM_TSV): $(COMPONENT_JSON) $(CONFIG_DIR)/mondo.sssom.config.yml | release-dirs
	$(UV_RUN) sssom parse $(COMPONENT_JSON) -I obographs-json \
		--prefix-map-mode metadata_only -m $(CONFIG_DIR)/mondo.sssom.config.yml -o $@
	$(UV_RUN) sssom sort $@ -o $@
	@echo "Built $@"

# ── Metrics and docs ────────────────────────────────────────────────────────────
$(METRICS_JSON): $(OUTPUT_OWL_LINKML) | release-dirs
	$(ROBOT) measure -i $(OUTPUT_OWL_LINKML) --format json --metrics extended -o $@
	@echo "Built $@"

$(METRICS_DOC): $(METRICS_JSON) $(CONFIG_DIR)/source_metrics.md.j2 | release-dirs
	$(UV_RUN) python $(SCRIPTS_DIR)/render_template.py \
		$(CONFIG_DIR)/source_metrics.md.j2 $(METRICS_JSON) $@
	@echo "Built $@"

$(SOURCE_DOC): $(METADATA_DIR)/icd10who-source.yml $(CONFIG_DIR)/source_documentation.md.j2 | release-dirs
	$(UV_RUN) python $(SCRIPTS_DIR)/render_template.py \
		$(CONFIG_DIR)/source_documentation.md.j2 $(METADATA_DIR)/icd10who-source.yml $@
	@echo "Built $@"

# ── Source version ──────────────────────────────────────────────────────────────
$(SOURCE_VERSION_TSV): $(MIRROR_OWL_RELEASE) $(SPARQL_DIR)/get-source-version.sparql | release-dirs
	@printf "source\tontology\tversionIRI\tversionInfo\n" > $@
	@$(ROBOT) query -i $(MIRROR_OWL_RELEASE) -f tsv --query $(SPARQL_DIR)/get-source-version.sparql \
		| tail -n +2 \
		| awk 'BEGIN{FS="\t"; OFS="\t"} {print "icd10who", $$1, $$2, $$3}' >> $@
	@echo "Built $@"

# Pin linkml-owl + main-branch linkml/linkml-runtime (comma-in-synonym workaround).
dependencies:
	$(UV) pip install linkml-owl==0.5.0 \
		"linkml @ git+https://github.com/linkml/linkml.git@main#subdirectory=packages/linkml" \
		"linkml-runtime @ git+https://github.com/linkml/linkml.git@main#subdirectory=packages/linkml_runtime" \
		semsql sssom

verify: $(YAML_OUT)
	$(UV_RUN) python $(SCRIPTS_DIR)/verify.py --yaml $(YAML_OUT)

# ── Full release bundle ─────────────────────────────────────────────────────────
build-release: release-dirs build $(RAW_TTL_RELEASE) $(MIRROR_TTL_RELEASE) \
	$(YAML_OUT) $(FUNCTIONAL_OWL) $(OUTPUT_OWL_LINKML) \
	$(MIRROR_OWL_RELEASE) $(DB_RELEASE) \
	$(MIRROR_SIGNATURE) $(COMPONENT_SIGNATURE) \
	$(SSSOM_TSV) $(METRICS_JSON) $(METRICS_DOC) $(SOURCE_DOC) $(SOURCE_VERSION_TSV)
	@echo "Release bundle complete:"
	@for f in $(RELEASE_ASSETS); do test -f $$f || (echo "Missing: $$f" && exit 1); done
	@ls -la $(RELEASE_ASSETS)

clean:
	rm -f $(OUTPUT_OWL_LINKML) $(YAML_OUT) \
		$(RAW_TTL_RELEASE) $(MIRROR_TTL_RELEASE) $(MIRROR_OWL_RELEASE) $(DB_RELEASE)
	rm -rf $(TMP_DIR) $(REPORTS_DIR) $(MAPPINGS_DIR) $(DOCS_SOURCES_DIR) $(DOCS_METRICS_DIR)
