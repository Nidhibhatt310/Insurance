# End-to-End Data Flow — Insurance Kafka Pipeline

## What this document covers

This document walks through the entire pipeline from a Kafka event arriving to a Power BI-ready
gold table. It explains *why* each layer exists, *what* each file does, and *how* they connect
to each other — with particular focus on how the metadata-driven design works.

---

## 1. The Big Picture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          KAFKA CLUSTER                               │
│  topics: agents, booking_line, claims, customers, policies,          │
│           risk_assessment                                            │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  SASL_SSL streaming
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    JOB 1 — kafka_to_bronze_job                       │
│  pipeline: kafka_to_bronze                                           │
│  engine:   KafkaBronzeIngester                                       │
│  writes:   insurance_dev.bronze.{topic}  (raw Kafka bytes as Delta)  │
└───────────────────────┬──────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    JOB 2 — bronze_to_rdv_job                         │
│                                                                      │
│  Phase 1 — BronzeFlattener                                           │
│    reads:  insurance_dev.bronze.{topic}                              │
│    parses: raw JSON  →  typed columns  (via schema_registry)         │
│    writes: insurance_dev.flatten.{topic}                             │
│                                                                      │
│  Phase 2 — RDVBuilder                                                │
│    reads:  insurance_dev.flatten.{topic}                             │
│    builds: hub_{entity}, sat_{entity}, link_{rel}  (Delta MERGE)     │
│    writes: insurance_dev.rdv.*                                       │
└───────────────────────┬──────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    JOB 3 — rdv_to_gold_job                           │
│  engine:   StarSchemaBuilder                                         │
│    reads:  insurance_dev.rdv.{hub + satellite + link}                │
│    builds: dim_customer, dim_agent, dim_policy                       │
│            fact_claims, fact_policies, fact_bookings                 │
│    writes: insurance_dev.gold.*                                      │
└───────────────────────┬──────────────────────────────────────────────┘
                        │
                        ▼
                    Power BI
```

---

## 2. What "Metadata-Driven" Means Here

Traditional pipelines hardcode topic names, table paths, and transformation rules inside Python.
Adding a new Kafka topic means editing multiple Python files.

In this project **all of that lives in YAML**. Python code is generic "engines" — they read
what to process from configuration and act on it. The result:

| To do this... | Old way | New way |
|---|---|---|
| Add a new Kafka topic | Edit `main.py`, consumer, flattener | Add one YAML file in `config/topics/` |
| Change a table name | Hunt through Python files | Edit the YAML |
| Add a new RDV satellite | Write Python logic | Add columns to the YAML |
| Add a gold dimension | Write Python logic | Add an entry to `gold_schemas.yml` |

---

## 3. Directory Map — What Every File Does

```
Insurance/
│
├── databricks.yml                  # DAB root: bundle name, artifact build, env targets
├── pyproject.toml                  # Python project: dependencies, entry-point, wheel build
│
├── resources/                      # DAB job definitions (one file per job)
│   ├── clusters.yml                # Cluster config (Spark version, node type, Kafka secrets)
│   ├── kafka_job.yml               # Job 1: kafka_to_bronze
│   ├── rdv_job.yml                 # Job 2: bronze_to_rdv
│   └── gold_job.yml                # Job 3: rdv_to_gold
│
└── src/insurance/
    │
    ├── config/                     ← THE METADATA LAYER (YAML only)
    │   ├── pipeline_config.yml     # Kafka secret keys, which topics run per pipeline stage
    │   ├── topics/
    │   │   ├── agents.yml          # Topic metadata: kafka name, bronze/flatten table, RDV hub/sat/link
    │   │   ├── booking_line.yml
    │   │   ├── claims.yml
    │   │   ├── customers.yml
    │   │   ├── policies.yml
    │   │   └── risk_assessment.yml
    │   └── gold_schemas.yml        # Dimension and fact table definitions
    │
    ├── utils/
    │   ├── config.py               # MetadataConfig — the central object injected into every engine
    │   ├── schema_registry.py      # PySpark StructType schemas (one per topic)
    │   ├── constants.py            # Layer names, column name constants
    │   ├── logger.py               # Standard logger factory
    │   └── spark_utils.py          # get_spark(), get_dbutils()
    │
    ├── ingestion/streaming/
    │   └── kafka_consumer.py       # KafkaBronzeIngester — reads Kafka, writes Bronze
    │
    ├── processing/
    │   ├── standardize_data.py     # BronzeFlattener — parses JSON, writes Flatten
    │   ├── rdv_builder.py          # RDVBuilder — builds Hub/Satellite/Link tables
    │   └── star_schema.py          # StarSchemaBuilder — builds Dimensions and Facts
    │
    ├── pipelines/                  # Thin orchestrators — one per DAB job
    │   ├── kafka_to_bronze.py      # Wires up KafkaBronzeIngester and runs it
    │   ├── bronze_to_rdv.py        # Runs BronzeFlattener then RDVBuilder in sequence
    │   └── rdv_to_gold.py          # Runs StarSchemaBuilder for all dims and facts
    │
    └── main.py                     # CLI entry point — routes to the right pipeline module
```

---

## 4. The Glue: MetadataConfig

`utils/config.py` is the most important file to understand. It is instantiated once per job
run and passed into every engine. Nothing else hardcodes a table name or path.

```python
# Every pipeline creates this at startup:
config = MetadataConfig(
    catalog="insurance_dev",     # comes from DAB variable ${var.catalog}
    env="dev",                   # comes from DAB variable ${var.env}
    base_location="abfss://...", # comes from DAB variable ${var.base_location}
)
```

What it gives you:

```python
# Table name resolution — catalog.layer.table
config.bronze_table(topic)     # → "insurance_dev.bronze.claims"
config.flatten_table(topic)    # → "insurance_dev.flatten.claims"
config.rdv_table("hub_claim")  # → "insurance_dev.rdv.hub_claim"
config.gold_table("fact_claims")# → "insurance_dev.gold.fact_claims"

# Checkpoint paths (for streaming recovery)
config.bronze_checkpoint(topic)  # → "abfss://.../checkpoints/bronze/claims"
config.flatten_checkpoint(topic) # → "abfss://.../checkpoints/flatten/claims"

# Topic metadata loaded from YAML on demand
topic = config.get_topic("claims")
topic.kafka_topic          # → "claims"
topic.rdv.hub.business_key # → "claim_id"
topic.rdv.hub.hash_key     # → "claim_hash_key"
topic.rdv.satellites[0].columns # → ["claim_amount", "claim_date", ...]
topic.rdv.links[0].foreign_refs[0].key # → "policy_id"

# Get all topics for a pipeline stage
topics = config.get_pipeline_topics("kafka_to_bronze")  # reads pipeline_config.yml
```

The YAML is loaded lazily (only when `get_topic()` is first called), and topics are cached
so the file is read only once per topic per job run.

---

## 5. Stage-by-Stage Walkthrough

### Stage 1 — Kafka → Bronze

**Triggered by**: `kafka_to_bronze_job` (DAB)
**Entry**: `main.py kafka_to_bronze insurance_dev dev abfss://...`
**File**: `pipelines/kafka_to_bronze.py` → `ingestion/streaming/kafka_consumer.py`

```
pipeline_config.yml
  pipelines.kafka_to_bronze.topics: [agents, booking_line, claims, ...]
        │
        ▼
MetadataConfig.get_pipeline_topics("kafka_to_bronze")
  → loads agents.yml, claims.yml, ... from config/topics/
        │
        ▼
KafkaBronzeIngester.ingest_topic(topic) for each topic
  - reads from Kafka using SASL_SSL (credentials from Databricks secrets)
  - checkpoint: abfss://.../checkpoints/bronze/{topic.name}
  - writes raw Kafka bytes (key, value, topic, partition, offset, timestamp)
    to:  insurance_dev.bronze.{topic.bronze_table}
```

**Bronze table schema** (same for all topics — raw Kafka structure):
```
key            STRING
value          BINARY   ← raw JSON payload as bytes
topic          STRING
partition      INT
offset         LONG
timestamp      TIMESTAMP
timestampType  INT
```

The Kafka secret scope and keys are read from `pipeline_config.yml`:
```yaml
kafka:
  secrets_scope: insurance-kafka-scope-dev
  bootstrap_server_key: kafka-bootstrap-server-dev
  sasl_key: kafka-key-dev
  sasl_secret_key: kafka-secret-dev
```
Changing the secret scope requires only a YAML change.

---

### Stage 2a — Bronze → Flatten

**Part of**: `bronze_to_rdv_job` (Phase 1)
**File**: `pipelines/bronze_to_rdv.py` → `processing/standardize_data.py`

```
Bronze table (raw bytes)
        │
        ▼
BronzeFlattener.flatten_topic(topic)
  - CAST(value AS STRING) → raw_json
  - from_json(raw_json, schema)  ← schema comes from schema_registry.get_schema(topic.schema_class)
  - malformed rows go to _rescued_data column
  - result: typed, flat columns + kafka metadata columns
        │
        ▼
insurance_dev.flatten.{topic.flatten_table}
```

**Flatten table schema** (example for claims):
```
key              STRING
raw_json         STRING
topic            STRING
partition        INT
offset           LONG
kafka_timestamp  TIMESTAMP
claim_id         STRING      ← parsed from JSON
policy_id        STRING
claim_amount     DOUBLE
claim_date       DATE
claim_status     STRING
fraud_flag       BOOLEAN
event_id         STRING
event_timestamp  TIMESTAMP
event_type       STRING
_rescued_data    STRING      ← any fields not in schema land here
```

The schema for `from_json` comes from `utils/schema_registry.py`, referenced by `topic.schema_class`
(the topic YAML sets `schema_class: claims` which maps to `get_claims_schema()`).

---

### Stage 2b — Flatten → RDV (Raw Data Vault)

**Part of**: `bronze_to_rdv_job` (Phase 2 — runs after Phase 1 completes)
**File**: `pipelines/bronze_to_rdv.py` → `processing/rdv_builder.py`

The RDV pattern separates *what a thing is* (hubs), *what it looks like* (satellites),
and *how things relate* (links).

#### Hub — "this entity exists"

```
Flatten table
        │
        ▼
RDVBuilder.build_hub(topic)
  - selects business_key (e.g. claim_id)
  - computes MD5 hash → hash_key  (e.g. claim_hash_key)
  - adds load_date = current_timestamp()
  - adds record_source = "KAFKA"
  - MERGE INTO hub_claim ON hash_key → INSERT if not exists
        │
        ▼
insurance_dev.rdv.hub_claim
  claim_hash_key   STRING   ← MD5 of claim_id
  claim_id         STRING   ← business key
  load_date        TIMESTAMP
  record_source    STRING
```

All of `hub.table`, `hub.business_key`, and `hub.hash_key` come from the YAML:
```yaml
rdv:
  hub:
    name: HUB_CLAIM
    table: hub_claim
    business_key: claim_id
    hash_key: claim_hash_key
```

#### Satellite — "here are the details, with history"

```
Flatten table
        │
        ▼
RDVBuilder.build_satellite(topic, sat)
  - selects hub hash_key + the sat columns (e.g. claim_amount, claim_date, ...)
  - computes hash_diff = MD5 of all sat columns concatenated
    (used to detect row-level changes)
  - MERGE INTO sat_claim
      ON hash_key + load_date → INSERT new row
      WHEN hash_diff changed  → UPDATE existing row
        │
        ▼
insurance_dev.rdv.sat_claim
  claim_hash_key   STRING
  claim_amount     DOUBLE
  claim_date       DATE
  claim_status     STRING
  fraud_flag       BOOLEAN
  event_type       STRING
  event_id         STRING
  load_date        TIMESTAMP
  hash_diff        STRING
  record_source    STRING
```

Satellite columns come from YAML:
```yaml
satellites:
  - name: SAT_CLAIM
    table: sat_claim
    columns: [claim_amount, claim_date, claim_status, fraud_flag, event_type, event_id]
```

#### Link — "these entities are related"

```
Flatten table
        │
        ▼
RDVBuilder.build_link(topic, link)
  - collects all business keys: [claim_id, policy_id]
  - computes link hash = MD5 of all keys concatenated
  - computes individual hash keys for each hub (claim_hash_key, policy_hash_key)
  - MERGE INTO link_claim_policy ON link_hash_key → INSERT if not exists
        │
        ▼
insurance_dev.rdv.link_claim_policy
  claim_policy_link_hash_key  STRING
  claim_hash_key              STRING
  policy_hash_key             STRING
  load_date                   TIMESTAMP
  record_source               STRING
```

Link definition from YAML:
```yaml
links:
  - name: LINK_CLAIM_POLICY
    table: link_claim_policy
    hash_key: claim_policy_link_hash_key
    foreign_refs:
      - hub_table: hub_policy
        key: policy_id
        hash_key: policy_hash_key
```

**Full RDV table inventory** (all 6 topics produce these):

| Topic | Hub | Satellite | Link |
|---|---|---|---|
| agents | hub_agent | sat_agent | — |
| booking_line | hub_booking | sat_booking | link_booking_policy |
| claims | hub_claim | sat_claim | link_claim_policy |
| customers | hub_customer | sat_customer | — |
| policies | hub_policy | sat_policy | link_policy_customer_agent |
| risk_assessment | hub_risk | sat_risk | link_risk_policy |

---

### Stage 3 — RDV → Gold (Star Schema)

**Triggered by**: `rdv_to_gold_job` (DAB)
**Entry**: `main.py rdv_to_gold insurance_dev dev abfss://...`
**File**: `pipelines/rdv_to_gold.py` → `processing/star_schema.py`

All definitions come from `config/gold_schemas.yml`.

#### Dimension tables (SCD Type 1)

Each dimension joins its hub with the *latest* satellite row (using `ROW_NUMBER` window).

```
hub_customer + sat_customer (latest row per hash_key)
        │
        ▼
StarSchemaBuilder.build_dimension(dim_cfg)
        │
        ▼
insurance_dev.gold.dim_customer
  customer_hash_key   STRING
  customer_id         STRING
  name                STRING
  email               STRING
  phone               STRING
  address             STRING
  load_date           TIMESTAMP
```

#### Fact tables

Each fact joins its hub + latest satellite + links to relevant dimensions.

```
hub_claim + sat_claim (latest) + link_claim_policy → dim_policy
        │
        ▼
StarSchemaBuilder.build_fact(fact_cfg)
        │
        ▼
insurance_dev.gold.fact_claims
  claim_hash_key   STRING
  claim_id         STRING
  claim_amount     DOUBLE   ← measure
  policy_hash_key  STRING   ← FK to dim_policy
  load_date        TIMESTAMP
```

**Full gold table inventory**:

| Table | Type | Grain | Measures | Dimensions |
|---|---|---|---|---|
| dim_customer | Dimension | customer_id | — | — |
| dim_agent | Dimension | agent_id | — | — |
| dim_policy | Dimension | policy_id | — | — |
| fact_claims | Fact | claim_id | claim_amount | dim_policy |
| fact_policies | Fact | policy_id | premium_amount | dim_customer, dim_agent |
| fact_bookings | Fact | booking_id | amount | dim_policy |

---

## 6. How DAB Wires It All Together

`databricks.yml` declares three targets: `dev`, `qa`, `prod`. Each target sets:
- `catalog` → `insurance_dev` / `insurance_qa` / `insurance_prod`
- `env` → `dev` / `qa` / `prod`

These are injected as `python_wheel_task` parameters into each job:

```
databricks bundle deploy --target dev
  → deploys 3 jobs, each with parameters:
      ["kafka_to_bronze", "insurance_dev", "dev", "abfss://..."]
      ["bronze_to_rdv",   "insurance_dev", "dev", "abfss://..."]
      ["rdv_to_gold",     "insurance_dev", "dev", "abfss://..."]

databricks bundle deploy --target prod
  → deploys the same 3 jobs but with:
      ["kafka_to_bronze", "insurance_prod", "prod", "abfss://..."]
      ...
```

`main.py` receives `sys.argv[1]` as the pipeline name and routes to the correct module:

```python
# main.py
pipeline = sys.argv[1]   # "kafka_to_bronze"
catalog  = sys.argv[2]   # "insurance_dev"
env      = sys.argv[3]   # "dev"
base_loc = sys.argv[4]   # "abfss://insurance@nbadls1..."

mod = importlib.import_module("insurance.pipelines.kafka_to_bronze")
mod.run(catalog=catalog, env=env, base_location=base_loc)
```

The YAML config files are embedded inside the wheel (packaged via hatchling's `artifacts`
setting in `pyproject.toml`). `MetadataConfig` resolves them at runtime via:
```python
_DEFAULT_CONFIG_DIR = Path(__file__).parent.parent / "config"
# which resolves to: insurance/config/ inside the installed wheel
```

---

## 7. How to Add a New Kafka Topic

This is where the metadata-driven approach pays off. Example: adding a `payments` topic.

**Step 1** — Create `src/insurance/config/topics/payments.yml`:
```yaml
name: payments
kafka_topic: payments
source_type: streaming
schema_class: payments

bronze:
  table: payments

flatten:
  table: payments

rdv:
  hub:
    name: HUB_PAYMENT
    table: hub_payment
    business_key: payment_id
    hash_key: payment_hash_key
  satellites:
    - name: SAT_PAYMENT
      table: sat_payment
      columns: [amount, currency, payment_date, status]
  links:
    - name: LINK_PAYMENT_POLICY
      table: link_payment_policy
      hash_key: payment_policy_link_hash_key
      foreign_refs:
        - hub_table: hub_policy
          key: policy_id
          hash_key: policy_hash_key
```

**Step 2** — Add `payments` to the topic lists in `pipeline_config.yml`:
```yaml
pipelines:
  kafka_to_bronze:
    topics: [..., payments]
  bronze_to_flatten:
    topics: [..., payments]
  flatten_to_rdv:
    topics: [..., payments]
```

**Step 3** — Add a PySpark schema to `utils/schema_registry.py`:
```python
def get_payments_schema() -> StructType: ...
```
(Update the `get_schema()` dispatch at the bottom of that file.)

**That's it.** No other Python changes needed. The next `databricks bundle deploy` will pick
up the new topic across all three jobs automatically.

---

## 8. Data Flow Diagram (All Layers)

```
Kafka Topic: "claims"
      │
      │  SASL_SSL streaming  (KafkaBronzeIngester)
      ▼
insurance_dev.bronze.claims
  key, value (bytes), topic, partition, offset, timestamp
      │
      │  from_json + schema  (BronzeFlattener)
      ▼
insurance_dev.flatten.claims
  key, raw_json, claim_id, policy_id, claim_amount,
  claim_date, claim_status, fraud_flag, event_*, _rescued_data
      │
      ├──── MD5(claim_id)  ──────────────────────────────────────────┐
      │                                                              ▼
      │                                              insurance_dev.rdv.hub_claim
      │                                               claim_hash_key, claim_id
      │
      ├──── MD5(claim_id) + attribute columns ──────────────────────┐
      │                                                              ▼
      │                                              insurance_dev.rdv.sat_claim
      │                                               claim_hash_key, claim_amount,
      │                                               claim_date, ..., hash_diff
      │
      └──── MD5(claim_id + policy_id)  ─────────────────────────────┐
                                                                     ▼
                                             insurance_dev.rdv.link_claim_policy
                                              claim_policy_link_hash_key,
                                              claim_hash_key, policy_hash_key
                                                                     │
                                    hub_claim + sat_claim (latest)   │
                                    + link_claim_policy              │
                                    + dim_policy  ───────────────────┘
                                                                     │
                                                                     ▼
                                              insurance_dev.gold.fact_claims
                                               claim_id, claim_amount,
                                               policy_hash_key, load_date
```

---

## 9. Key Design Decisions

| Decision | Why |
|---|---|
| YAML configs embedded in the wheel | Self-contained deployment; no separate config store needed |
| `MetadataConfig` injected (not imported globally) | Easy to unit-test with a different catalog or config dir |
| Separate DAB job per pipeline stage | Ops flexibility — retry Bronze without re-running Gold |
| Delta MERGE for RDV (not overwrite) | Idempotent; safe to re-run after failures |
| `availableNow` trigger for streaming | Runs as a batch-style job (processes backlog then exits) |
| Satellite hash_diff column | Detects attribute changes without full table scans |
| Gold uses `CREATE OR REPLACE TABLE` | Gold is always fully rebuilt from RDV; simple and correct |
| Single `main.py` entry point | One wheel artifact serves all three jobs via a routing arg |

---

## 10. End-to-End Function Call Trace — `claims` Topic

This section walks the **exact call sequence** for the `claims` topic from CLI invocation to a
Power BI-ready gold row. Every function is shown with the parameters it receives, what it does
internally, and the concrete value it produces.

The working example uses:
- `catalog = "insurance_dev"`, `env = "dev"`
- `base_location = "abfss://insurance@nbadls1.dfs.core.windows.net/data"`
- Kafka message: `{"claim_id":"CLM-001","policy_id":"POL-999","claim_amount":4200.0,"claim_date":"2024-03-01","claim_status":"OPEN","fraud_flag":false,"event_id":"EVT-1","event_timestamp":"2024-03-01T10:00:00","event_type":"CLAIM_CREATED"}`

---

### 10.1 CLI → `main.main()`

**File**: [src/insurance/main.py](src/insurance/main.py)

```
sys.argv = ["main", "kafka_to_bronze", "insurance_dev", "dev",
            "abfss://insurance@nbadls1.dfs.core.windows.net/data"]
```

| Step | Code | Concrete value |
|---|---|---|
| Parse argv | `pipeline, catalog, env, base_location = sys.argv[1:]` | `"kafka_to_bronze"`, `"insurance_dev"`, `"dev"`, `"abfss://..."` |
| Lookup module | `_PIPELINES[pipeline]` | `"insurance.pipelines.kafka_to_bronze"` |
| Dynamic import | `importlib.import_module(...)` | module object for `kafka_to_bronze.py` |
| Dispatch | `mod.run(catalog=..., env=..., base_location=...)` | calls `kafka_to_bronze.run(...)` |

---

### 10.2 Job 1 — `kafka_to_bronze.run()`

**File**: [src/insurance/pipelines/kafka_to_bronze.py](src/insurance/pipelines/kafka_to_bronze.py)

#### Step A — `MetadataConfig.__init__(...)`

```python
config = MetadataConfig(
    catalog="insurance_dev",
    env="dev",
    base_location="abfss://insurance@nbadls1.dfs.core.windows.net/data",
)
```

What happens internally:
- Sets `self.catalog`, `self.env`, `self.base_location`
- Calls `_load_yaml("pipeline_config.yml")` → reads `config/pipeline_config.yml` into `self._pipeline_cfg`
- Initialises `self._topics = {}` (empty cache, topics loaded lazily)

Expected state after construction:
```python
config.catalog          # → "insurance_dev"
config.env              # → "dev"
config._pipeline_cfg    # → full dict from pipeline_config.yml
config._topics          # → {}  (nothing loaded yet)
```

---

#### Step B — `MetadataConfig.get_kafka_secrets_config()`

```python
kafka_cfg = config.get_kafka_secrets_config()
```

Reads `self._pipeline_cfg["kafka"]` (already in memory — no file I/O).

```python
# Returns:
{
    "secrets_scope":       "insurance-kafka-scope-dev",
    "bootstrap_server_key":"kafka-bootstrap-server-dev",
    "sasl_key":            "kafka-key-dev",
    "sasl_secret_key":     "kafka-secret-dev",
}
```

---

#### Step C — `get_kafka_creds(dbutils, kafka_cfg)`

**File**: [src/insurance/ingestion/streaming/kafka_consumer.py](src/insurance/ingestion/streaming/kafka_consumer.py)

```python
creds = get_kafka_creds(dbutils, kafka_cfg)
```

Calls `dbutils.secrets.get(scope, key)` three times:

| Secret key in YAML | Databricks secret scope | Returns |
|---|---|---|
| `bootstrap_server_key` | `insurance-kafka-scope-dev` | `"broker1.confluent.cloud:9092"` |
| `sasl_key` | `insurance-kafka-scope-dev` | `"api-key-abc123"` |
| `sasl_secret_key` | `insurance-kafka-scope-dev` | `"api-secret-xyz789"` |

```python
# Returns:
KafkaCreds(
    bootstrap_server="broker1.confluent.cloud:9092",
    sasl_key="api-key-abc123",
    sasl_secret="api-secret-xyz789",
)
```

---

#### Step D — `MetadataConfig.get_pipeline_topics("kafka_to_bronze")`

```python
topics = config.get_pipeline_topics("kafka_to_bronze")
```

1. Reads `self._pipeline_cfg["pipelines"]["kafka_to_bronze"]["topics"]`
   → `["agents", "booking_line", "claims", "customers", "policies", "risk_assessment"]`
2. For each name calls `get_topic(name)`:
   - `_load_yaml("topics/claims.yml")` → reads `claims.yml`
   - `_parse_topic(raw)` builds a `TopicConfig` dataclass

For `claims.yml`, `_parse_topic` produces:

```python
TopicConfig(
    name="claims",
    kafka_topic="claims",
    source_type="streaming",
    schema_class="claims",
    bronze_table="claims",
    flatten_table="claims",
    rdv=RDVTopicConfig(
        hub=HubConfig(
            name="HUB_CLAIM",
            table="hub_claim",
            business_key="claim_id",
            hash_key="claim_hash_key",
        ),
        satellites=[
            SatelliteConfig(
                name="SAT_CLAIM",
                table="sat_claim",
                columns=["claim_amount","claim_date","claim_status",
                         "fraud_flag","event_type","event_id"],
            )
        ],
        links=[
            LinkConfig(
                name="LINK_CLAIM_POLICY",
                table="link_claim_policy",
                hash_key="claim_policy_link_hash_key",
                foreign_refs=[
                    LinkForeignRef(
                        hub_table="hub_policy",
                        key="policy_id",
                        hash_key="policy_hash_key",
                    )
                ],
            )
        ],
    ),
)
```

The object is cached: `config._topics["claims"] = <above>`. Subsequent calls return the cached object without re-reading the file.

---

#### Step E — `KafkaBronzeIngester.ingest_topic(topic)`

Called once per topic (6 concurrent streams launched). For `claims`:

**`_read_topic(topic)`** — builds the streaming DataFrame:

```python
spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker1.confluent.cloud:9092")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="api-key-abc123" password="api-secret-xyz789";')
    .option("subscribe", "claims")          # ← topic.kafka_topic
    .option("startingOffsets", "earliest")
    .load()
```

Returns a streaming DataFrame with Kafka's native schema:

| Column | Type | Example value |
|---|---|---|
| `key` | BINARY | `b"CLM-001"` |
| `value` | BINARY | `b'{"claim_id":"CLM-001",...}'` |
| `topic` | STRING | `"claims"` |
| `partition` | INT | `0` |
| `offset` | LONG | `1042` |
| `timestamp` | TIMESTAMP | `2024-03-01 10:00:00` |
| `timestampType` | INT | `0` |

**Checkpoint and table helpers** resolve the write destination:

```python
config.bronze_checkpoint(topic)
# → "abfss://insurance@nbadls1.dfs.core.windows.net/data/checkpoints/bronze/claims"

config.bronze_table(topic)
# topic.bronze_table = "claims"
# → "insurance_dev.bronze.claims"
```

**Write stream** configured as:
```python
df.writeStream
  .format("delta")
  .option("checkpointLocation", "abfss://.../checkpoints/bronze/claims")
  .queryName("kafka_bronze_claims")
  .outputMode("append")
  .trigger(availableNow=True)   # processes backlog then exits
  .toTable("insurance_dev.bronze.claims")
```

Returns a `StreamingQuery`. `awaitTermination()` blocks until the stream drains all available Kafka offsets and stops.

---

### 10.3 Job 2 — `bronze_to_rdv.run()` — Phase 1: Flatten

**File**: [src/insurance/pipelines/bronze_to_rdv.py](src/insurance/pipelines/bronze_to_rdv.py)

#### Step F — `BronzeFlattener.flatten_topic(topic)`

**File**: [src/insurance/processing/standardize_data.py](src/insurance/processing/standardize_data.py)

```python
schema = get_schema(topic.schema_class)   # topic.schema_class = "claims"
```

**`get_schema("claims")`** — File: [src/insurance/utils/schema_registry.py](src/insurance/utils/schema_registry.py)

Dispatches to `get_claims_schema()` which returns:

```python
StructType([
    StructField("claim_id",         StringType(),    nullable=False),
    StructField("policy_id",        StringType(),    nullable=True),
    StructField("claim_amount",     DoubleType(),    nullable=True),
    StructField("claim_date",       DateType(),      nullable=True),
    StructField("claim_status",     StringType(),    nullable=True),
    StructField("fraud_flag",       BooleanType(),   nullable=True),
    StructField("event_id",         StringType(),    nullable=True),
    StructField("event_timestamp",  TimestampType(), nullable=True),
    StructField("event_type",       StringType(),    nullable=True),
])
```

Back in `flatten_topic`:

```python
df = spark.readStream.table("insurance_dev.bronze.claims")
```

Reads the bronze table as a streaming source.

**`.selectExpr(...)`** renames and casts Kafka columns:

| Expression | Input | Output column | Example value |
|---|---|---|---|
| `CAST(key AS STRING) as key` | `b"CLM-001"` | `key` STRING | `"CLM-001"` |
| `CAST(value AS STRING) as raw_json` | bytes | `raw_json` STRING | `'{"claim_id":"CLM-001",...}'` |
| `"topic"` | `"claims"` | `topic` | `"claims"` |
| `"partition"` | `0` | `partition` | `0` |
| `"offset"` | `1042` | `offset` | `1042` |
| `"timestamp as kafka_timestamp"` | `2024-03-01 10:00:00` | `kafka_timestamp` | `2024-03-01 10:00:00` |

**`.withColumn("parsed", from_json(col("raw_json"), schema, ...))`**:
- Applies the `StructType` schema to `raw_json`
- Any field not in the schema goes into `_rescued_data` (via `columnNameOfCorruptRecord` option)
- A well-formed row for CLM-001 produces:

```python
parsed.claim_id        = "CLM-001"
parsed.policy_id       = "POL-999"
parsed.claim_amount    = 4200.0
parsed.claim_date      = date(2024, 3, 1)
parsed.claim_status    = "OPEN"
parsed.fraud_flag      = False
parsed.event_id        = "EVT-1"
parsed.event_timestamp = datetime(2024, 3, 1, 10, 0, 0)
parsed.event_type      = "CLAIM_CREATED"
parsed._rescued_data   = None   # (no malformed fields)
```

**`.select("key", "raw_json", "topic", "partition", "offset", "kafka_timestamp", "parsed.*")`**
expands the struct so every field is a top-level column.

Returns a **streaming DataFrame** — not yet written.

---

#### Step G — `BronzeFlattener.write_flatten(df, topic)`

```python
config.flatten_checkpoint(topic)
# → "abfss://insurance@nbadls1.dfs.core.windows.net/data/checkpoints/flatten/claims"

config.flatten_table(topic)
# topic.flatten_table = "claims"
# → "insurance_dev.flatten.claims"
```

```python
df.writeStream
  .format("delta")
  .option("checkpointLocation", "abfss://.../checkpoints/flatten/claims")
  .queryName("flatten_claims")
  .outputMode("append")
  .trigger(availableNow=True)
  .toTable("insurance_dev.flatten.claims")
```

Row written to `insurance_dev.flatten.claims`:

| Column | Value |
|---|---|
| `key` | `"CLM-001"` |
| `raw_json` | `'{"claim_id":"CLM-001","policy_id":"POL-999",...}'` |
| `topic` | `"claims"` |
| `partition` | `0` |
| `offset` | `1042` |
| `kafka_timestamp` | `2024-03-01 10:00:00` |
| `claim_id` | `"CLM-001"` |
| `policy_id` | `"POL-999"` |
| `claim_amount` | `4200.0` |
| `claim_date` | `2024-03-01` |
| `claim_status` | `"OPEN"` |
| `fraud_flag` | `false` |
| `event_id` | `"EVT-1"` |
| `event_timestamp` | `2024-03-01 10:00:00` |
| `event_type` | `"CLAIM_CREATED"` |
| `_rescued_data` | `null` |

---

### 10.4 Job 2 — `bronze_to_rdv.run()` — Phase 2: RDV

**File**: [src/insurance/processing/rdv_builder.py](src/insurance/processing/rdv_builder.py)

All three methods are called from `process_topic(topic)` in order: hub → satellites → links.

---

#### Step H — `RDVBuilder.build_hub(topic)`

```python
hub = topic.rdv.hub
# HubConfig(name="HUB_CLAIM", table="hub_claim",
#           business_key="claim_id", hash_key="claim_hash_key")

hub_table = config.rdv_table(hub.table)
# → "insurance_dev.rdv.hub_claim"
```

**`_flatten_df(topic)`**:
```python
spark.table("insurance_dev.flatten.claims")   # batch read
```

DataFrame transformation:

| Expression | Input | Output | Concrete value |
|---|---|---|---|
| `F.md5(F.col("claim_id").cast("string"))` | `"CLM-001"` | `claim_hash_key` | `"a1b2c3d4e5..."` (32-char MD5 hex) |
| `F.col("claim_id")` | `"CLM-001"` | `claim_id` | `"CLM-001"` |
| `F.current_timestamp()` | — | `load_date` | `2024-03-01 10:05:00` |
| `F.lit("KAFKA")` | — | `record_source` | `"KAFKA"` |

`.dropDuplicates(["claim_hash_key"])` — deduplicates so a re-run of the same Kafka offsets doesn't insert duplicate hubs.

**`_ensure_table(hub_df, "insurance_dev.rdv.hub_claim")`**:
- Tries `spark.table("insurance_dev.rdv.hub_claim")`
- If `AnalysisException` (table doesn't exist): creates it via `hub_df.limit(0).write.format("delta").saveAsTable(...)`
- This bootstraps the schema so the MERGE below always has a target table

**MERGE SQL**:
```sql
MERGE INTO insurance_dev.rdv.hub_claim AS tgt
USING _hub_updates AS src
ON tgt.claim_hash_key = src.claim_hash_key
WHEN NOT MATCHED THEN INSERT *
```

Row inserted into `hub_claim`:

| Column | Value |
|---|---|
| `claim_hash_key` | `"a1b2c3d4e5f6..."` |
| `claim_id` | `"CLM-001"` |
| `load_date` | `2024-03-01 10:05:00` |
| `record_source` | `"KAFKA"` |

Hubs are insert-only — no UPDATE clause. A hub row represents "this entity was first seen"; it never changes.

---

#### Step I — `RDVBuilder.build_satellite(topic, sat)`

```python
sat = topic.rdv.satellites[0]
# SatelliteConfig(name="SAT_CLAIM", table="sat_claim",
#                 columns=["claim_amount","claim_date","claim_status",
#                          "fraud_flag","event_type","event_id"])

sat_table = config.rdv_table(sat.table)
# → "insurance_dev.rdv.sat_claim"
```

DataFrame transformation:

| Expression | Input | Output column | Concrete value |
|---|---|---|---|
| `F.md5(F.col("claim_id").cast("string"))` | `"CLM-001"` | `claim_hash_key` | `"a1b2c3d4..."` |
| `F.col("claim_amount")` | `4200.0` | `claim_amount` | `4200.0` |
| `F.col("claim_date")` | `2024-03-01` | `claim_date` | `2024-03-01` |
| `F.col("claim_status")` | `"OPEN"` | `claim_status` | `"OPEN"` |
| `F.col("fraud_flag")` | `false` | `fraud_flag` | `false` |
| `F.col("event_type")` | `"CLAIM_CREATED"` | `event_type` | `"CLAIM_CREATED"` |
| `F.col("event_id")` | `"EVT-1"` | `event_id` | `"EVT-1"` |
| `F.col("event_timestamp")` | `2024-03-01 10:00:00` | `load_date` | `2024-03-01 10:00:00` |
| `F.md5(concat_ws("\|\|", sat_cols...))` | all sat values concat'd | `hash_diff` | MD5 of `"4200.0\|\|2024-03-01\|\|OPEN\|\|false\|\|CLAIM_CREATED\|\|EVT-1"` |
| `F.lit("KAFKA")` | — | `record_source` | `"KAFKA"` |

The `hash_diff` column is the fingerprint of the full satellite row. It enables change detection:
- `hash_diff` unchanged → same data, skip
- `hash_diff` changed → attributes updated, overwrite the row

**MERGE SQL**:
```sql
MERGE INTO insurance_dev.rdv.sat_claim AS tgt
USING _sat_updates AS src
ON tgt.claim_hash_key = src.claim_hash_key
   AND tgt.load_date  = src.load_date
WHEN MATCHED AND tgt.hash_diff <> src.hash_diff THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Row inserted into `sat_claim`:

| Column | Value |
|---|---|
| `claim_hash_key` | `"a1b2c3d4..."` |
| `claim_amount` | `4200.0` |
| `claim_date` | `2024-03-01` |
| `claim_status` | `"OPEN"` |
| `fraud_flag` | `false` |
| `event_type` | `"CLAIM_CREATED"` |
| `event_id` | `"EVT-1"` |
| `load_date` | `2024-03-01 10:00:00` |
| `hash_diff` | `"d3f1a2..."` |
| `record_source` | `"KAFKA"` |

---

#### Step J — `RDVBuilder.build_link(topic, link)`

```python
link = topic.rdv.links[0]
# LinkConfig(name="LINK_CLAIM_POLICY", table="link_claim_policy",
#            hash_key="claim_policy_link_hash_key",
#            foreign_refs=[LinkForeignRef(hub_table="hub_policy",
#                                        key="policy_id",
#                                        hash_key="policy_hash_key")])

all_biz_keys = ["claim_id", "policy_id"]   # hub key + all foreign ref keys
link_table = config.rdv_table(link.table)
# → "insurance_dev.rdv.link_claim_policy"
```

DataFrame transformation:

| Expression | Input | Output column | Concrete value |
|---|---|---|---|
| `F.md5(concat_ws("\|\|", "claim_id", "policy_id"))` | `"CLM-001\|\|POL-999"` | `claim_policy_link_hash_key` | MD5 of composite key |
| `F.md5(F.col("claim_id"))` | `"CLM-001"` | `claim_hash_key` | `"a1b2c3d4..."` |
| `F.md5(F.col("policy_id"))` | `"POL-999"` | `policy_hash_key` | `"f7e8d9..."` |
| `F.current_timestamp()` | — | `load_date` | `2024-03-01 10:05:00` |
| `F.lit("KAFKA")` | — | `record_source` | `"KAFKA"` |

`.dropDuplicates(["claim_policy_link_hash_key"])` — each claim-policy relationship is unique.

**MERGE SQL**:
```sql
MERGE INTO insurance_dev.rdv.link_claim_policy AS tgt
USING _link_updates AS src
ON tgt.claim_policy_link_hash_key = src.claim_policy_link_hash_key
WHEN NOT MATCHED THEN INSERT *
```

Row inserted into `link_claim_policy`:

| Column | Value |
|---|---|
| `claim_policy_link_hash_key` | `"b5c6d7..."` |
| `claim_hash_key` | `"a1b2c3d4..."` |
| `policy_hash_key` | `"f7e8d9..."` |
| `load_date` | `2024-03-01 10:05:00` |
| `record_source` | `"KAFKA"` |

---

### 10.5 Job 3 — `rdv_to_gold.run()`

**File**: [src/insurance/pipelines/rdv_to_gold.py](src/insurance/pipelines/rdv_to_gold.py)

#### Step K — `MetadataConfig.get_gold_schemas()`

```python
gold_cfg = config.get_gold_schemas()
```

Calls `_load_yaml("gold_schemas.yml")` (not cached — re-read on every call to stay fresh).

Returns a dict with `"dimensions"` and `"facts"` lists, e.g.:
```python
{
  "dimensions": [
    {"name": "DIM_CUSTOMER", "table": "dim_customer",
     "source_hub": "hub_customer", "source_satellite": "sat_customer",
     "hub_hash_key": "customer_hash_key", "hub_biz_key": "customer_id",
     "columns": ["name","email","phone","address"]},
    ...
  ],
  "facts": [
    {"name": "FACT_CLAIMS", "table": "fact_claims",
     "source_hub": "hub_claim", "source_satellite": "sat_claim",
     "hub_hash_key": "claim_hash_key", "hub_biz_key": "claim_id",
     "measures": ["claim_amount"],
     "dimensions": [
       {"link_table": "link_claim_policy", "dim_table": "dim_policy",
        "dim_hash_key": "policy_hash_key"}
     ]},
    ...
  ]
}
```

---

#### Step L — `StarSchemaBuilder.build_dimension(dim_cfg)` for `dim_customer`

```python
hub_table = config.rdv_table("hub_customer")   # → "insurance_dev.rdv.hub_customer"
sat_table = config.rdv_table("sat_customer")   # → "insurance_dev.rdv.sat_customer"
dim_table = config.gold_table("dim_customer")  # → "insurance_dev.gold.dim_customer"
hash_key  = "customer_hash_key"
biz_key   = "customer_id"
cols      = "s.name, s.email, s.phone, s.address"
```

SQL executed:
```sql
CREATE OR REPLACE TABLE insurance_dev.gold.dim_customer
USING DELTA AS
SELECT
    h.customer_hash_key,
    h.customer_id,
    s.name, s.email, s.phone, s.address,
    s.load_date
FROM insurance_dev.rdv.hub_customer h
INNER JOIN (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_hash_key
                              ORDER BY load_date DESC) AS _rn
    FROM insurance_dev.rdv.sat_customer
) s ON h.customer_hash_key = s.customer_hash_key AND s._rn = 1
```

The `ROW_NUMBER() ... ORDER BY load_date DESC` window picks the **most recent** satellite row per
customer, implementing SCD Type 1 (latest-value-wins). `_rn = 1` is filtered in the join, so the
`_rn` column never appears in the final table.

Example row in `insurance_dev.gold.dim_customer`:

| Column | Value |
|---|---|
| `customer_hash_key` | `"c1d2e3..."` |
| `customer_id` | `"CUST-042"` |
| `name` | `"Jane Smith"` |
| `email` | `"jane@example.com"` |
| `phone` | `"+1-555-0100"` |
| `address` | `"123 Main St"` |
| `load_date` | `2024-03-01 09:00:00` |

---

#### Step M — `StarSchemaBuilder.build_fact(fact_cfg)` for `fact_claims`

```python
hub_table  = "insurance_dev.rdv.hub_claim"
sat_table  = "insurance_dev.rdv.sat_claim"
fact_table = "insurance_dev.gold.fact_claims"
hash_key   = "claim_hash_key"
biz_key    = "claim_id"
measures   = "s.claim_amount"
```

The `dimensions` loop iterates over one entry (`link_claim_policy` → `dim_policy`).
For index `i=0`:
```python
alias    = "d0"
link_tbl = "insurance_dev.rdv.link_claim_policy"
dim_tbl  = "insurance_dev.gold.dim_policy"
dim_hk   = "policy_hash_key"
```

SQL executed:
```sql
CREATE OR REPLACE TABLE insurance_dev.gold.fact_claims
USING DELTA AS
SELECT
    h.claim_hash_key,
    h.claim_id,
    s.claim_amount,
    d0.policy_hash_key,
    s.load_date
FROM insurance_dev.rdv.hub_claim h
INNER JOIN (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY claim_hash_key
                              ORDER BY load_date DESC) AS _rn
    FROM insurance_dev.rdv.sat_claim
) s ON h.claim_hash_key = s.claim_hash_key AND s._rn = 1
LEFT JOIN insurance_dev.rdv.link_claim_policy lnk0
    ON h.claim_hash_key = lnk0.claim_hash_key
LEFT JOIN insurance_dev.gold.dim_policy d0
    ON lnk0.policy_hash_key = d0.policy_hash_key
```

Example row in `insurance_dev.gold.fact_claims`:

| Column | Value | Role |
|---|---|---|
| `claim_hash_key` | `"a1b2c3d4..."` | surrogate key |
| `claim_id` | `"CLM-001"` | business key |
| `claim_amount` | `4200.0` | measure |
| `policy_hash_key` | `"f7e8d9..."` | FK → `dim_policy` |
| `load_date` | `2024-03-01 10:00:00` | audit |

This row is now queryable from Power BI by joining `fact_claims.policy_hash_key`
to `dim_policy.policy_hash_key`.

---

### 10.6 Complete Call Graph Summary

```
main.main()
│
├─► kafka_to_bronze.run(catalog, env, base_location)
│     ├─ MetadataConfig.__init__()               → loads pipeline_config.yml into memory
│     ├─ MetadataConfig.get_kafka_secrets_config() → returns Kafka secret key names (dict)
│     ├─ get_kafka_creds(dbutils, kafka_cfg)      → KafkaCreds (3 Databricks secret lookups)
│     ├─ MetadataConfig.get_pipeline_topics("kafka_to_bronze")
│     │    └─ MetadataConfig.get_topic(name)      → loads & caches topics/{name}.yml
│     │         └─ MetadataConfig._parse_topic()  → TopicConfig dataclass
│     └─ KafkaBronzeIngester.ingest_topic(topic)  [×6 topics, parallel streams]
│          ├─ KafkaBronzeIngester._read_topic()   → streaming DataFrame (Kafka native schema)
│          ├─ MetadataConfig.bronze_checkpoint()  → "abfss://.../checkpoints/bronze/{topic}"
│          └─ MetadataConfig.bronze_table()       → "insurance_dev.bronze.{topic}"
│
├─► bronze_to_rdv.run(catalog, env, base_location)   [Phase 1]
│     ├─ MetadataConfig.get_pipeline_topics("bronze_to_flatten") → [TopicConfig ...]
│     └─ BronzeFlattener.flatten_topic(topic)         [×6 topics]
│          ├─ schema_registry.get_schema(schema_class) → StructType
│          ├─ MetadataConfig.bronze_table()            → "insurance_dev.bronze.{topic}"
│          ├─ from_json(raw_json, StructType)          → typed columns + _rescued_data
│          └─ BronzeFlattener.write_flatten(df, topic)
│               ├─ MetadataConfig.flatten_checkpoint() → "abfss://.../checkpoints/flatten/{topic}"
│               └─ MetadataConfig.flatten_table()      → "insurance_dev.flatten.{topic}"
│
│                                                       [Phase 2]
│     ├─ MetadataConfig.get_pipeline_topics("flatten_to_rdv") → [TopicConfig ...]
│     └─ RDVBuilder.process_topic(topic)               [×6 topics, sequential]
│          ├─ RDVBuilder.build_hub(topic)
│          │    ├─ RDVBuilder._flatten_df()            → spark.table("flatten.{topic}")
│          │    ├─ F.md5(business_key)                 → hash_key column
│          │    ├─ RDVBuilder._ensure_table()          → create Delta table if missing
│          │    └─ spark.sql("MERGE INTO hub_... INSERT only")
│          ├─ RDVBuilder.build_satellite(topic, sat)   [×N satellites]
│          │    ├─ RDVBuilder._flatten_df()
│          │    ├─ F.md5(concat_ws sat_cols)           → hash_diff column
│          │    └─ spark.sql("MERGE INTO sat_... INSERT + UPDATE on hash_diff change")
│          └─ RDVBuilder.build_link(topic, link)       [×N links]
│               ├─ RDVBuilder._flatten_df()
│               ├─ F.md5(concat_ws all_biz_keys)       → link hash_key
│               ├─ F.md5(each ref key)                 → foreign hash_keys
│               └─ spark.sql("MERGE INTO link_... INSERT only")
│
└─► rdv_to_gold.run(catalog, env, base_location)
      ├─ MetadataConfig.get_gold_schemas()             → loads gold_schemas.yml (dict)
      ├─ StarSchemaBuilder.build_dimension(dim_cfg)    [×3 dimensions]
      │    ├─ MetadataConfig.rdv_table(source_hub)     → "insurance_dev.rdv.hub_{entity}"
      │    ├─ MetadataConfig.rdv_table(source_satellite) → "insurance_dev.rdv.sat_{entity}"
      │    ├─ MetadataConfig.gold_table(table)         → "insurance_dev.gold.dim_{entity}"
      │    └─ spark.sql("CREATE OR REPLACE TABLE ... hub JOIN sat(latest via ROW_NUMBER)")
      └─ StarSchemaBuilder.build_fact(fact_cfg)        [×3 fact tables]
           ├─ MetadataConfig.rdv_table / gold_table    → resolved table names
           └─ spark.sql("CREATE OR REPLACE TABLE ... hub JOIN sat(latest)
                         LEFT JOIN link LEFT JOIN dim [×N dims]")
```
