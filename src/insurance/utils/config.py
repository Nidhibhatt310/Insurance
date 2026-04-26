from __future__ import annotations

import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_DEFAULT_CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class HubConfig:
    name: str
    table: str
    business_key: str
    hash_key: str


@dataclass
class SatelliteConfig:
    name: str
    table: str
    columns: list[str]


@dataclass
class LinkForeignRef:
    hub_table: str
    key: str
    hash_key: str


@dataclass
class LinkConfig:
    name: str
    table: str
    hash_key: str
    foreign_refs: list[LinkForeignRef]


@dataclass
class RDVTopicConfig:
    hub: Optional[HubConfig]
    satellites: list[SatelliteConfig]
    links: list[LinkConfig]


@dataclass
class TopicConfig:
    name: str
    kafka_topic: str
    source_type: str
    schema_class: str
    bronze_table: str
    flatten_table: str
    rdv: RDVTopicConfig


@dataclass
class KafkaCreds:
    bootstrap_server: str
    sasl_key: str
    sasl_secret: str


class MetadataConfig:
    """
    Central metadata loader. All pipeline engines receive an instance of this class
    and resolve every table name, checkpoint path, and secret reference through it.
    No hardcoded topic names, catalog paths, or secret keys exist anywhere else.
    """

    def __init__(
        self,
        catalog: str,
        env: str,
        base_location: str,
        config_dir: Optional[str] = None,
    ):
        self.catalog = catalog
        self.env = env
        self.base_location = base_location
        self._config_dir = Path(config_dir) if config_dir else _DEFAULT_CONFIG_DIR
        self._pipeline_cfg: dict = self._load_yaml("pipeline_config.yml")
        self._topics: dict[str, TopicConfig] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_yaml(self, relative_path: str) -> dict:
        with open(self._config_dir / relative_path) as f:
            return yaml.safe_load(f)

    def _parse_topic(self, raw: dict) -> TopicConfig:
        rdv_raw = raw.get("rdv", {})

        hub = HubConfig(**rdv_raw["hub"]) if rdv_raw.get("hub") else None

        satellites = [SatelliteConfig(**s) for s in rdv_raw.get("satellites", [])]

        links = []
        for link in rdv_raw.get("links", []):
            refs = [LinkForeignRef(**r) for r in link.get("foreign_refs", [])]
            links.append(
                LinkConfig(
                    name=link["name"],
                    table=link["table"],
                    hash_key=link["hash_key"],
                    foreign_refs=refs,
                )
            )

        return TopicConfig(
            name=raw["name"],
            kafka_topic=raw["kafka_topic"],
            source_type=raw["source_type"],
            schema_class=raw["schema_class"],
            bronze_table=raw["bronze"]["table"],
            flatten_table=raw["flatten"]["table"],
            rdv=RDVTopicConfig(hub=hub, satellites=satellites, links=links),
        )

    # ------------------------------------------------------------------
    # Topic access
    # ------------------------------------------------------------------

    def get_topic(self, name: str) -> TopicConfig:
        if name not in self._topics:
            self._topics[name] = self._parse_topic(self._load_yaml(f"topics/{name}.yml"))
        return self._topics[name]

    def get_pipeline_topics(self, pipeline: str) -> list[TopicConfig]:
        names = self._pipeline_cfg["pipelines"][pipeline]["topics"]
        return [self.get_topic(n) for n in names]

    # ------------------------------------------------------------------
    # Global config access
    # ------------------------------------------------------------------

    def get_kafka_secrets_config(self) -> dict:
        return self._pipeline_cfg["kafka"]

    def get_gold_schemas(self) -> dict:
        return self._load_yaml("gold_schemas.yml")

    def get_dq_rules(self) -> dict:
        return self._load_yaml("dq_rules.yml")

    # ------------------------------------------------------------------
    # Unity Catalog table name helpers  (catalog.layer.table)
    # ------------------------------------------------------------------

    def bronze_table(self, topic: TopicConfig) -> str:
        return f"{self.catalog}.bronze.{topic.bronze_table}"

    def flatten_table(self, topic: TopicConfig) -> str:
        return f"{self.catalog}.flatten.{topic.flatten_table}"

    def rdv_table(self, table_name: str) -> str:
        return f"{self.catalog}.rdv.{table_name}"

    def gold_table(self, table_name: str) -> str:
        return f"{self.catalog}.gold.{table_name}"

    # ------------------------------------------------------------------
    # Checkpoint path helpers
    # ------------------------------------------------------------------

    def bronze_checkpoint(self, topic: TopicConfig) -> str:
        return f"{self.base_location}/checkpoints/bronze/{topic.name}"

    def flatten_checkpoint(self, topic: TopicConfig) -> str:
        return f"{self.base_location}/checkpoints/flatten/{topic.name}"
