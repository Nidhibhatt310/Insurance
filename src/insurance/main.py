import sys

_PIPELINES = {
    "kafka_to_bronze": "insurance.pipelines.kafka_to_bronze",
    "bronze_to_flatten": "insurance.pipelines.bronze_to_flatten",
    "flatten_to_rdv": "insurance.pipelines.flatten_to_rdv",
    "rdv_to_gold": "insurance.pipelines.rdv_to_gold",
}


def main():
    from insurance.utils.logger import configure_logging, get_logger

    configure_logging()
    logger = get_logger(__name__)

    if len(sys.argv) < 5:
        logger.error("Usage: main <pipeline> <catalog> <env> <base_location>")
        logger.error("  pipeline: one of %s", list(_PIPELINES))
        sys.exit(1)

    pipeline, catalog, env, base_location = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

    if pipeline not in _PIPELINES:
        logger.error("Unknown pipeline '%s'. Valid options: %s", pipeline, list(_PIPELINES))
        sys.exit(1)

    import importlib

    mod = importlib.import_module(_PIPELINES[pipeline])
    mod.run(catalog=catalog, env=env, base_location=base_location)


if __name__ == "__main__":
    main()
