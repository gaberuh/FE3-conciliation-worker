import os
import logging
from dotenv import load_dotenv

# Carrega variáveis de ambiente do .env
load_dotenv()

class Settings:
    """
    Configurações da aplicação carregadas a partir de variáveis de ambiente.
    Fornece valores padrão para facilitar o desenvolvimento local.
    """

    # Logging e Ambiente
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "homol").lower()

    # Agendamento
    JOB_INTERVAL_SECONDS: int = int(os.getenv("JOB_INTERVAL_SECONDS", "3600"))  # 1 hora padrão

    # MinIO / S3A
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_BUCKET_NAME: str = os.getenv("MINIO_BUCKET_NAME", "conciliation-data")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "True").lower() == "true"

    # Path de Leitura (Onde os dados de transação estão particionados)
    DATA_INPUT_PATH: str = f"s3a://{MINIO_BUCKET_NAME}/mock/lancamentos_cj_particionado_unico"

    # Spark
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    PARQUET_COMPRESSION: str = os.getenv("PARQUET_COMPRESSION", "snappy")

    # RabbitMQ
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_VHOST: str = os.getenv("RABBITMQ_VHOST", "/")

    # Configurações de Fila Principal e DLQ/DLX (Novo)
    RABBITMQ_QUEUE_NAME: str = os.getenv("QUEUE_NAME", "conciliation_lancamentos_groups")
    RABBITMQ_DLQ_NAME: str = os.getenv("DLQ_NAME", "conciliation_lancamentos_groups.dlq")
    RABBITMQ_DLX_NAME: str = os.getenv("DLX_NAME", "conciliation_lancamentos_groups.dlx")

    # Chaves de Partição
    PARTITION_KEYS: list[str] = ["empresa", "conta_contabil"]


settings = Settings()

# --- Logger centralizado ---
logger = logging.getLogger("reconciler-orchestrator")
logger.setLevel(settings.LOG_LEVEL.upper())

formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d %(levelname)s [%(name)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)

# StreamHandler (console)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)