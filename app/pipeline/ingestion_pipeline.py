import json
import os
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import trim, upper, col
from pyspark.sql.types import DecimalType

from app.core.config import settings, logger


# --- Utilitário de Mapeamento ---
def load_data_mapping(file_path: str = os.path.join("app", "models", "data_mapping.json")) -> Dict[str, Any]:
    """Carrega o arquivo de mapeamento de dados JSON."""
    try:
        with open(file_path, "r") as f:
            mapping = json.load(f)
        return mapping
    except Exception as e:
        logger.error(f"Falha ao carregar data mapping de {file_path}: {e}")
        raise


# --- Utilitário de Caminho ---
def _build_full_s3a_path(base_path: str, bucket_name: str) -> str:
    """
    Constrói o caminho S3A completo para leitura.
    Se o base_path já for um URL S3A completo, ele é retornado diretamente.
    """
    if base_path.startswith("s3a://"):
        return base_path

    # Remove barras iniciais do base_path e constrói o caminho com o bucket
    return f"s3a://{bucket_name}/{base_path.lstrip('/')}"


# --- Funções de I/O ---
def read_data_slice(spark: SparkSession, base_path: str,
                    empresa: str, conta_contabil: str) -> DataFrame:
    """
    Lê uma fatia específica de dados (partição) usando Predicate Pushdown.
    O 'base_path' é o caminho raiz do dataset particionado (e.g., s3a://bucket/data/).
    """
    # CHAMA O UTILITÁRIO CORRIGIDO
    url_to_read = _build_full_s3a_path(base_path, settings.MINIO_BUCKET_NAME)
    logger.info(f"  -> Lendo partição: Empresa={empresa}, Conta={conta_contabil} em {url_to_read}")

    try:
        df = (
            spark.read
            .option("mergeSchema", "false")
            .option("partitionColumnTypeInference.enabled", "false")
            .parquet(url_to_read)
            # FILTRO CRÍTICO: Partition Pruning / Predicate Pushdown
            .where((col("empresa") == empresa) & (col("conta_contabil") == conta_contabil))
        )

        total_rows = df.count()
        logger.info(f"  -> LEITURA CONCLUÍDA: {total_rows} registros.")
        return df

    except Exception as e:
        logger.error(f"Erro crítico na leitura da fatia de dados em {url_to_read}: {e}")
        raise


# --- Funções de Transformação ---
def apply_mapping(df: DataFrame, dataset_name: str, mapping: dict) -> DataFrame:
    """Seleciona colunas com base no mapping."""
    if dataset_name not in mapping:
        logger.warning(f"Nenhum mapping encontrado para {dataset_name}, retornando todas as colunas")
        return df

    dataset_mapping = mapping[dataset_name]
    columns = dataset_mapping.get("columns", []) if isinstance(dataset_mapping, dict) else dataset_mapping
    valid_columns = [c for c in columns if c in df.columns]

    logger.info(f"  -> Aplicando mapping: {valid_columns}")
    return df.select(*valid_columns)


def apply_domain_transformations(df: DataFrame) -> DataFrame:
    """Aplica transformações de domínio (tipagem, limpeza de strings)."""
    # Tipagem Decimal
    financial_columns = ["valor", "valor_net"]
    for col_name in financial_columns:
        if col_name in df.columns:
            # Usando DecimalType(18, 2)
            df = df.withColumn(col_name, col(col_name).cast(DecimalType(18, 2)))

    # Limpeza de String (Trim e Uppercase)
    string_columns = ["cliente_nome", "empresa", "conta_contabil", "transactionID"]
    for col_name in string_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, upper(trim(df[col_name])))

    return df


def apply_ordering(df: DataFrame, dataset_name: str, mapping: dict) -> DataFrame:
    """Aplica ordenação no DataFrame, se definida no mapping."""
    # O mapping deve ser o dicionário completo retornado por load_data_mapping
    order_cols = mapping.get(dataset_name, {}).get("order_by", None)
    if order_cols:
        valid_order_cols = [c for c in order_cols if c in df.columns]
        if valid_order_cols:
            df = df.orderBy(*valid_order_cols)
    return df