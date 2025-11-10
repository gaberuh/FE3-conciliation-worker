import json
import os
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, StringType

from app.core.config import logger, settings


class OutputWriterService:
    """
    Serviço responsável por aplicar o schema de saída e escrever o DataFrame
    diretamente no MinIO (via S3A) usando escrita distribuída do Spark.
    """

    def __init__(self, schema_file: str = None):
        self.schema_file = schema_file or os.path.join("app", "models", "output_schema.json")
        self.schema = self._load_schema()
        # O número de partições de shuffle é agora determinado dinamicamente pelo Worker.

    def _load_schema(self) -> Dict[str, Any]:
        """Carrega o esquema de saída de um arquivo JSON."""
        try:
            with open(self.schema_file, "r") as f:
                schema = json.load(f)
            return schema
        except Exception as e:
            logger.error(f"Falha ao carregar output schema: {e}")
            raise

    def apply_schema(self, df: DataFrame, schema_name: str) -> DataFrame:
        """Aplica schema: colunas, tipos e ordenação (Operação Distribuída Spark)"""
        if schema_name not in self.schema:
            logger.warning(f"Nenhum mapping encontrado para {schema_name}, retornando todas as colunas")
            return df

        schema_config = self.schema[schema_name]
        columns: List[str] = schema_config.get("columns", df.columns)
        types: Dict[str, str] = schema_config.get("types", {})
        order_by: List[str] = schema_config.get("order_by", [])

        # 1. Converte tipos (Distribuído)
        for c in columns:
            if c in df.columns and c in types:
                t = types[c].lower()
                if t == "decimal":
                    df = df.withColumn(c, col(c).cast(DecimalType(18, 2)))
                elif t == "string":
                    df = df.withColumn(c, col(c).cast(StringType()))

        # 2. Seleciona e Reordena Colunas (Distribuído)
        df = df.select(*[c for c in columns if c in df.columns])

        # 3. Ordena se houver colunas válidas (Distribuído)
        valid_order_cols = [c for c in order_by if c in df.columns]
        if valid_order_cols:
            df = df.orderBy(*valid_order_cols)

        return df

    def write(self, df: DataFrame, schema_name: str, folder: str = "results", partition_by: List[str] = None,
              target_partitions: int = 1):
        """
        Aplica schema, garante repartitionamento/coalesce e escreve o resultado no MinIO (S3A).

        Args:
            df (DataFrame): DataFrame a ser escrito.
            schema_name (str): Nome do esquema para formatação.
            folder (str): Sub-caminho do S3A para escrita.
            partition_by (List[str]): Colunas para particionamento de pastas S3.
            target_partitions (int): O número exato de partições que o Worker calculou. (Padrão: 1)
        """
        partition_by = partition_by or []
        bucket_name = settings.MINIO_BUCKET_NAME
        s3a_path = f"s3a://{bucket_name}/{folder}"

        logger.info(f"INICIANDO ESCRITA DISTRIBUÍDA: {s3a_path}")

        try:
            # 1. APLICA O SCHEMA FINAL (Tipagem, Seleção e Ordenação)
            df_to_write = self.apply_schema(df, schema_name)

            # 2. AJUSTE DE PARTIÇÕES (Coalesce / Repartition)
            current_partitions = df_to_write.rdd.getNumPartitions()

            if current_partitions != target_partitions:
                if current_partitions > target_partitions:
                    # Coalesce é mais eficiente para REDUZIR partições (de 12 para 1)
                    df_to_write = df_to_write.coalesce(target_partitions)
                    logger.info(
                        f"  -> Coalesce/Redução: Forçado de {current_partitions} para {target_partitions} partições.")
                else:
                    # Repartition é necessário para AUMENTAR partições (de 1 para 5)
                    df_to_write = df_to_write.repartition(target_partitions)
                    logger.info(
                        f"  -> Repartition/Aumento: Forçado de {current_partitions} para {target_partitions} partições.")
            else:
                logger.info(
                    f"  -> Partições atuais ({current_partitions}) já correspondem ao alvo ({target_partitions}). Escrita direta.")

            # 3. ESCRITA
            writer = df_to_write.write.mode("overwrite")

            if partition_by:
                # Note que este 'partitionBy' é para a estrutura de pastas S3
                writer = writer.partitionBy(*partition_by)

            writer.parquet(s3a_path)

            logger.info(f"ESCRITA CONCLUÍDA: Resultados enviados para o caminho S3A: {s3a_path}")

        except Exception as e:
            logger.error(f"Falha na escrita distribuída via S3A em {s3a_path}: {e}")
            raise