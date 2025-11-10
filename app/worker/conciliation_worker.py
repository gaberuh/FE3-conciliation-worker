import math
from typing import Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from app.core.config import logger
from app.pipeline.ingestion_pipeline import (
    read_data_slice,
    load_data_mapping,
    apply_mapping,
    apply_domain_transformations,
    apply_ordering
)
from app.pipeline.reconciliation_pipeline import reconcile_partition_data
from app.services.output_writer_service import OutputWriterService

# --- CONSTANTES PARA CÁLCULO DINÂMICO DE PARTIÇÕES DE ESCRITA ---
TARGET_FILE_SIZE_MB = 128
ESTIMATED_ROW_SIZE_BYTES = 40
TARGET_ROW_COUNT_PER_FILE = int(TARGET_FILE_SIZE_MB * 1024 * 1024 / ESTIMATED_ROW_SIZE_BYTES)


def run_conciliation_worker(spark: SparkSession, payload: Dict[str, Any]):
    """
    Executa o processo de conciliação do FE3 usando Spark.
    Esta função é executada em um thread separado pelo loop de eventos assíncrono.
    """
    # Usamos .get() com um valor padrão para garantir que não haja KeyErrors
    partition_id = payload.get('partition_id', 'N/A')
    data = payload.get('data', {})

    empresa = str(data.get('empresa'))
    conta_contabil = str(data.get('conta_contabil'))
    data_source_path = data.get('data_source_path')

    logger.info(f"Iniciando conciliação para partição: {partition_id}")
    logger.info(f"  -> Empresa: {empresa}, Conta Contábil: {conta_contabil}")
    logger.info(f"  -> Path de origem (Base): {data_source_path}")

    # Validação mínima
    if not (empresa and conta_contabil and data_source_path):
        error_msg = f"Payload incompleto. Requer: empresa, conta_contabil e data_source_path. Recebido: {data}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # ============================================

    try:
        logger.info(f"  -> Tamanho alvo máximo por arquivo de saída: {TARGET_FILE_SIZE_MB} MB")
        logger.info(f"  -> Linhas alvo por arquivo (Estimativa): {TARGET_ROW_COUNT_PER_FILE}")

        # Inicializa o serviço de escrita e carrega os mappings
        writer_service = OutputWriterService()
        data_mapping = load_data_mapping()
        DATASET_NAME = "lancamentos"  # Nome da chave no data_mapping.json

        # 1. LEITURA DA PARTIÇÃO ESPECÍFICA
        df_partition = read_data_slice(
            spark=spark,
            base_path=data_source_path,
            empresa=empresa,
            conta_contabil=conta_contabil
        )

        # 2. VERIFICAÇÃO E MAPPING
        if df_partition.isEmpty():
            logger.warning(f"DataFrame vazio para a partição {partition_id}. Nada para conciliar.")
            return

        logger.info(f"Partição carregada: {df_partition.count()} linhas. Iniciando pipeline...")

        # Aplica Mapping (Seleção de colunas)
        df_mapped = apply_mapping(df_partition, DATASET_NAME, data_mapping)

        # Aplica Transformações de Domínio (Tipagem, Uppercase/Trim)
        df_transformed = apply_domain_transformations(df_mapped)

        # Aplica Ordenação (Se necessário para conciliação ou visualização)
        df_transformed_ordered = apply_ordering(df_transformed, DATASET_NAME, data_mapping)

        # 3. EXECUTA A LÓGICA DE CONCILIAÇÃO
        df_reconciled = reconcile_partition_data(
            df_transformed=df_transformed_ordered,
            writer_service=writer_service
        )

        # --- CÁLCULO DINÂMICO DO NÚMERO DE PARTIÇÕES PARA ESCRITA ---

        # A. Força a contagem final das linhas (AÇÃO CARA, mas necessária para o cálculo de tamanho)
        final_row_count = df_reconciled.count()
        logger.info(f"  -> Contagem final de linhas no DataFrame reconciliado: {final_row_count}")

        # B. Calcula o número de partições (usando teto - ceil)
        if final_row_count == 0:
            required_partitions = 1
        else:
            required_partitions = math.ceil(final_row_count / TARGET_ROW_COUNT_PER_FILE)
            required_partitions = max(1, required_partitions)  # Garante que é no mínimo 1

        logger.info(f"  -> Partições de escrita calculadas dinamicamente: {required_partitions}")

        # 4. ESCRITA DO RESULTADO FINAL NO MINIO/S3A
        output_folder = "reconciliation_results/conciliacao_lancamentos"

        # Criando as colunas de partição de output, se ainda não existirem
        df_reconciled = df_reconciled.withColumn(
            "empresa_part", f.col("empresa")
        ).withColumn(
            "conta_contabil_part", f.col("conta_contabil")
        )

        # Passa o número de partições calculado dinamicamente
        writer_service.write(
            df=df_reconciled,
            schema_name=DATASET_NAME,
            folder=output_folder,
            partition_by=["empresa_part", "conta_contabil_part"],
            target_partitions=required_partitions
        )

        logger.info(f"Conciliação concluída e resultados salvos com sucesso para partição: {partition_id}")

    except Exception as e:
        logger.exception(f"Erro fatal durante o processamento Spark para partição {partition_id}: {e}")
        # A exceção será propagada para o rabbitmq_consumer para ser rejeitada e enviada para a DLQ
        raise