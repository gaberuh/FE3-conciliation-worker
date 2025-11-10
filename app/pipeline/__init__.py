import gc
from typing import Dict, Any

from pyspark.sql import SparkSession
from app.core.config import logger, settings

# Importamos os módulos da arquitetura existente
from app.services.output_writer_service import OutputWriterService
from app.pipeline import ingestion_pipeline as ing
from app.pipeline import reconciliation_pipeline as rec


def run_conciliation_worker(spark: SparkSession, message_payload: Dict[str, Any]):
    """
    Worker principal responsável por processar UMA única partição de conciliação,
    cujas chaves são recebidas via mensagem de fila.

    Se ocorrer uma falha crítica no Spark (ex: erro de I/O), a exceção é relançada
    para que o consumidor RabbitMQ possa NACK.

    :param spark: SparkSession inicializada.
    :param message_payload: Payload da mensagem contendo as chaves da partição.
    """

    # 1. Extrair chaves da partição do payload
    message_data = message_payload.get("data", {})

    try:
        # Chaves de particionamento (devem ser strings para o Spark WHERE)
        empresa = str(message_data["empresa"])
        conta_contabil = str(message_data["conta_contabil"])
        base_path = settings.PARTITION_BASE_PATH

    except KeyError as e:
        logger.error(f"❌ Payload inválido. Chave '{e.args[0]}' ausente no 'data': {message_payload}")
        # Lança ValueError para ser capturado no RabbitMQ Consumer e ser NACK (DLQ)
        raise ValueError("Chave de partição essencial ausente no payload.")

    log_prefix = f"[WORKER] Empresa: {empresa} | Conta: {conta_contabil}"
    logger.info(f"\n{log_prefix} - INICIANDO PROCESSAMENTO DE PARTIÇÃO (via mensagem).")

    raw_df = None
    df_transformed = None
    df_reconciliado = None

    try:
        # 2. Carregar o mapping e o serviço de escrita
        mapping = ing.load_data_mapping()
        writer_service = OutputWriterService()

        # --- FASE 1: INGESTÃO E TRANSFORMAÇÃO ---

        # 2.1. Leitura Otimizada (Predicate Pushdown)
        raw_df = ing.read_data_slice(spark, base_path, empresa, conta_contabil)

        if raw_df.count() == 0:
            logger.warning(f"{log_prefix} - DataFrame vazio. Retorno bem-sucedido.")
            return  # Retorna sem erro, resultando em ACK

        # 2.2. Aplicação do pipeline de transformação
        df_mapped = ing.apply_mapping(raw_df, "lancamentos_cj", mapping)
        df_transformed = ing.apply_domain_transformations(df_mapped)
        df_transformed = ing.apply_ordering(df_transformed, "lancamentos_cj", mapping)

        # --- FASE 2: CONCILIAÇÃO ---

        # 3.1. Executa a conciliação
        df_reconciliado = rec.reconcile_partition_data(df_transformed, writer_service)

        # 4. Escrita: Salvar o resultado final desta partição
        output_folder = f"results_by_partition/empresa={empresa}/conta_contabil={conta_contabil}"
        writer_service.write(
            df_reconciliado,
            schema_name="reconciliation_final_result",
            folder=output_folder,
            partition_by=None
        )

        logger.info(f"{log_prefix} - ✅ SUCESSO: Partição conciliada e escrita.")

    except Exception as e:
        logger.error(f"{log_prefix} - ❌ FALHA CRÍTICA NO PROCESSAMENTO do worker: {e}", exc_info=True)
        # Relança a exceção para que o RabbitMQ Consumer possa NACK (enviar para DLQ)
        raise

    finally:
        # 5. Limpeza Explícita de Memória e Cache
        try:
            if raw_df is not None: raw_df.unpersist()
            if df_transformed is not None: df_transformed.unpersist()
            if df_reconciliado is not None: df_reconciliado.unpersist()

            del raw_df
            del df_transformed
            del df_reconciliado

        except NameError:
            pass

        gc.collect()
        spark.catalog.clearCache()
        logger.info(f"{log_prefix} - RECURSOS SPARK/PYTHON LIBERADOS.")