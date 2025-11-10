from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

from app.core.config import logger
from app.services.output_writer_service import OutputWriterService


def reconcile_partition_data(df_transformed: DataFrame, writer_service: OutputWriterService) -> DataFrame:
    """
    Executa o pipeline de conciliação chaveada por transação (TransactionID).

    :param df_transformed: DataFrame de transações transformado.
    :param writer_service: Serviço de escrita (mantido para assinatura).
    :return: DataFrame com o status de conciliação final e conciliation_id.
    """
    logger.info("  >> Iniciando Conciliação Chaveada por TransactionID.")

    grouping_keys = ["empresa", "conta_contabil", "transactionID"]

    # 1. Agregação: Calcula o saldo para cada grupo
    df_transaction_summary = df_transformed.groupBy(*grouping_keys).agg(
        f.round(f.sum(col("valor_net")), 2).alias("saldo_transacional")
    ).withColumn(
        "is_reconciled",
        col("saldo_transacional") == lit(0.00)
    ).withColumn(
        # Gera o UUID apenas se o saldo do grupo for zero
        "new_conciliation_id",
        f.when(col("is_reconciled"), f.expr("uuid()")).otherwise(lit(None))
    ).select(
        *grouping_keys,
        "saldo_transacional",
        "new_conciliation_id"
    )

    # 2. Join: Junta as informações de conciliação ao DataFrame de transações original
    df_reconciled = df_transformed.join(
        f.broadcast(df_transaction_summary),
        on=grouping_keys,
        how="left"
    )

    # 3. Aplica o Status e a Coniliation ID final
    df_reconciled = df_reconciled.withColumn(
        "reconciliation_status",
        f.when(col("new_conciliation_id").isNotNull(), lit("CONCILIADO"))
        .otherwise(lit("PENDENTE")).cast(StringType())
    ).withColumn(
        "conciliation_id",
        col("new_conciliation_id")
    ).drop(
        "saldo_transacional", "new_conciliation_id"
    )

    logger.info("  >> Conciliação por TransactionID concluída.")
    return df_reconciled