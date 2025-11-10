import asyncio
import logging
from typing import Any
from pyspark.sql import SparkSession
from app.core.config import logger
from app.core.spark_session import get_spark_session
from app.services.rabbitmq_consumer import consume_messages
from app.worker.conciliation_worker import run_conciliation_worker

logger = logging.getLogger("reconciler-orchestrator.main")


async def _run_conciliation_worker(spark_session: SparkSession, msg_data: dict) -> Any:
    """
    Executa a sua função de worker de forma assíncrona.

    Este é o ponto CRÍTICO: usa loop.run_in_executor para executar a tarefa
    bloqueante (Spark) em um ThreadPoolExecutor, liberando o loop de I/O
    do aio-pika para continuar recebendo mensagens.
    """
    logger.info(f"Enviando dados para o Conciliation Worker: {list(msg_data.keys())}")

    # Executa a função síncrona do worker em um thread separado
    loop = asyncio.get_event_loop()

    await loop.run_in_executor(
        None,  # Usa o ThreadPoolExecutor padrão
        run_conciliation_worker,
        spark_session,
        msg_data
    )
    logger.info("Conciliation Worker concluído.")


def main():
    """
    Função principal que inicializa o Spark e inicia o consumidor do RabbitMQ.
    """
    logger.info("================ INICIANDO FE3 CONCILIATION WORKER ================")

    spark_session = get_spark_session()

    try:
        # Passamos a session do Spark e a função wrapper assíncrona do worker
        asyncio.run(consume_messages(spark_session, _run_conciliation_worker))

    except KeyboardInterrupt:
        logger.info("Worker interrompido pelo usuário (CTRL+C).")
    except Exception as e:
        logger.critical(f"Erro fatal no worker principal: {e}", exc_info=True)
    finally:
        if spark_session:
            spark_session.stop()
            logger.info("SparkSession encerrada.")
        logger.info("================ FE3 CONCILIATION WORKER ENCERRADO ================")


if __name__ == "__main__":
    main()