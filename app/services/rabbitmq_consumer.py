import asyncio
import json
import logging
from typing import Callable, Any
import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pyspark.sql import SparkSession

# O logger deve ser obtido de app.core.config para consistência
try:
    from app.core.config import settings, logger
except ImportError:
    # Fallback simples de logging (idealmente, você usaria o logger do seu projeto)
    logger = logging.getLogger("reconciler-orchestrator")
    logger.setLevel(logging.INFO)


# --- Funções de Ajuda Assíncronas ---

async def _process_message(
        spark_session: SparkSession,
        message: AbstractIncomingMessage,
        spark_worker_func: Callable[[SparkSession, dict], Any]
):
    """
    Função principal assíncrona para tratar cada mensagem recebida.
    Usa o contexto message.process(requeue=False) para controle automático de ACK/NACK.
    """
    try:
        async with message.process(requeue=False):
            # 1. Parsing da Mensagem
            msg_data = json.loads(message.body.decode('utf-8'))
            logger.info(f"[RabbitMQ Async] Mensagem recebida (Tag: {message.delivery_tag}) para processamento: {msg_data}")

            # 2. Executa o Worker do Spark
            # Esta função deve ser um wrapper async que executa o código Spark em um executor de thread
            await spark_worker_func(spark_session, msg_data)

            # O ACK é enviado ao sair do bloco 'async with' sem exceções.
            logger.info(
                f"[RabbitMQ Async] Mensagem processada com SUCESSO e ACK para delivery_tag: {message.delivery_tag}")

    except Exception as e:
        # A exceção aqui apenas loga o erro. O NACK (requeue=False) já foi
        # executado automaticamente pelo 'async with message.process(requeue=False):'
        logger.error(
            f"[RabbitMQ Async] Erro no processamento da mensagem, REJECT(requeue=False) enviado para DLX/DLQ: {e}",
            exc_info=True
        )


# --- Função Principal de Consumo ---

async def consume_messages(spark_session: SparkSession, spark_worker_func: Callable[[SparkSession, dict], Any]):
    """
    Conecta ao RabbitMQ, configura DLQ/DLX e inicia o consumo de mensagens.
    """

    amqp_url = (
        f"amqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASSWORD}@"
        f"{settings.RABBITMQ_HOST}:{settings.RABBITMQ_PORT}/{settings.RABBITMQ_VHOST.lstrip('/')}"
    )

    connection = None
    try:
        # connect_robust trata automaticamente reconexões
        connection = await aio_pika.connect_robust(amqp_url)
        logger.info("[RabbitMQ Async] Conexão robusta estabelecida.")

        channel_to_use = await connection.channel()
        await channel_to_use.set_qos(prefetch_count=1)

        # --- DEFINIÇÃO E DECLARAÇÃO DE DLQ/DLX ---

        queue_name = settings.RABBITMQ_QUEUE_NAME
        dlx_exchange_name = f"{queue_name}.dlx"
        dlq_queue_name = f"{queue_name}.dlq"
        # Mantendo o routing key como o nome da fila principal, como você fez.
        dl_routing_key = queue_name

        # 1. Declarar o Dead Letter Exchange (DLX)
        await channel_to_use.declare_exchange(
            dlx_exchange_name,
            type='direct',
            durable=True
        )

        # 2. Declarar a DLQ e Bind
        dlq_queue_object = await channel_to_use.declare_queue(
            dlq_queue_name,
            durable=True
        )

        await dlq_queue_object.bind(
            exchange=dlx_exchange_name,
            routing_key=dl_routing_key
        )

        # 3. Declarar a fila principal COM argumentos DLX
        queue = await channel_to_use.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx_exchange_name,
                "x-dead-letter-routing-key": dl_routing_key
            }
        )

        logger.info(f"[RabbitMQ Async] Fila principal '{queue_name}' e DLQ/DLX configuradas.")

        # Inicia o consumo, passando o SparkSession e o Worker wrapper
        await queue.consume(
            lambda message: _process_message(spark_session, message, spark_worker_func),
            no_ack=False
        )

        logger.info(f"[RabbitMQ Async] Iniciando consumo da fila '{queue_name}'.")

        # Mantém o loop de eventos ativo
        await asyncio.Future()

    except aio_pika.exceptions.ChannelPreconditionFailed as e:
        logger.error(
            f"[RabbitMQ Async] Erro CRÍTICO de pré-condição. A fila '{settings.RABBITMQ_QUEUE_NAME}' já existe com argumentos DLX diferentes. SOLUÇÃO: Delete a fila no console do RabbitMQ e reinicie o worker."
        )
        raise
    except Exception as e:
        logger.error(f"[RabbitMQ Async] Erro inesperado no consumidor: {e}", exc_info=True)
        raise
    finally:
        if connection and not connection.is_closed:
            await connection.close()
            logger.info("[RabbitMQ Async] Conexão RabbitMQ fechada.")