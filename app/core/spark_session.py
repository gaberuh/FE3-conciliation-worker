from pyspark.sql import SparkSession

from app.core.config import settings, logger

# Versões estáveis e compatíveis com Spark 3.5.1
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.568"

_spark_session = None

# Constante para os Timeouts em milissegundos
TIMEOUT_MS_STR = "60000"
MAX_ATTEMPTS_STR = "10"


def get_spark_session(app_name="reconciler-orchestrator") -> SparkSession:
    """
    Retorna uma instância singleton da SparkSession configurada para o pipeline,
    com robustez para S3A (MinIO) usando SSL (HTTPS) ou não, baseada nas settings.
    """
    global _spark_session
    if _spark_session is None:
        logger.info("Inicializando SparkSession...")

        # --- 1. Determina o prefixo do Endpoint e o status do SSL ---
        # CORREÇÃO: Usando settings.MINIO_SECURE, conforme especificado no config.py do usuário.
        is_secure = settings.MINIO_SECURE
        endpoint_prefix = "https://" if is_secure else "http://"
        endpoint = f"{endpoint_prefix}{settings.MINIO_ENDPOINT}"

        logger.info(f"  -> Conectando ao MinIO em: {endpoint}")
        logger.info(f"  -> SSL Habilitado: {'Sim' if is_secure else 'Não'}")

        # --- 2. Inicializa a SparkSession e Dependências (Injeção via spark.hadoop.) ---
        _spark_session = (  # Resultado da builder é atribuído aqui
            SparkSession.builder
            .appName(app_name)
            .master(settings.SPARK_MASTER)
            .config("spark.sql.parquet.compression.codec", settings.PARQUET_COMPRESSION)
            .config("spark.sql.shuffle.partitions", "120")
            .config("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "2")
            .config("spark.memory.storageFraction", "0.6")
            .config(
                "spark.jars.packages",
                f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION},"
                f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}"
            )

            # --- CONFIGURAÇÕES S3A/MINIO ---
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")  # Redireciona s3:// para s3a

            # Configuração de SSL (baseada na variável MINIO_SECURE)
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(is_secure).lower())
            .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Necessário para MinIO

            # ** FIX: DESABILITAR VERIFICAÇÃO DE HOST SSL para MinIO customizado (muitas vezes necessário) **
            .config("spark.hadoop.fs.s3a.disable.ssl.host.check", "true")

            # Novo: Simplificando I/O para evitar erro de inicialização do client (Reforço)
            .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "normal")
            .config("spark.hadoop.fs.s3a.client.factory.impl", "org.apache.hadoop.fs.s3a.DefaultS3ClientFactory")

            # Performance
            .config("spark.hadoop.fs.s3a.multipart.size", str(128 * 1024 * 1024))
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")

            # Timeouts (TODAS as propriedades forçadas para milissegundos)
            .config("spark.hadoop.fs.s3a.connection.timeout", TIMEOUT_MS_STR)
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", TIMEOUT_MS_STR)
            .config("spark.hadoop.fs.s3a.socket.timeout", TIMEOUT_MS_STR)
            .config("spark.hadoop.fs.s3a.read.timeout", TIMEOUT_MS_STR)
            .config("spark.hadoop.fs.s3a.write.timeout", TIMEOUT_MS_STR)
            .config("spark.hadoop.fs.s3a.timeout", TIMEOUT_MS_STR)
            .config("spark.hadoop.fs.s3a.attempts.maximum", MAX_ATTEMPTS_STR)

            .getOrCreate()
        )

        # ** FIX: ADICIONANDO GUARD-CLAUSE **
        if _spark_session is None:
            logger.error(
                "Falha CRÍTICA ao inicializar a SparkSession. Verifique logs detalhados da JVM para causas (e.g., MinIO/SSL/Credenciais).")
            raise RuntimeError(
                "A SparkSession não foi inicializada. Verifique a configuração S3A e logs de dependências.")

        # --- 3. Última Defesa: Sobrescrever Timeouts via hadoopConfiguration() ---
        # Garantindo que nenhuma configuração antiga sobrescreva as nossas.
        hadoop_conf = _spark_session._jsc.hadoopConfiguration()

        # Injeção de Timeouts com valores em MS
        hadoop_conf.set("fs.s3a.connection.timeout", TIMEOUT_MS_STR)
        hadoop_conf.set("fs.s3a.connection.establish.timeout", TIMEOUT_MS_STR)
        hadoop_conf.set("fs.s3a.socket.timeout", TIMEOUT_MS_STR)
        hadoop_conf.set("fs.s3a.read.timeout", TIMEOUT_MS_STR)
        hadoop_conf.set("fs.s3a.write.timeout", TIMEOUT_MS_STR)
        hadoop_conf.set("fs.s3a.timeout", TIMEOUT_MS_STR)
        hadoop_conf.set("fs.s3a.attempts.maximum", MAX_ATTEMPTS_STR)

        # Garante que o SSL está configurado corretamente no objeto Java
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(is_secure).lower())
        hadoop_conf.set("fs.s3a.disable.ssl.host.check", "true")

        # Reforçando o fadvise
        hadoop_conf.set("fs.s3a.experimental.input.fadvise", "normal")

        # --- 4. Configura Spark log level ---
        _spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        log_level = "WARN" if settings.ENVIRONMENT == "production" else settings.LOG_LEVEL.upper()
        _spark_session.sparkContext.setLogLevel(log_level)
        logger.info(f"Spark Log Level configurado para: {log_level}")

        # --- 5. Silenciar loggers específicos do Spark/Hadoop ---
        from py4j.java_gateway import java_import
        java_import(_spark_session._jvm, "org.apache.log4j.Logger")
        java_import(_spark_session._jvm, "org.apache.log4j.Level")
        level_warn = _spark_session._jvm.Level.WARN

        loggers_to_silence = [
            "org.apache.spark",
            "org.apache.spark.sql",
            "org.apache.spark.sql.execution.FileScanRDD",
            "org.apache.spark.sql.execution.datasources.FileSourceScanExec",
            "org.apache.spark.sql.catalyst",
            "org.apache.spark.sql.execution.CodeGenerator",
            "org.apache.hadoop",
            "org.apache.hadoop.fs.s3a.S3AInputStream",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "org.apache.hadoop.fs.FileSystem",
        ]
        for name in loggers_to_silence:
            Logger = _spark_session._jvm.Logger.getLogger(name)
            Logger.setLevel(level_warn)

        logger.info("SparkSession inicializada e logs silenciados com sucesso.")

    return _spark_session