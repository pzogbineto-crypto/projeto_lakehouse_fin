import os
import sys
import urllib.request

# ==============================================================================
# 🛠️ AMBIENTE WINDOWS: AUTOMAÇÃO E BLINDAGEM DO HADOOP (WINUTILS)
# ==============================================================================
HADOOP_DIR = r"C:\hadoop"
BIN_DIR = os.path.join(HADOOP_DIR, "bin")

# Cria as pastas físicas no C:\ de forma autônoma
if not os.path.exists(BIN_DIR):
    try:
        os.makedirs(BIN_DIR, exist_ok=True)
    except PermissionError:
        print("❌ ERRO DE PERMISSÃO: Por favor, abra o seu VS Code ou PowerShell como ADMINISTRADOR.")
        sys.exit(1)

# Faz o download dos emuladores nativos do Windows se não existirem
winutils_path = os.path.join(BIN_DIR, "winutils.exe")
hadoop_dll_path = os.path.join(BIN_DIR, "hadoop.dll")

if not os.path.exists(winutils_path):
    print("📥 Baixando winutils.exe para compatibilidade Windows...")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe", 
        winutils_path
    )

if not os.path.exists(hadoop_dll_path):
    print("📥 Baixando hadoop.dll para compatibilidade Windows...")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll", 
        hadoop_dll_path
    )
# Injeta as variáveis de ambiente direto na memória do processo atual
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = BIN_DIR + os.pathsep + os.environ["PATH"]
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\pzogb\OneDrive\Documentos\Projetos Pessoais - Profissionais\enterprise_lakehouse\gcp-key.json"

# ==============================================================================
# 📦 IMPORTS DO ECOSSISTEMA SPARK E GOOGLE CLOUD
# ==============================================================================
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from google.cloud import pubsub_v1

# ==============================================================================
# 🏢 CONFIGURAÇÕES DA INFRAESTRUTURA LAKEHOUSE
# ==============================================================================
PROJECT_ID = "enterprise-finance-lakehouse"
SUBSCRIPTION_ID = "realtime-transactions-sub"
BUCKET_GCS = "gs://enterprise-finance-lakehouse-pedro-zogbi"
ICEBERG_CATALOG = "gcs_catalog"
ICEBERG_DATABASE = "finance_db"
ICEBERG_TABLE = "transactions_raw"

def init_spark():
    """Inicializa a SparkSession local usando Jars Nativos para evitar Conflito de Guava"""
    return (SparkSession.builder
        .appName("Enterprise-Finance-Lakehouse-Ingestion")
        .master("local[*]")
        
        # 👇 ADICIONE ESTAS DUAS LINHAS PARA BLINDAR A REDE NO WINDOWS
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        
        # A MÁGICA: Em vez de baixar da internet, aponta pro arquivo .jar isolado na sua pasta raiz
        .config("spark.jars", "gcs-connector.jar") 
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3") 
        
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", f"{BUCKET_GCS}/lakehouse/")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("google.cloud.auth.service.account.enable", "true")
        .config("google.cloud.auth.service.account.json.keyfile", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
        .getOrCreate())

def create_pubsub_subscription():
    """Garante que a Assinatura (Subscription) do Pub/Sub exista no GCP"""
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = f"projects/{PROJECT_ID}/topics/realtime-transactions"
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    try:
        subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
        print(f"✅ Assinatura Pub/Sub criada com sucesso: {SUBSCRIPTION_ID}")
    except Exception:
        print(f"🔄 Assinatura Pub/Sub já existente. Conectando...")
    return subscription_path

def main():
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN") # Limpa o excesso de logs informativos na tela
    subscription_path = create_pubsub_subscription()
    
    # 1. Definição Estrita do Schema da Transação Financeira
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("sender_account", StringType(), False),
        StructField("receiver_account", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False)
    ])

    # 2. Criação do Banco de Dados e da Tabela Iceberg no GCS (se não existirem)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE} (
            transaction_id STRING,
            timestamp STRING,
            sender_account STRING,
            receiver_account STRING,
            transaction_type STRING,
            amount DOUBLE
        ) USING iceberg
        PARTITIONED BY (transaction_type)
    """)
    print("🏛️ Tabela Apache Iceberg validada/criada no GCS com sucesso!")

    subscriber = pubsub_v1.SubscriberClient()
    print("🎧 Aguardando transações do Pub/Sub em tempo real... (Pressione Ctrl+C para parar)")

    while True:
        try:
            # Aumentamos o timeout para 10 segundos para dar fôlego em oscilações de internet
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 100}, 
                timeout=10.0
            )
            
            messages = []
            ack_ids = []
            
            for msg in response.received_messages:
                data_dict = json.loads(msg.message.data.decode("utf-8"))
                row_tuple = (
                    str(data_dict.get("transaction_id")),
                    str(data_dict.get("timestamp")),
                    str(data_dict.get("sender_account")),
                    str(data_dict.get("receiver_account")),
                    str(data_dict.get("transaction_type")),
                    float(data_dict.get("amount", 0.0))
                )
                messages.append(row_tuple)
                ack_ids.append(msg.ack_id)
            
            if messages:
                print(f"📦 Processando lote de {len(messages)} transações no Spark local...")
                df = spark.createDataFrame(messages, schema=schema)
                df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.{ICEBERG_TABLE}")
                
                subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
                print(f"🚀 {len(messages)} transações salvas e commitadas com sucesso no Iceberg Lakehouse!")

        except KeyboardInterrupt:
            print("\n🛑 Encerrando o Consumidor Spark com segurança.")
            break
        except Exception as e:
            # SE O ERRO FOR APENAS TIMEOUT DE REDE, ELE NÃO MATA MAIS O PIPELINE!
            if "Deadline Exceeded" in str(e) or "504" in str(e):
                print("⏳ [Aviso de Rede] O Google demorou para responder. Tentando puxar o próximo lote...")
            else:
                print(f"⚠️ [Aviso] Erro temporário no lote: {e}. Reconectando em 5 segundos...")
                import time
                time.sleep(5)

if __name__ == "__main__":
    main()