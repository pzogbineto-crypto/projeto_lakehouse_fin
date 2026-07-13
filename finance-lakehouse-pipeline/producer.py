from datetime import datetime, timedelta # Atualizar
import json
import random
import time
import os
import uuid
from google.cloud import pubsub_v1
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\pzogb\OneDrive\Documentos\Projetos Pessoais - Profissionais\enterprise_lakehouse\gcp-key.json"
# 1. Configurações do GCP (Ajuste com os seus dados)
PROJECT_ID = "enterprise-finance-lakehouse"
TOPIC_ID = "realtime-transactions"

# Inicializa o cliente do Pub/Sub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def generate_transaction() -> dict:
    """Gera um dicionário simulando uma transação bancária com anomalias

    intencionais de valor (1% das vezes) para testes analíticos.
    """
    # 1. Identificadores e Timestamp
    transaction_id = str(uuid.uuid4())
    # O jeito moderno e recomendado para Python 3.12+:
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # 2. Simulação de Contas (Ex: "341-98765")
    def gen_account():
        agency = random.randint(100, 999)
        account = random.randint(10000, 99999)
        return f"{agency}-{account}"

    sender_account = gen_account()
    receiver_account = gen_account()

    # Evita que a conta de origem seja igual à de destino
    while receiver_account == sender_account:
        receiver_account = gen_account()

    # 3. Tipo de Transação
    transaction_type = random.choice(["PIX", "CREDIT_CARD", "TED"])

    # 4. O pulo do gato: Lógica de Outliers (Anomalias)
    # Sorteia um número de 1 a 100. Se for 1 (1% de chance), gera a fraude.
    if random.randint(1, 100) == 1:
        # 1% das vezes: Valores absurdos
        amount = round(random.uniform(50000.00, 100000.00), 2)
    else:
        # 99% das vezes: Valores normais do dia a dia
        amount = round(random.uniform(5.00, 2000.00), 2)

    # Retorna o dicionário estruturado
    return {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "sender_account": sender_account,
        "receiver_account": receiver_account,
        "transaction_type": transaction_type,
        "amount": amount,
    }


if __name__ == "__main__":
    print(f"🚀 Iniciando Streaming de Transações para o Tópico: {TOPIC_ID}...")

    # --- CONFIGURAÇÃO DO BACKFILL ---
    dias_para_gerar = 6  # Do dia 07 ao dia 12
    transacoes_por_dia = 500
    total_transacoes = dias_para_gerar * transacoes_por_dia
    
    # Data de início do "buraco" (07 de Julho de 2026 à meia-noite)
    start_date = datetime(2026, 7, 7)

    print(f"🚀 Iniciando Backfill: Gerando {total_transacoes} transações históricas...")

    for i in range(total_transacoes):
        # 1. A Viagem no tempo (Sorteando data e hora)
        random_days = random.randint(0, dias_para_gerar - 1)
        random_seconds = random.randint(0, 86399) # Segundos em um dia
        simulated_timestamp = start_date + timedelta(days=random_days, seconds=random_seconds)

        # 2. Gerando o valor aleatório da transação (Mantenha a sua lógica se for diferente)
        amount = round(random.uniform(10.0, 15000.0), 2)
        
        # 3. Montando o Payload com a data forjada
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "sender_account": f"{random.randint(100, 999)}-{random.randint(10000, 99999)}",
            "receiver_account": f"{random.randint(100, 999)}-{random.randint(10000, 99999)}",
            "transaction_type": random.choice(["PIX", "TED", "CREDIT_CARD", "DEBIT"]),
            "amount": amount,
            "timestamp": simulated_timestamp.isoformat() # A Mágica acontece aqui!
        }

        # 4. Empacotando e enviando para o Pub/Sub
        message_bytes = json.dumps(transaction).encode("utf-8")
        
        try:
            publish_future = publisher.publish(topic_path, data=message_bytes)
            publish_future.result() # Aguarda a confirmação de envio
        except Exception as e:
            print(f"Erro ao enviar mensagem: {e}")

        # Remova ou comente o time.sleep(1) para o script disparar as 3000 mensagens o mais rápido possível!
        
        # Apenas um print a cada 500 mensagens para você acompanhar no terminal
        if (i + 1) % 500 == 0:
            print(f"✅ {i + 1} transações enviadas...")

    print("🏁 Backfill concluído com sucesso!")
        

    