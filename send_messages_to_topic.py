"""
    Esse código será usado em uma Cloud Function para enviar as mensagens recebidas no Big Table para o tópico
    no Pub/Sub e assim que a assinatura ler essa mensagem, esta será enviada para o Bigquery, para uma tabela que iremos
    definir posteriormente.

    Change logs:
        0.1-dev:
            - Criado conexão com o projeto do GCP e com o tópico que o script irá usar;
            - Criado código para publicar as mensagens no tópico, o tópico irá receber e a assinatura irá ler.

    Versão: 0.1-dev
"""
from google.cloud import pubsub_v1


# Identificadores do projeto e do tópico
project_id = "airy-charmer-386522"
topic_id = "test-topic"

# Função que irá estabelecer conexão com o PubSub
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):
    data_str = f"Message number {n}"
    # É preciso converter a mensagem em bytestring
    data = data_str.encode('utf-8')
    future = publisher.publish(topic_path, data)

    print(future.result())

print(f"Mensagens publicadas em {topic_path}")
