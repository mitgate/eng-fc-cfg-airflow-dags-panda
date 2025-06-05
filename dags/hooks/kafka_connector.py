from airflow.models import Variable, Connection
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from datetime import datetime
import tempfile
import json
import ssl
import time

class KafkaConnector():
    """
    
    Function created to facilitate sending and receiving information to Kafka. Install the connection through this class and send the parameters to start the connection through the create_kafka_connection function.
    Args:
        topic_var_name (_string_): Name of the var with topic name on which messages will be sent or received.
        kafka_url_var_name (_string_): Name of var with _URL_ Kafka to connect.
        kafka_port_var_name (_string_): _Kafka Name of var with _PORT_ Kafka to connect.
        kafka_variable_pem_content (_string_): The _name_ of the variable that contains the contents of the _PEM_ file for connecting to Kafka_
        kafka_connection_id (_string_): Connection id that contains the json with Kafka connection metadata.
        
    Usage:
        Example of using the KafkaConnector class:

        ```python
        from hooks.kafka_connector import KafkaConnector  # Assuming the class is saved in kafka_connector.py

        druid_url_variable_name = "variable_druid_prod"
        kafka = KafkaConnector(
            topic_var_name= "topic_x",
            kafka_url_var_name="kafka_url_var_name",
            kafka_port_var_name="port_var_name",
            kafka_variable_pem_content="content_pem_prod",
            kafka_connection_id = 'kafka_default')   
            
        producer = kafka.create_kafka_connection()   
        
        test = {"message": "context"}
        kafka.publish_message_with_key(test,producer)```
              
    Created By:
        Leandro ヾ(•ω•`)o
    """
    def __init__(self,topic_var_name,kafka_url_var_name,kafka_port_var_name,kafka_variable_pem_content,kafka_connection_id):
       
        self.topic = Variable.get(topic_var_name)
        self.kafka_url = Variable.get(kafka_url_var_name) # 'amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br'
        self.kafka_port =Variable.get(kafka_port_var_name)
        self.kafka_variable_pem_content = kafka_variable_pem_content        
        self.kafka_connection_id = kafka_connection_id #'kafka_default'  
        
        connection = Connection.get_connection_from_secrets(self.kafka_connection_id)

        self.bootstrap_servers = connection.extra_dejson.get('bootstrap.servers')
        self.security_protocol = connection.extra_dejson.get('security.protocol')
        self.sasl_mechanism = connection.extra_dejson.get('sasl.mechanism')
        self.sasl_username = connection.extra_dejson.get('sasl.username')
        self.sasl_password = connection.extra_dejson.get('sasl.password')
    
    # Callback error do Kafka
    def delivery_callback(self, err, msg):
        if err:
            print('❌ Error al enviar mensaje: %s' % err)
    
    # Create kafka connection
    def create_kafka_connection(self, type_connection='Producer', consumer_name='default_consumer_airflow'):
        """
            Establishes a connection to a Kafka server using SSL and SASL authentication.

            This function configures Kafka connection parameters, including security protocol,
            SASL mechanism, SSL certificate, and Kafka server details. It supports creating
            a Kafka Producer based on the specified connection type.

            Args:
                type_connection (str): The type of Kafka connection to create. Supported values: 'Producer'.
                consumer_name (str): Define this information if you use the Consumer option when connecting to Kafka. Attention! Do not repeat the consumer's name in separate scripts, as this may cause problems!
            Returns:
                Producer: A configured Kafka producer instance if the type_connection is '_Producer_'.
                Consumer: A configured Kafka producer instance if the type_connection is '_Consumer_'.
            Kafka Configuration Parameters:
                - bootstrap.servers: URL and port of the Kafka server.
                - security.protocol: Security protocol to use (e.g., "SASL_SSL").
                - sasl.mechanism: Authentication mechanism (e.g., "SCRAM-SHA-512").
                - sasl.username: Username for SASL authentication.
                - sasl.password: Password for SASL authentication.
                - ssl.ca.location: Path to the SSL certificate.
                - message.max.bytes: Maximum size of a message (default: 1 GB).
                - batch.num.messages: Maximum number of messages in a batch.
                - linger.ms: Delay before sending a batch to optimize batching.
                - compression.type: Compression algorithm for messages (e.g., "lz4").
                - queue.buffering.max.messages: Maximum number of messages in the buffer.
                - queue.buffering.max.kbytes: Maximum buffer size in kilobytes.
                - max.in.flight.requests.per.connection: Maximum in-flight requests per connection.
                - queue.buffering.max.ms: Maximum buffering time in milliseconds.
                - message.send.max.retries: Maximum retry attempts for failed messages.
                - retry.backoff.ms: Backoff time between retry attempts.

            Example:
                ```python
                kafka_instance = KafkaClient()
                producer = kafka_instance.create_kafka_connection(type_connection='Producer')
                ```

            Notes:
                - The function retrieves the SSL PEM content from an Airflow Variable.
                - Temporary files are used to store the SSL certificate for the connection.
                - Only "Producer" connection type is implemented in this version.
        """
        DEFAULT_TOPIC = self.topic
        KAFKA_URL = self.kafka_url 
        KAFKA_PORT = self.kafka_port
        pem_content = Variable.get(self.kafka_variable_pem_content)

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pem_content.encode())

        ssl_location = temp_file.name
        # Configurações do Kafka
        print(f'🔌 Informations of connection:')
        print(f'🔌Url:      {KAFKA_URL}')
        print(f'🔌Port:     {KAFKA_PORT}')
        print(f'🔌Topic:    {DEFAULT_TOPIC}')
        conf = {
            "bootstrap.servers": f"{KAFKA_URL}:{KAFKA_PORT}",
            "security.protocol": self.security_protocol, #"SASL_SSL",
            "sasl.mechanism": self.sasl_mechanism, #"SCRAM-SHA-512",
            "sasl.username": self.sasl_username, #"flink",
            "sasl.password": self.sasl_password, #"NMoW680oSNitVesBQti9jlsjl7GC8u36",
            'ssl.ca.location': ssl_location,
            'message.max.bytes': '1000000000',
            'batch.num.messages': 1000000,
            'linger.ms': 500,
            'compression.type': 'lz4',
            'queue.buffering.max.messages': 2000000,
            'queue.buffering.max.kbytes': 2097152,
            'max.in.flight.requests.per.connection': 5,
            'queue.buffering.max.ms': 500,
            'message.send.max.retries': 5,
            'retry.backoff.ms': 500,
        }
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(conf['ssl.ca.location'])
        if type_connection == 'Producer':
            return Producer(**conf)
        if type_connection == 'Consumer':
            conf['group.id'] = consumer_name
            conf['auto.offset.reset'] = 'latest'
            return Consumer(conf)
    
    # Internal Funcion
    def send_mult_message(self, message,  producer, key=None):
        """
        ❌ Internal funciont, don't use ❌
        """
        try:

            producer.produce(self.topic, key=key, value=json.dumps(message).encode('utf-8')
                            , callback=self.delivery_callback)  
        except Exception as e:
            print(f"❌ Erro ao enviar mensagem para o Kafka: {e}")
    
    # Send a specific message
    def send_message(self, message,  producer, key=None):
        """
        Function to send a specific message to the instantiated topic.
        Args:
            message (_json_): Message that will be sent 
            producer (_type_): _Producer_ _Kafka_
            key (_type_, optional): _description_. Defaults to None.
        """
        try:

            producer.produce(self.topic, key=key, value=json.dumps(message).encode('utf-8')
                            , callback=self.delivery_callback) 
            producer.flush()
        except Exception as e:
            print(f"❌ Erro ao enviar mensagem para o Kafka: {e}")
    
    # Send a specific message with text type
    def send_message_text(self, message,  producer, key=None):
        """
        Function to send a specific message to the instantiated topic.
        Args:
            message (_json_): Message that will be sent 
            producer (_type_): _Producer_ _Kafka_
            key (_type_, optional): _description_. Defaults to None.
        """
        try:

            producer.produce(self.topic, key=key, value=message.encode('utf-8')
                            , callback=self.delivery_callback) 
            producer.flush()
        except Exception as e:
            print(f"❌ Erro ao enviar mensagem para o Kafka: {e}") 
    
    # Send multiple messages
    def send_mult_messages_to_kafka(self, menssages, producer, key=None):
        """
        Function to send multiple messages to the instantiated topic. The expected format for sending is an array: _[~,~,~]_.

        Args:
            menssages (_array_): Messages that will be sent
            producer (_object_): _Producer_ _Kafka_
            key (_string_, optional): _description_. Defaults to None.
        """
        for mensagem in menssages:
            self.send_mult_message(mensagem, producer, key)
        producer.flush()
    
    def create_kafka_consumer_manual_offset(self, consumer_name='default_consumer_airflow', extra_consumer_configs=None):
        """
        Cria um CONSUMER Kafka com commit manual, sem alterar a função create_kafka_connection() já existente.
        Ideal para cenários onde não queremos pular mensagens caso ocorra erro de processamento.

        Args:
            consumer_name (str): Nome (group.id) do consumer. Não repetir em diferentes DAGs se quiser independência.
            extra_consumer_configs (dict): Configurações extras (ex: {"enable.auto.commit": False}).

        Returns:
            Consumer: Instância do confluent_kafka.Consumer com offset manual.
        """
        if extra_consumer_configs is None:
            extra_consumer_configs = {}

        # Lê o PEM do airflow
        pem_content = Variable.get(self.kafka_variable_pem_content)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pem_content.encode())
        ssl_location = temp_file.name

        conf = {
            "bootstrap.servers": f"{self.kafka_url}:{self.kafka_port}",
            "security.protocol": self.security_protocol,
            "sasl.mechanism": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            'ssl.ca.location': ssl_location,
            'message.max.bytes': '1000000000',
            'batch.num.messages': 1000000,
            'linger.ms': 500,
            'compression.type': 'lz4',
            'queue.buffering.max.messages': 2000000,
            'queue.buffering.max.kbytes': 2097152,
            'max.in.flight.requests.per.connection': 5,
            'queue.buffering.max.ms': 500,
            'message.send.max.retries': 5,
            'retry.backoff.ms': 500,
            # Configurações específicas de consumer
            'group.id': consumer_name,
            'auto.offset.reset': 'latest', 
        }

        # Mescla configs extras (ex.: enable.auto.commit=False)
        for k, v in extra_consumer_configs.items():
            conf[k] = v

        # Debug
        print("🔌 Criando Consumer com commit manual!")
        for ck, cv in conf.items():
            if ck in ["bootstrap.servers", "security.protocol", "group.id", "enable.auto.commit", "auto.offset.reset"]:
                print(f"   > {ck}: {cv}")

        return Consumer(conf)

    # Consumer messages from Kafka
    def process_messages(self,
                         consumer,
                         timeout = 10.0
                         ):
        """
            Consome mensagens do Kafka a partir de um tópico específico, filtrando mensagens com base na data da última leitura e 
            atualizando a variável com a data da última execução bem-sucedida.

            Args:
                consumer (KafkaConsumer): O consumidor Kafka utilizado para a coleta de dados.
                topic_name (str): Nome do tópico Kafka que será consumido.
                timeout (float, optional): Tempo limite para polling em segundos (padrão: 10.0).
                format_date (str, optional): Formato padrão para a data e hora a ser utilizado (padrão: '%Y-%m-%d %H:%M:%S').

            Returns:
                list[dict]: Lista de mensagens processadas, onde cada mensagem é representada como um dicionário.
        """
        messages = []
        consumer.subscribe([self.topic])

        try:
            while True:
                msg = consumer.poll(timeout=timeout)

                if msg is None:
                    break  # Sem novas mensagens, sai do loop

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue  # Fim da partição, continua
                    else:
                        raise KafkaException(msg.error())  # Lança exceção em caso de erro crítico

                # Processa a mensagem (exemplo de decodificação)
                message_value = msg.value().decode('utf-8')
                try:
                    message_data = json.loads(message_value)
                    messages.append(message_data)

                except Exception as e:
                    print(f"❌ Message {message_value} | Error: {e}")
           
        except Exception as e:
            print(e)
         
        finally:
            print('🔵 Processo de coleta finalizada!')   
        
        return messages
    

    
    def process_messages_streaming(self, consumer, callback_function, max_processing_time=300, timeout=30.0, max_empty_attempts=3):
        """
        Processa mensagens do Kafka em streaming, chamando uma função de callback para cada mensagem.
        
        Args:
            consumer (KafkaConsumer): O consumidor Kafka utilizado para a coleta de dados.
            callback_function (function): Função que será chamada para cada mensagem processada.
                A função deve aceitar um parâmetro com o conteúdo da mensagem (já decodificado como dicionário).
            max_processing_time (int, optional): Tempo máximo de processamento em segundos (padrão: 300).
            timeout (float, optional): Timeout para o consumer em segundos (padrão: 30.0).
            max_empty_attempts (int, optional): Número máximo de tentativas vazias consecutivas (padrão: 3).
            
        Returns:
            dict: Estatísticas do processamento, incluindo total de mensagens processadas e tempo total.
        """
        consumer.subscribe([self.topic])
        
        start_time = time.time()
        total_messages = 0
        empty_attempts = 0
        last_log_time = start_time
        
        try:
            while True:
                # Verificar se excedeu o tempo máximo
                elapsed_time = time.time() - start_time
                if elapsed_time > max_processing_time:
                    print(f"⏰ Tempo máximo de processamento atingido ({max_processing_time} segundos)")
                    break
                
                # Consumir mensagem única
                msg = consumer.poll(timeout=timeout)
                
                if msg is None:
                    empty_attempts += 1
                    print(f"⏳ Sem mensagens para processar no momento... (Tentativa {empty_attempts}/{max_empty_attempts})")
                    
                    if empty_attempts >= max_empty_attempts:
                        print("⚠️ Número máximo de tentativas vazias atingido. Verificando se há mais mensagens disponíveis...")
                        empty_attempts = 0
                        time.sleep(5)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Erro ao consumir mensagem: {msg.error()}")
                        break
                
                # Resetar o contador de tentativas vazias quando receber mensagem
                empty_attempts = 0
                
                # Processar a mensagem
                try:
                    message_value = msg.value().decode('utf-8')
                    message_data = json.loads(message_value)
                    
                    # Chamar a função de callback com a mensagem
                    callback_function(message_data)
                    
                    total_messages += 1
                    
                    # Log a cada 5 segundos para não sobrecarregar
                    current_time = time.time()
                    if current_time - last_log_time >= 5:
                        print(f"✅ Mensagens processadas: {total_messages} | Taxa: {total_messages/elapsed_time:.1f} msg/s")
                        last_log_time = current_time
                    
                except Exception as e:
                    print(f"❌ Erro ao processar mensagem: {str(e)}")
                    continue
        
        except Exception as e:
            print(f"❌ Erro durante o processamento: {str(e)}")
            raise
        finally:
            processing_time = time.time() - start_time
            print("\n" + "="*50)
            print("📊 RESUMO DO PROCESSAMENTO")
            print("="*50)
            print(f"✅ Processamento concluído!")
            print(f"📦 Total de mensagens processadas: {total_messages}")
            print(f"⏱️ Tempo total de processamento: {processing_time:.2f} segundos")
            print(f"🚀 Taxa média de processamento: {total_messages/processing_time:.2f} mensagens/segundo")
            print("="*50)
            
            return {
                "total_messages": total_messages,
                "processing_time": processing_time,
                "rate": total_messages/processing_time if processing_time > 0 else 0
            }
    
