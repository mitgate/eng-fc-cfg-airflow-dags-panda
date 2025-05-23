import time
import json
import requests
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from urllib3.exceptions import InsecureRequestWarning
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
scroll_time_duration = "60m"
username = "acessoapi"
password = "Telefonica@Panda15"


def basic_auth(username, password):
    token = b64encode(f"{username}:{password}".encode('utf-8')).decode("ascii")
    return f'Basic {token}'

pass_base64 = basic_auth(
    username = username,
    password =password
)  

class IptvConnector(object):
    def __init__(self, index):
        self.index = index
        self.headers = {
        'Accept': '*/*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json',
        'osd-version': '2.17.1',
        'osd-xsrf': 'osd-fetch',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Authorization': pass_base64
        }
        self.url_query = f"https://10.205.190.178:9200/analytics/_search?pretty=true&scroll={scroll_time_duration}"
        self.url_query_scroll = f"https://10.205.190.178:9200/_search/scroll"
        #"https://10.205.190.178:5601/internal/search/opensearch-with-long-numerals"


    def get_data_field(self, key):
        try: 
            return_date_field = {
                #✅
                "sldmaindex-active-analytics_changepoints":{ 
                    "date_filter_field": "CustomData.CreationTime",
                    "date_filter_mapping": "_source.CustomData.CreationTime"    
                },
                #✅
                "canais*":{ 
                    "date_filter_field": "Data Apresentação",
                    "date_filter_mapping": "_source.Data Apresentação"    
                },
                #✅
                "downdetector*":{ 
                    "date_filter_field": "startdate",
                    "date_filter_mapping": "_source.startdate"    
                },
                #✅
                "hlx*":{ 
                    "date_filter_field": "Data Apresentação",
                    "date_filter_mapping": "_source.Data Apresentação"    
                },
                #✅
                "equipamentos*":{ 
                    "date_filter_field": "Data Apresentação",
                    "date_filter_mapping": "_source.Data Apresentação"    
                },
                #✅
                "disponibilidade*":{ 
                    "date_filter_field": "Timestamp",
                    "date_filter_mapping": "_source.Timestamp"    
                },
                #✅
                "happiness-score*":{ 
                    "date_filter_field": "Timestamp-Fim",
                    "date_filter_mapping": "_source.Timestamp-Fim"    
                },
                #✅
                "sldmaindex-active-alarms":{ 
                    "date_filter_field": "RootTime",    
                    "date_filter_mapping": "_source.RootTime"    
                },
                #✅
                "erros204*":{ 
                    "date_filter_field": "Timestamp",    
                    "date_filter_mapping": "_source.Timestamp"    
                },
                #✅
                "sldmaindex-analytics_changepoints-2024.02.09.14-000001":{ 
                    "date_filter_field": "CustomData.EndTime",    
                    "date_filter_mapping": "_source.CustomData.EndTime"    
                },
                #✅
                "gestor*":{ 
                    "date_filter_field": "Data_Criacao",    
                    "date_filter_mapping": "_source.Data_Criacao",    
                },
                #✅
                "sldmaindex*":{ 
                    "date_filter_field": "RootTime",    
                    "date_filter_mapping": "_source.RootTime",    
                },
                #✅
                "sldmaindex-analytics*":{ 
                    "date_filter_field": "CustomData.StartTime",    
                    "date_filter_mapping": "_source.CustomData.StartTime",    
                },
                #✅
                "speedtest*":{ 
                    "date_filter_field": "ts_result",    
                    "date_filter_mapping": "_source.ts_result",    
                },
                #✅
                "analytics":{ 
                    "date_filter_field": "CustomData.CreationTime",    
                    "date_filter_mapping": "_source.CustomData.CreationTime",    
                }
            }
            return return_date_field[key]
        except:
            print(f'❌ Algo deu erro ao buscar a chave: {key}')
            raise

    def generate_query(self,
                       index,
                       start_date=f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}",
                       end_date=f"{(datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%SZ')}",
                       scroll_token = None
                       ):
        key_info = self.get_data_field(index)
        if scroll_token is None:
            return json.dumps(
            {
                "sort": [
                    {
                    key_info['date_filter_field']: {
                        "order": "desc",
                        "unmapped_type": "boolean"
                    }
                    }
                ],
                "size": 10000,
                "version": True,
                "stored_fields": [
                    "*"
                ],
                "script_fields": {},
                "docvalue_fields": [
                    {
                        "field": "CloseTime",
                        "format": "date_time"
                    },
                    {
                        "field": "CreationTime",
                        "format": "date_time"
                    },
                    {
                        "field": "CustomData.CreationTime",
                        "format": "date_time"
                    },
                    {
                        "field": "CustomData.EndTime",
                        "format": "date_time"
                    },
                    {
                        "field": "CustomData.LastUpdate",
                        "format": "date_time"
                    },
                    {
                        "field": "CustomData.SL_Internal_TimeField",
                        "format": "date_time"
                    },
                    {
                        "field": "CustomData.StartTime",
                        "format": "date_time"
                    },
                    {
                        "field": "LastCacheTime",
                        "format": "date_time"
                    },
                    {
                        "field": "LastCleaned",
                        "format": "date_time"
                    },
                    {
                        "field": "LastSquashTime",
                        "format": "date_time"
                    },
                    {
                        "field": "RootCreationTime",
                        "format": "date_time"
                    },
                    {
                        "field": "RootTime",
                        "format": "date_time"
                    },
                    {
                        "field": "TimeOfArrival",
                        "format": "date_time"
                    }   
                ],
                "_source": {
                    "excludes": []
                },
                "query": {
                    "bool": {
                        "must": [],
                        "filter": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "bool": {
                                                "must_not": {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "_index": "sldmaindex-activealarms"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "bool": {
                                                "must_not": {
                                                    "bool": {
                                                        "should": [
                                                            {
                                                                "match_phrase": {
                                                                    "_index": "sldmaindex-alarms"
                                                                }
                                                            }
                                                        ],
                                                        "minimum_should_match": 1
                                                    }
                                                }
                                            }
                                        },
                                        {
                                        "range": {
                                            key_info['date_filter_field'] : {
                                            "gte": start_date,
                                            "lte": end_date,
                                            "format": "strict_date_optional_time"
                                            }
                                        }
                                        }
                                    ]
                                }
                            }
                        ],
                        "should": [],
                        "must_not": []
                    }
                },
                "highlight": {
                    "pre_tags": [
                        "@opensearch-dashboards-highlighted-field@"
                    ],
                    "post_tags": [
                        "@/opensearch-dashboards-highlighted-field@"
                    ],
                    "fields": {
                        "*": {}
                    }
                }
            }
            )
        
        else:
            return json.dumps(
                {
            "scroll": scroll_time_duration,
                "scroll_id": scroll_token
            }
                            
            )
    def get_max_date(self, json_array, date_key):
        """
        Retorna a maior data de um campo específico em um array de JSONs, lidando com formatos ISO 8601 e EPOCH.
        A saída será sempre no formato ISO 8601 com sufixo 'Z' (ex: 2024-11-22T10:00:47.649000Z).

        :param json_array: Lista de dicionários contendo os dados.
        :param date_key: Nome da chave onde a data está localizada, incluindo caminhos aninhados (ex: 'CustomData.StartTime').
        :return: A maior data encontrada como string em ISO 8601 com sufixo 'Z' ou None caso a chave não exista.
        """
        max_date = None

        for item in json_array:
            # Divide o caminho da chave
            keys = date_key.split(".")
            current_value = item

            try:
                # Navega até o valor desejado
                for key in keys:
                    current_value = current_value[key]

                # Verifica se é EPOCH (inteiro ou string) ou ISO 8601
                if isinstance(current_value, (int, float)):  # EPOCH format
                    date_value = datetime.utcfromtimestamp(current_value / 1000).replace(tzinfo=timezone.utc)
                elif isinstance(current_value, str):  # ISO 8601 format
                    date_value = datetime.fromisoformat(current_value.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
                else:
                    raise ValueError("❌ Formato de data não reconhecido")

                # Compara para encontrar a maior data
                if max_date is None or date_value > max_date:
                    max_date = date_value

            except (KeyError, ValueError, TypeError):
                # Ignora se a chave não existir ou o valor for inválido
                continue

        # Retorna a data máxima encontrada como string ISO 8601 com sufixo 'Z'
        return max_date.isoformat().replace("+00:00", "Z") if max_date else None
    def send_query(self, query, url, method="GET"):

        max_retries = 5 # tentativas
        retry_delay = 2 # tempo entre as tentativas

        for attempt in range(max_retries):
            try:

                response = requests.request(method, url, headers=self.headers, data=query, verify=False)

                if response.status_code == 200:
                    #print('✅ Retorno dos regstros...')
                    return response.json()
                else:
                    print(f"🔃 Tentativa {attempt + 1}: Status {response.status_code}. Retentando...")
                    print(query)
            except requests.exceptions.RequestException as e:
                print(f"❌ Erro na tentativa {attempt + 1}: {e}. Retentando...")

            # Aguarda antes da próxima tentativa
            time.sleep(retry_delay)
        else:
            raise Exception("❌ Não foi possível obter uma resposta após 5 tentativas.")

    def main(self,start_date):
        start_time_validation = time.time()

        end_date_timestamp = datetime.now().strftime(DATE_FORMAT)  # string
        end_date_datetime_object = datetime.strptime(end_date_timestamp, DATE_FORMAT) # object
        #print('TIPO END_DATE',type(end_date_datetime_object))    

        start_date_timestamp = datetime.strptime(start_date, DATE_FORMAT)
        #print('TIPO START_DATE',type(start_date_timestamp))

        if start_date_timestamp < end_date_datetime_object:
            print(f'✅ Processo iniciado, período correto para coleta')

            print(f'🔹 INDICE: {self.index} 🔹')
            print(f'⌚ Filtrando de {start_date} até {end_date_timestamp}')

            query = self.generate_query(
                self.index,
                start_date=start_date,
                end_date=end_date_timestamp
            )
            
            total_return_data = []

            response_data = self.send_query(query, self.url_query)
            
            # Total hist esperados
            total_hists_desperate = response_data['hits']['total']['value']
            
            # Total hists da primeira coleta
            data_array = response_data['hits']['hits']
            
            # Scroll id para paginação...
            scroll_id = response_data['_scroll_id']

            if len(data_array) < total_hists_desperate: # Verifica se reotnrou tudo que deveria
                
                # reset da coleta
                total_return_data.extend(data_array)
                
                print('🔎 Inicio do processo de coleta de dados segmentados...')
                
                # Iteração com ajuste de 1 segundo no end_date
                while True:
                    print(f'🚛 Executando scroll do token: {scroll_id}')
                    query = self.generate_query(self.index,scroll_token=scroll_id)
                    
                    response_data = self.send_query(query, self.url_query_scroll, "POST")
                    data_array = response_data['hits']['hits']
                    if len(data_array) > 0:
                        print(f'💧 Total de registros retornados: {len(data_array)}')
                        total_return_data.extend(data_array)        
                        print(f'💧 Total coletado ate agora... {len(total_return_data)}')
                        print(f'🚚 Atribuindo novo scroll_id...')
                        scroll_id = response_data['_scroll_id']
                    else:
                        print('🆗 Fim da coleta')
                        break
            else: 
                total_return_data.extend(data_array)
                print('👌 Retornou todos os registros...')

            max_datetime = self.get_max_date(
                total_return_data,
                date_key=self.get_data_field(self.index)['date_filter_mapping']
            )
            print(f'{"✅" if len(total_return_data) == total_hists_desperate else "❌"} Total esperado X coletado: {len(total_return_data)} ✖ {total_hists_desperate}')
            print(f"🕒 Último registro coletado: {max_datetime if max_datetime else None }")
            print(f'✅ Total de registros coletados: {len(total_return_data)}')     


            end_time_validation = time.time()
            execution_time = end_time_validation - start_time_validation
            print(f"⏰ Tempo de execução: {execution_time:.2f} segundos")
            return total_return_data, max_datetime
        else:
            print(f'🐰 Não necessária coleta agora.')
            raise AirflowSkipException(f'🐰 Não necessária coleta agora.')