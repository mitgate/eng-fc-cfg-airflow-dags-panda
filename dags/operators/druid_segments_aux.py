import requests
import json

class SegmentsAuxDruid(object):
    def __init__(self, druid_url):
        self.druid_url=druid_url
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json'
        }
    def send_quey(self, query):
        # Construa o payload como um dicionário
        payload = {
            "query": query,
            "variables": {}
        }
        # Converta o payload para JSON
        # Converta o payload para JSON
        payload_json = json.dumps(payload)
        base_url = f"{self.druid_url}/druid/v2/sql"        
        response = requests.post(base_url, headers=self.headers, data=payload_json)
        if response.status_code < 200 or response.status_code > 299:
            print(f"❌ Erro ao executar consultas. | Código do erro: {response.status_code}")
        else:
            print(f"✅ Consulta executas!")
            
            return response.json() 
    def delete(self, segment_id,datasource):
        base_url = f"{self.druid_url}/druid/coordinator/v1/datasources/{datasource}/segments/{segment_id}"        
        response = requests.delete(base_url, headers=self.headers)
        if response.status_code < 200 or response.status_code > 299:
            print(f"❌ Erro inesperado: {response.text}")
        else:
            print(f"💥 Segment deleted Name: {segment_id} | Datasource: {datasource} ")