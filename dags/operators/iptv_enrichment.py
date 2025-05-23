import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor

class IPTV_Enrichment(object):
    def __init__(self,default_ip,user,password,default_view=-1):
        self.token= ""
        self.default_ip = default_ip
        self.username = user
        self.password = password
        self.default_view = default_view
        
    def make_request_with_retry(self,url, method="GET", retries=10, delay=10, **kwargs):
        """
        Faz uma requisição HTTP com lógica de retry em caso de erro.

        :param url: URL para a requisição.
        :param method: Método HTTP (GET, POST, etc.).
        :param retries: Número máximo de tentativas em caso de erro.
        :param delay: Tempo (em segundos) entre as tentativas.
        :param kwargs: Parâmetros adicionais para requests.request (headers, data, etc.).
        :return: Resposta da requisição ou None se falhar após todas as tentativas.
        """
        for attempt in range(1, retries + 1):
            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()  # Levanta exceção para códigos de erro HTTP (4xx e 5xx)
                return response.json()
            except requests.RequestException as e:
                print(f"❎ Attempt {attempt} failed: {e}")
                if attempt < retries:
                    print(f"🕓 Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    print("❌ All retries failed.")
                    break

    def enable_access(self):
        url = f"{self.default_ip}/API/v1/Json.asmx/ConnectApp"

        payload = json.dumps({
        "host": "Test",
        "login": self.username,
        "password": self.password,
        "clientAppName": "Teste",
        "clientAppVersion": "v0",
        "clientComputerName": "Test"
        })
        headers = {
        'Content-Type': 'application/json'
        }

        data_return = self.make_request_with_retry(
            url= url,
            method="POST",
            headers=headers,
            data=payload
            )
        self.token = data_return['d']
        
    def get_elements_name(self):

        url = f"{self.default_ip}/API/v1/Json.asmx/GetElementsForView"

        payload = json.dumps({
        "connection": self.token,
        "viewID": -1,
        "includeSubViews": True,
        "includeServices": True
        })
        headers = {
        'Content-Type': 'application/json'
        }
        return_data = self.make_request_with_retry(
            url=url,
            method="POST",
            headers=headers,
            data=payload
            )
        return_data = return_data['d']
        
        base_filtered = []
        for i in return_data:
            if i["Type"] == "Element" and i["State"] == "Active":
                base_filtered.append(i)
        return base_filtered

    def get_metrics_name(self,dmaid, elementid):
        url = f"{self.default_ip}/API/v1/Json.asmx/GetParametersForElement"

        payload = json.dumps({
        "connection": self.token,
        "dmaID": f"{dmaid}",
        "elementID": f"{elementid}"
        })
        headers = {
        'Content-Type': 'application/json'
        }

        return_data = self.make_request_with_retry(
            url=url,
            method="POST",
            headers=headers,
            data=payload
        ) 
        return return_data['d']
    
    def process_element(self, element):
        elementid = element['ID']
        dmaid = element['DataMinerID']
        return self.get_metrics_name(dmaid, elementid)