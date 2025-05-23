import requests
import json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class SupersetConnector(object):
    def __init__(self,url):
        self.url = url
        pass
    def get_data_from_chart(self,chart_id,no_cache=False):
        import requests
        if no_cache:
            url = f"{self.url}/api/v1/chart/{chart_id}/data/?force=True"
        else:
            url = f"{self.url}/api/v1/chart/{chart_id}/data/"
        headers = {
        'Accept': 'application/json',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
        'Content-Type': 'application/json',
        }

        response = requests.request("GET", url, headers=headers, verify=False)
        response_status = response.status_code
        
        if response_status == 200:
            return response.json()['result'][0]['data']
        else:
            print(f"❌ Error: {response.text}")
            return None
    def get_warmup_cache(self,chart_id,dashboard_id):
        # Função para aquecer o cache
        url = f"{self.url}/api/v1/chart/warm_up_cache"
        
        payload = json.dumps({
            "chart_id": int(chart_id),
            "dashboard_id": int(dashboard_id)
        })
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        response = requests.put(url, headers=headers, data=payload)
        if response.status_code == 200:
            print(f'✅ | Dashboard_id: {dashboard_id} | Chart_id: {chart_id}')
            return True
        else:
            print(f'❌ | Dashboard_id: {dashboard_id} | Chart_id: {chart_id}')
            raise f"Failed to warm up cache for dashboard [{dashboard_id}] and [{chart_id}]"
            
