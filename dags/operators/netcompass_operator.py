import json
from hooks.retry_request import RetryRequest
r = RetryRequest()
USERNAME = "panda-client"
PASSWORD = "vivo%40123"
TOKEN = "bmV0Y29tcGFzcy1hcGk6Qjg4am5YeldJRW81VTBnR25SNmtsbGdjajRJUFhaazE="

class NetcompassOperator(object):
    def __init__(self,
                 url="https://api-netcompass.tdigital-vivo.com.br",
                 sso_url="https://sso.tdigital-vivo.com.br"):
        self.url = url
        self.sso_url = sso_url
        
    def get_token(self):
        url = f"{self.sso_url}/auth/realms/netcompass/protocol/openid-connect/token"

        payload = f'grant_type=password&username={USERNAME}&password={PASSWORD}'
        headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
        'Authorization': f'Basic {TOKEN}',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',

        }

        response = r.post(url=url, headers=headers, data=payload)

        return response.json()['access_token']
    def send_query(self,token, limit, offset, max=False):
        url = f"{self.url}/inventory/v1/central/filter"

        payload = json.dumps({
            "payLoad": {
                "objects": [
                    "nodes"
                ],
                "sortCondition": "SORT doc.lastModifiedDate DESC",
                "limit": limit,
                "offSet": offset
            }
        })
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
            'authorization': f'Bearer {token}',
            'content-type': 'application/json',
        }

        response = r.post(url, headers=headers, data=payload)
        
        if response.status_code == 204:
            return 204
        else: 
            if max: 
                return response.json()
            else:
                data_x  = response.json()['payLoad']['nodes']
                return [{"attributes": i["attributes"], "discoveryAttributes": i["discoveryAttributes"]} for i in data_x]        
    def get_total_records(self,token):
        # Aqui você deve implementar a lógica para obter o total de registros
        initial_response = self.send_query(token, 1, 0, True)  # Requisição para obter o total
        return initial_response.get("size", 0)
    def paginate_api(self,token, limit):
        total_records = self.get_total_records(token)
        print(f'📃 Total de registros: {total_records}')
        pages = total_records // limit + (1 if total_records % limit > 0 else 0)
        print('📘 Totla de página: ', pages)
        all_data = []  # Lista para armazenar todos os dados coletados
        
        for page in range(pages):
            offset = page * limit
            print(f'📓 Requisitando página {page + 1} com offset {offset}')
            response = self.send_query(token, limit, offset)
            if response == 204:
                print("🆗 Nenhum dado encontrado na página.")
                continue
            else:
                all_data.extend(response)
                #print(f'⏩ Total de registros coletados até agora: {len(all_data)}')
                # Adiciona os dados da resposta à lista all_data
                #all_data.extend(response.get("payload", {}).get("objects", []))
        print(f'⏩ Total de registros coletados: {len(all_data)}')
        return all_data  # Retorna todos os dados coletados
    def extract_fields(input_json,contexto):
        output_json = {}
        mapeamento_internal = contexto['mapeamento']
        description = contexto['descricao']
        domain = contexto['dominio']
        export_extra_data = contexto['exportarCamposNaoMapeados']
        query = contexto['query']
        # Preenchendo o novo JSON com os dados mapeados
        for key, value in mapeamento_internal.items():
            # Separando a chave do mapeamento_internal
            section, field = key.split('.')
            
            # Inicializando a seção se não existir
            if section not in output_json:
                output_json[section] = {}
            
            # Verificando se o campo existe no JSON de entrada
            if field == '*':
                # Para campos que usam '*', adicionamos todos os discoveryAttributes
                output_json[section] = input_json.get('discoveryAttributes', {})
            else:
                # Para campos específicos
                field_path = value.split('.')
                data = input_json
                for part in field_path:
                    data = data.get(part, {})
                output_json[section][field] = data
        output_json['description'] = description
        output_json['domain'] = domain
        output_json['export_extra_data'] = export_extra_data
        output_json['query'] = query
        
        return output_json