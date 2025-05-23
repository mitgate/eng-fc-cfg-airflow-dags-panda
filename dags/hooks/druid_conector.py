from concurrent.futures import ProcessPoolExecutor
from airflow.models import Variable
import requests
import json
import time
import math
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DruidConnector():
    """
    DruidConnector Class

    This class provides a convenient way to interact with a Druid database by sending SQL queries and retrieving results.

    Attributes:
        retry_delay (int): Time in seconds to wait between retries after a failed query.
        retries (int): Maximum number of retry attempts for a failed query.
        druid_url (str): The base URL of the Druid server.

    Usage:
        Example of using the DruidConnector class:

        ```python
        from hooks.druid_connector import DruidConnector  # Assuming the class is saved in druid_connector.py

        druid_url_variable_name = "variable_druid_prod"
        connector = DruidConnector(druid_url_variable=druid_url_variable_name, r_delay=10, retries=3)
        ```          
    Created By:
        Sadir ヾ(•ω•`)o
    """
    def __init__(self, druid_url_variable, r_delay=15, retries=5):
        self.retry_delay = r_delay
        self.retries = retries
        self.druid_url_variable = druid_url_variable

    def send_query(
        self,
        query=''):
        """
        send_query Method

        Sends an SQL query to the Druid server and retrieves the results.

        Args:
            query (str): The SQL query to execute. Must be passed as a raw SQL string using triple quotes.

        Returns:
            list[dict]: A list of dictionaries containing the query results. Each dictionary represents a row.

        Raises:
            Exception: If all retry attempts fail or there is an unexpected error during execution.

        Notes:
            - The query must be a raw SQL string formatted with triple quotes for readability.
            - The first row of the result is considered a header and is excluded from the return value.
            - The function retries the query up to the specified retry count (`retries`) with a delay (`retry_delay`) between attempts.
            - SSL verification is disabled for simplicity. Ensure caution in production environments.

        Example Query:
            Use triple quotes to format your query:
            ```python
            query = " " "
            SELECT 
                __time, 
                COUNT(*) AS count 
            FROM 
                "druid_table_name"
            WHERE 
                __time BETWEEN TIMESTAMP '2023-01-01 00:00:00' AND TIMESTAMP '2023-01-02 00:00:00'
            GROUP BY 
                __time
            ORDER BY 
                __time
            " " "
            ```
        """
        url = Variable.get(self.druid_url_variable)
        url = f"{url}/druid/v2/sql"
        payload = json.dumps({
            "query": query,
            "resultFormat": "object",
            "header": True,
            "typesHeader": True,
            "sqlTypesHeader": True,
            "context": {
                "enableWindowing": True,
                "useParallelMerge": True,
                "executionMode": "ASYNC",
                "populateCache": True,
                "useCache": True
            }
        })
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json'
        }

        attempt = 0
        while attempt < self.retries:
            try:
                response = requests.post(url, headers=headers, data=payload, verify=False)
                if response.status_code == 400:
                    print(f'❌ Error: {response.text}')
                    print(f'❌ ===================================================')
                    print(f'❌ Query: {query}')
                    print(f'❌ ===================================================')
                response.raise_for_status()
                result = response.json()
                return result[1:]  # Retorna o resultado a partir da segunda linha (ignorando o cabeçalho)
            except Exception as e:
                print(f"❌ Erro na tentativa {attempt + 1}: {e}")
                attempt += 1
                if attempt < self.retries:
                    time.sleep(self.retry_delay)
                else:
                    raise
    
    def execute_queries_in_batches(
        self,
        queries = [],
        workers = 4,
        batch_size = 10
    ):
        """
        Executes multiple SQL queries in parallel using subprocesses.

        Args:
            queries (list[str]): A list of SQL queries to execute.
            workers (int): Number of subprocess workers to use for parallel execution. Default is 4.
            batch_size (int): Number of queries to send in each batch. Default is 10.

        Returns:
            list[dict]: A consolidated list of results from all queries.

        Raises:
            Exception: If any batch fails after all retries.
        """
        def execute_batch(batch_queries: list[str]):
            results = []
            for query in batch_queries:
                try:
                    result = self.send_query(query=query)
                    results.extend(result)
                except Exception as e:
                    print(f"❌ Failed query: {query}\nError: {e}")
            return results

        # Partition the queries into batches
        total_batches = math.ceil(len(queries) / batch_size)
        query_batches = [queries[i * batch_size:(i + 1) * batch_size] for i in range(total_batches)]

        consolidated_results = []

        with ProcessPoolExecutor(max_workers=workers) as executor:
            # Submit each batch to the executor
            futures = [executor.submit(execute_batch, batch) for batch in query_batches]

            # Collect results as they complete
            for future in futures:
                try:
                    consolidated_results.extend(future.result())
                except Exception as e:
                    print(f"❌ Error processing a batch: {e}")

        return consolidated_results                