import requests
import time

class RetryRequest():
    """
    # RetryRequest Class Documentation

    ## Overview

    The `RetryRequest` class provides a mechanism to automatically retry HTTP requests using the `requests` library in Python. It is designed to handle transient errors by retrying the request a specified number of times with an exponential backoff strategy.

    ## Installation

    Make sure you have the `requests` library installed. You can install it using pip:

    ```bash
    pip install requests
    """
    def __init__(self, max_retries=3, backoff_factor=1):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def get(self, url, **kwargs):
        return self._retry_request(requests.get, url, **kwargs)

    def post(self, url, **kwargs):
        return self._retry_request(requests.post, url, **kwargs)

    def _retry_request(self, method, url, **kwargs):
        for attempt in range(self.max_retries):
            try:
                response = method(url, **kwargs)
                response.raise_for_status()  # Levanta um erro para códigos de status HTTP 4xx/5xx
                return response
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:  # Se não for a última tentativa
                    wait_time = self.backoff_factor * (2 ** attempt)  # Exponencial backoff
                    print(f"❌ Tentativa {attempt + 1} falhou: {e}. Tentando novamente em {wait_time} segundos...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Todas as tentativas falharam: {e}")
                    raise  # Relevanta a exceção após todas as tentativas falharem
