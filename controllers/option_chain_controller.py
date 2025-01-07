import requests


class OptionChainController:
    def __init__(self):
        self.base_url = "https://www.nseindia.com/api/option-chain-indices"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/114.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        self.session = requests.Session()
        self.session.get("https://www.nseindia.com", headers=self.headers)

    def fetch_option_chain(self, symbol):
        url = f"{self.base_url}?symbol={symbol}"
        try:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()  # Raise an error for bad status codes
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
