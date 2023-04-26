import requests
import json
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from utils import currentUnixTime, initLogger

LOGGER = initLogger("API_SERVICE_LOG")


class APIService:
    def getJson(self, url):
        try:
            response = requests.get(url, headers={'x-access-token': 'coinrankinge05d1206ad6a526d1e5ada3a927cf2152f6f3d470a80b59b'})
            statusCode = response.status_code
            assert statusCode == 200, f"Response status code is : {statusCode}"
            LOGGER.info("Received JSON response")
            return response.json()
        except (AssertionError, ConnectionError, Timeout, TooManyRedirects) as e:
            LOGGER.error(f"Exception occurred in getJson: {e}")
            return {}

    def parseCoinData(self, coinData):
        try:
            print("===coin data ==" , json.dumps(coinData, indent=4))
            return {"name_coin": coinData["name"],
                    "symbol_coin": coinData["symbol"],
                    # "id": coinData["id"],
                    "id": 0,
                    "uuid": coinData["uuid"],
                    # "number_of_markets": coinData["numberOfMarkets"],
                    "number_of_markets": 0,
                    "volume": coinData["24hVolume"],
                    "market_cap": coinData["marketCap"],
                    # "total_supply": coinData["totalSupply"],
                    "total_supply": 0,
                    "price": coinData["price"],
                    "percent_change_24hr": coinData["change"],
                    "timestamp": currentUnixTime()}
        except Exception as e:
            LOGGER.error(f"Exception occurred in parseCoinData : {e}")
            return {}

    def filterJson(self, rawJson):
        filteredJson = map(lambda coinData: self.parseCoinData(coinData),
                           rawJson["data"]["coins"])
        LOGGER.info("Parsed JSON response")
        # for k in filteredJson:
        #     print(k)
        # print(json.dumps(filteredJson, indent=4))
        return filteredJson
