
from json import dumps
from utils import initLogger, getConfigPath, readConfigFile
from apiService import APIService
import time
import json
import csv

CONFIG_FILE_PATH = getConfigPath()

file = "data.csv"
CSV_DATA = []
fd = open(file, 'a', newline='')
fileWriter = csv.writer(fd, delimiter=',', quotechar='|')

def produceMessage(filteredJson):
    global CSV_DATA, fileWriter
    # print("THE LEN IS ====", len(list(filteredJson)))
    for coinData in filteredJson:
        nameCoin = coinData["name_coin"]
        try:
            fileWriter.writerow([coinData['name_coin'], coinData['symbol_coin'], coinData['id'],
             coinData['uuid'], coinData['number_of_markets'],
            coinData['volume'], coinData['market_cap'], coinData['total_supply'], 
            coinData['price'], coinData['percent_change_24hr'],coinData['timestamp']])
            print("======SENDING")
        except Exception as e:
            print(e)

if __name__ == "__main__":
    api = APIService()
    fileWriter.writerow(['name_coin', 'symbol_coin', 'id',
        'uuid', 'number_of_markets',
        'volume', 'market_cap', 'total_supply', 
        'price', 'percent_change_24hr', 'timestamp'])
    while True:
        rawJson = api.getJson('https://api.coinranking.com/v2/coins')
        temp = json.loads(json.dumps(rawJson))
        if temp:
            if "data" in temp.keys():
                filteredJson = api.filterJson(rawJson)
                print("ç filteredJson")
                produceMessage(filteredJson)
        time.sleep(5)
