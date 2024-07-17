import time
import requests
import websocket
from typing import List, Dict
import json


class OrderBookClient:
    def __init__(self, symbols: List[str]):
        self.__base_uri = "wss://stream.binance.com:9443/ws"
        self.__symbols: List[str] = symbols
        self.__ws = websocket.WebSocketApp(self.__base_uri,
                                           on_message=self.__on_message,
                                           on_error=self.__on_error,
                                           on_close=self.__on_close)
        self.__ws.on_open = self.__on_open
        self.__lookup_snapshot_id: Dict[str, int] = dict()
        self.__lookup_update_id: Dict[str, int] = dict()

        self.__closed = False

    def __connect(self) -> bool:
        self.__ws.run_forever()
        self.__closed = True
        return True

    def __on_message(self, _ws, message):
        data = json.loads(message)
        update_id_low = data.get("U")
        update_id_upp = data.get("u")
        if update_id_low is None:
            return

        symbol = data.get("s")
        snapshot_id = self.__lookup_snapshot_id.get(symbol)
        if snapshot_id is None:
            self.get_snapshot(symbol)
            return
        elif update_id_upp < snapshot_id:
            return

        self.__log_message(message)
        prev_update_id = self.__lookup_update_id.get(symbol)
        if prev_update_id is None:
            assert update_id_low <= snapshot_id <= update_id_upp
        else:
            assert update_id_low == prev_update_id + 1

        self.__lookup_update_id[symbol] = update_id_upp
        return

    def __on_error(self, _ws, error):
        print(f"Encountered error: {error}")
        return

    def __on_close(self, _ws, _close_status_code, _close_msg):
        print("Connection closed")
        return

    def __on_open(self, _ws):
        print("Connection opened")
        for symbol in self.__symbols:
            _ws.send(f"{{\"method\": \"SUBSCRIBE\",  \"params\": [\"{symbol.lower()}@depth\"], \"id\": 1}}")
        return

    def __log_message(self, msg: str) -> None:
        print(msg)
        return

    def get_snapshot(self, symbol: str):
        snapshot_url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1000"
        x = requests.get(snapshot_url)
        content = x.content.decode("utf-8")
        data = json.loads(content)
        self.__lookup_snapshot_id[symbol] = data["lastUpdateId"]
        self.__log_message(content)
        return

    def start(self) -> bool:
        self.__connect()
        return True

    def stop(self) -> bool:
        self.__ws.close()
        while not self.__closed:
            time.sleep(1)
        return True


def main():
    symbols = ["BTCUSDT", "ETHUSDT"]
    orderbook_client = OrderBookClient(symbols)
    orderbook_client.start()


if __name__ == '__main__':
    main()
