#
#
# import numpy as np
# import websocket
# import json
# import threading
# import pyopencl as cl
# import time
# import requests
# import queue
# import os
# from concurrent.futures import ThreadPoolExecutor
#
# os.environ['PYOPENCL_NO_CACHE'] = '1'
#
#
# class OrderBookClient:
#     def __init__(self, symbols):
#         self.__base_uri = "wss://stream.binance.com:9443/ws"
#         self.__symbols = symbols
#         self.__message_queues = {symbol: queue.Queue() for symbol in symbols}
#         self.__snapshot_ids = {symbol: threading.Event() for symbol in symbols}
#         self.__update_ids = {symbol: None for symbol in symbols}
#         self.__lock = threading.Lock()
#         self.__processing_queue = queue.Queue()
#
#         self.__initialize_opencl()
#
#     def __initialize_opencl(self):
#         self.context = cl.create_some_context()
#         self.queue = cl.CommandQueue(self.context)
#         self.program = cl.Program(self.context, """
#         void atomic_add_float(volatile __global float *source, float operand) {
#             union {
#                 unsigned int intVal;
#                 float floatVal;
#             } newVal, prevVal;
#             do {
#                 prevVal.floatVal = *source;
#                 newVal.floatVal = prevVal.floatVal + operand;
#             } while (atomic_cmpxchg((volatile __global unsigned int *)source, prevVal.intVal, newVal.intVal) != prevVal.intVal);
#         }
#
#         __kernel void process_orderbook(__global const float *bids, __global const float *asks, __global float *results) {
#             int gid = get_global_id(0);
#             float bid_price = bids[gid * 2];
#             float bid_volume = bids[gid * 2 + 1];
#             float ask_price = asks[gid * 2];
#             float ask_volume = asks[gid * 2 + 1];
#
#             atomic_add_float(&results[0], bid_price * bid_volume);
#             atomic_add_float(&results[1], bid_volume);
#             atomic_add_float(&results[2], ask_price * ask_volume);
#             atomic_add_float(&results[3], ask_volume);
#         }
#         """).build()
#
#     def __create_ws_connection(self, symbol):
#         ws = websocket.WebSocketApp(
#             f"{self.__base_uri}/{symbol.lower()}@depth",
#             on_open=lambda ws: self.__on_open(ws, symbol),
#             on_message=lambda ws, msg: self.__on_message(ws, msg, symbol),
#             on_error=lambda ws, err: self.__on_error(ws, err, symbol),
#             on_close=lambda ws, code, msg: self.__on_close(ws, code, msg, symbol)
#         )
#         ws.run_forever()
#
#     def __on_open(self, ws, symbol):
#         print(f"Connection opened for {symbol}")
#         ws.send(json.dumps({
#             "method": "SUBSCRIBE",
#             "params": [f"{symbol.lower()}@depth"],
#             "id": 1
#         }))
#
#     def __on_message(self, ws, message, symbol):
#         try:
#             print(f"Received message for {symbol}: {message[:100]}...")
#             data = json.loads(message)
#             update_id_low = data.get("U")
#             update_id_upp = data.get("u")
#             if update_id_low is None:
#                 print(f"Warning: update_id_low is None for {symbol}")
#                 return
#
#             with self.__lock:
#                 if not self.__snapshot_ids[symbol].is_set():
#                     print(f"Snapshot not set for {symbol}, starting snapshot thread")
#                     threading.Thread(target=self.get_snapshot, args=(symbol,)).start()
#                     return
#
#                 stored_update_id = self.__update_ids[symbol]
#
#                 if stored_update_id is None:
#                     if update_id_low <= stored_update_id <= update_id_upp:
#                         print(f"First valid update for {symbol}")
#                         self.__update_ids[symbol] = update_id_upp
#                     else:
#                         print(
#                             f"Discarding update for {symbol}: update_id_low({update_id_low}) <= stored({stored_update_id}) <= update_id_upp({update_id_upp}) not satisfied")
#                         return
#                 elif update_id_low <= stored_update_id + 1:
#                     print(f"Valid subsequent update for {symbol}")
#                     self.__update_ids[symbol] = max(stored_update_id, update_id_upp)
#                 else:
#                     print(f"Missed updates for {symbol}")
#                     self.__snapshot_ids[symbol].clear()
#                     threading.Thread(target=self.get_snapshot, args=(symbol,)).start()
#                     return
#
#             self.__processing_queue.put((data, symbol))
#         except Exception as e:
#             print(f"Error in __on_message for {symbol}: {str(e)}")
#             import traceback
#             traceback.print_exc()
#
#     def __process_orderbook(self, data, symbol):
#         try:
#             bids = np.array(data['b'], dtype=np.float32).flatten()
#             asks = np.array(data['a'], dtype=np.float32).flatten()
#
#             if len(bids) == 0 or len(asks) == 0:
#                 print(f"Warning: Empty bids or asks for {symbol}")
#                 return None
#
#             bids_buffer = cl.Buffer(self.context, cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR, hostbuf=bids)
#             asks_buffer = cl.Buffer(self.context, cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR, hostbuf=asks)
#             results = np.zeros(4, dtype=np.float32)
#             results_buffer = cl.Buffer(self.context, cl.mem_flags.WRITE_ONLY, results.nbytes)
#
#             self.program.process_orderbook(self.queue, (bids.shape[0] // 2,), None, bids_buffer, asks_buffer,
#                                            results_buffer)
#             cl.enqueue_copy(self.queue, results, results_buffer).wait()
#
#             total_bid_value, total_bid_volume, total_ask_value, total_ask_volume = results
#
#             if total_bid_volume == 0 or total_ask_volume == 0:
#                 print(f"Warning: Zero volume for bids or asks in {symbol}")
#                 return None
#
#             vwap_bids = total_bid_value / total_bid_volume
#             vwap_asks = total_ask_value / total_ask_volume
#             imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume)
#
#             return {
#                 "symbol": symbol,
#                 "vwap_bids": float(vwap_bids),
#                 "vwap_asks": float(vwap_asks),
#                 "imbalance": float(imbalance)
#             }
#         except Exception as e:
#             print(f"Error in __process_orderbook for {symbol}: {str(e)}")
#             import traceback
#             traceback.print_exc()
#             return None
#
#     def __on_error(self, ws, error, symbol):
#         print(f"Encountered error for {symbol}: {error}")
#
#     def __on_close(self, ws, close_status_code, close_msg, symbol):
#         print(f"Connection closed for {symbol}: {close_status_code} - {close_msg}")
#
#     def get_snapshot(self, symbol):
#         url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"
#         try:
#             response = requests.get(url)
#             response.raise_for_status()
#             data = response.json()
#             with self.__lock:
#                 self.__update_ids[symbol] = data['lastUpdateId']
#                 self.__snapshot_ids[symbol].set()
#             print(f"Successfully got snapshot for {symbol}, lastUpdateId: {data['lastUpdateId']}")
#         except Exception as e:
#             print(f"Failed to get snapshot for {symbol}: {str(e)}")
#             import traceback
#             traceback.print_exc()
#
#     def start(self):
#         with ThreadPoolExecutor(max_workers=len(self.__symbols)) as executor:
#             executor.map(self.__create_ws_connection, self.__symbols)
#
#     def process_messages(self):
#         while True:
#             try:
#                 data, symbol = self.__processing_queue.get(timeout=0.1)
#                 processed_data = self.__process_orderbook(data, symbol)
#                 if processed_data:
#                     self.__message_queues[symbol].put(processed_data)
#                     print(f"Processed data for {symbol}: {processed_data}")
#                 else:
#                     print(f"Warning: __process_orderbook returned None for {symbol}")
#             except queue.Empty:
#                 pass
#             except Exception as e:
#                 print(f"Error in process_messages: {str(e)}")
#                 import traceback
#                 traceback.print_exc()
#
#
# if __name__ == "__main__":
#     symbols = ["BTCUSDT", "ETHUSDT"]
#     client = OrderBookClient(symbols)
#
#     threading.Thread(target=client.start, daemon=True).start()
#
#     client.process_messages()
#
#
#
# # [0-9] * [0-9]


import numpy as np
import websocket
import json
import threading
import pyopencl as cl
import time
import requests
import queue
import os
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
# import sqlite3
import matplotlib
matplotlib.use('TkAgg')

import msgpack
from collections import deque

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

os.environ['PYOPENCL_NO_CACHE'] = '1'


class Config:
    BASE_URI = "wss://stream.binance.com:9443/ws"
    SYMBOLS = ["BTCUSDT", "ETHUSDT"]
    MAX_DATA_POINTS = 100


class DataLogger:
    def __init__(self, max_messages=10000):
        self.message_queue = queue.Queue()
        self.message_buffer = deque(maxlen=max_messages)
        self.stop_event = threading.Event()
        self.logger_thread = threading.Thread(target=self._logger_loop, daemon=True)
        self.logger_thread.start()

    def log_message(self, message):
        self.message_queue.put(message)

    def _logger_loop(self):
        while not self.stop_event.is_set():
            try:
                message = self.message_queue.get(timeout=1)
                self.message_buffer.append(message)
                if len(self.message_buffer) % 1000 == 0:  # Save 1000 messages everytime
                    self._save_to_disk()
            except queue.Empty:
                continue

    def _save_to_disk(self):
        with open(f"market_data_{int(time.time())}.msgpack", "wb") as f:
            msgpack.dump(list(self.message_buffer), f)

    def stop(self):
        self.stop_event.set()
        self.logger_thread.join()
        self._save_to_disk()  # save the rest




class OrderBookClient:
    def __init__(self, symbols):
        self.__base_uri = Config.BASE_URI
        self.__symbols = symbols
        self.__message_queues = {symbol: queue.Queue() for symbol in symbols}
        self.__snapshot_ids = {symbol: threading.Event() for symbol in symbols}
        self.__update_ids = {symbol: None for symbol in symbols}
        self.__lock = threading.Lock()
        self.__processing_queue = queue.Queue()

        self.__recent_data = {symbol: {'timestamps': [], 'vwaps': [], 'imbalances': []} for symbol in symbols}
        self.__initialize_opencl()
        self.data_logger = DataLogger()


    def __create_table(self):
        cursor = self.__db_conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_book_data
            (symbol TEXT, timestamp INTEGER, vwap_bids REAL, vwap_asks REAL, imbalance REAL)
        ''')
        self.__db_conn.commit()

    def __initialize_opencl(self):
        self.context = cl.create_some_context()
        self.queue = cl.CommandQueue(self.context)
        self.program = cl.Program(self.context, """
        void atomic_add_float(volatile __global float *source, float operand) {
            union {
                unsigned int intVal;
                float floatVal;
            } newVal, prevVal;
            do {
                prevVal.floatVal = *source;
                newVal.floatVal = prevVal.floatVal + operand;
            } while (atomic_cmpxchg((volatile __global unsigned int *)source, prevVal.intVal, newVal.intVal) != prevVal.intVal);
        }

        __kernel void process_orderbook(__global const float *bids, __global const float *asks, __global float *results) {
            int gid = get_global_id(0);
            float bid_price = bids[gid * 2];
            float bid_volume = bids[gid * 2 + 1];
            float ask_price = asks[gid * 2];
            float ask_volume = asks[gid * 2 + 1];

            atomic_add_float(&results[0], bid_price * bid_volume);  // total bid value
            atomic_add_float(&results[1], bid_volume);              // total bid volume
            atomic_add_float(&results[2], ask_price * ask_volume);  // total ask value
            atomic_add_float(&results[3], ask_volume);              // total ask volume
        }
        """).build()

    def __create_ws_connection(self, symbol):
        ws = websocket.WebSocketApp(
            f"{self.__base_uri}/{symbol.lower()}@depth",
            on_open=lambda ws: self.__on_open(ws, symbol),
            on_message=lambda ws, msg: self.__on_message(ws, msg, symbol),
            on_error=lambda ws, err: self.__on_error(ws, err, symbol),
            on_close=lambda ws, code, msg: self.__on_close(ws, code, msg, symbol)
        )
        ws.run_forever()

    def __on_open(self, ws, symbol):
        logging.info(f"Connection opened for {symbol}")
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [f"{symbol.lower()}@depth"],
            "id": 1
        }))

    def __on_message(self, ws, message, symbol):
        try:
            data = json.loads(message)
            self.data_logger.log_message(message)
            update_id_low = data.get("U")
            update_id_upp = data.get("u")
            if update_id_low is None:
                logging.warning(f"Warning: update_id_low is None for {symbol}")
                return

            with self.__lock:
                if not self.__snapshot_ids[symbol].is_set():
                    logging.info(f"Snapshot not set for {symbol}, starting snapshot thread")
                    threading.Thread(target=self.get_snapshot, args=(symbol,)).start()
                    return

                stored_update_id = self.__update_ids[symbol]

                if stored_update_id is None:
                    if update_id_low <= stored_update_id <= update_id_upp:
                        logging.info(f"First valid update for {symbol}")
                        self.__update_ids[symbol] = update_id_upp
                    else:
                        logging.warning(f"Discarding update for {symbol}: update_id_low({update_id_low}) <= stored({stored_update_id}) <= update_id_upp({update_id_upp}) not satisfied")
                        return
                elif update_id_low <= stored_update_id + 1:
                    logging.info(f"Valid subsequent update for {symbol}")
                    self.__update_ids[symbol] = max(stored_update_id, update_id_upp)
                else:
                    logging.warning(f"Missed updates for {symbol}, resyncing...")
                    self.__snapshot_ids[symbol].clear()
                    threading.Thread(target=self.get_snapshot, args=(symbol,)).start()
                    return

            self.__processing_queue.put((data, symbol))
        except Exception as e:
            logging.error(f"Error in __on_message for {symbol}: {str(e)}", exc_info=True)

    def __process_orderbook(self, data, symbol):
        try:
            bids = np.array(data['b'], dtype=np.float32).flatten()
            asks = np.array(data['a'], dtype=np.float32).flatten()

            if len(bids) == 0 or len(asks) == 0:
                logging.warning(f"Warning: Empty bids or asks for {symbol}")
                return None

            bids_buffer = cl.Buffer(self.context, cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR, hostbuf=bids)
            asks_buffer = cl.Buffer(self.context, cl.mem_flags.READ_ONLY | cl.mem_flags.COPY_HOST_PTR, hostbuf=asks)
            results = np.zeros(4, dtype=np.float32)
            results_buffer = cl.Buffer(self.context, cl.mem_flags.WRITE_ONLY, results.nbytes)

            self.program.process_orderbook(self.queue, (bids.shape[0] // 2,), None, bids_buffer, asks_buffer,
                                           results_buffer)
            cl.enqueue_copy(self.queue, results, results_buffer).wait()

            total_bid_value, total_bid_volume, total_ask_value, total_ask_volume = results

            if total_bid_volume == 0 or total_ask_volume == 0:
                logging.warning(f"Warning: Zero volume for bids or asks in {symbol}")
                return None

            vwap_bids = total_bid_value / total_bid_volume
            vwap_asks = total_ask_value / total_ask_volume
            imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume)
            vwap = (vwap_bids + vwap_asks) / 2
            timestamp = int(time.time())
            with self.__lock:
                self.__recent_data[symbol]['timestamps'].append(timestamp)
                self.__recent_data[symbol]['vwaps'].append(vwap)
                self.__recent_data[symbol]['imbalances'].append(imbalance)

                # restrict in 100
                if len(self.__recent_data[symbol]['timestamps']) > Config.MAX_DATA_POINTS:
                    self.__recent_data[symbol]['timestamps'] = self.__recent_data[symbol]['timestamps'][-Config.MAX_DATA_POINTS:]
                    self.__recent_data[symbol]['vwaps'] = self.__recent_data[symbol]['vwaps'][-Config.MAX_DATA_POINTS:]
                    self.__recent_data[symbol]['imbalances'] = self.__recent_data[symbol]['imbalances'][-Config.MAX_DATA_POINTS:]

                return {
                    "symbol": symbol,
                    "vwap": float(vwap),
                    "imbalance": float(imbalance),
                    "timestamp": timestamp
                }
        except Exception as e:
            logging.error(f"Error in __process_orderbook for {symbol}: {str(e)}", exc_info=True)
            return None

    def get_recent_data(self, symbol):
        with self.__lock:
            return self.__recent_data[symbol]


    def __on_error(self, ws, error, symbol):
        logging.error(f"Encountered error for {symbol}: {error}")

    def __on_close(self, ws, close_status_code, close_msg, symbol):
        logging.info(f"Connection closed for {symbol}: {close_status_code} - {close_msg}")

    def get_snapshot(self, symbol):
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit=1000"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            with self.__lock:
                self.__update_ids[symbol] = data['lastUpdateId']
                self.__snapshot_ids[symbol].set()
            logging.info(f"Successfully got snapshot for {symbol}, lastUpdateId: {data['lastUpdateId']}")
        except Exception as e:
            logging.error(f"Failed to get snapshot for {symbol}: {str(e)}", exc_info=True)

    def start(self):
        with ThreadPoolExecutor(max_workers=len(self.__symbols)) as executor:
            executor.map(self.__create_ws_connection, self.__symbols)

    def process_messages(self):
        while True:
            try:
                data, symbol = self.__processing_queue.get(timeout=0.1)
                processed_data = self.__process_orderbook(data, symbol)
                if processed_data:
                    self.__message_queues[symbol].put(processed_data)
                    logging.info(f"Processed data for {symbol}: {processed_data}")
                else:
                    logging.warning(f"Warning: __process_orderbook returned None for {symbol}")
            except queue.Empty:
                pass
            except Exception as e:
                logging.error(f"Error in process_messages: {str(e)}", exc_info=True)

class Visualizer:
    def __init__(self, symbols):
        self.symbols = symbols
        self.fig, self.axes = plt.subplots(len(symbols), 2, figsize=(15, 5*len(symbols)))
        self.lines = {}

    def init_plot(self):
        for i, symbol in enumerate(self.symbols):
            self.lines[symbol] = {
                # 'vwap': self.axes[i, 0].plot([], [], 'r-', label='VWAP')[0],
                # 'imbalance': self.axes[i, 1].plot([], [], 'b-', label='Imbalance')[0]
                'vwap': self.axes[i, 0].plot([], [], 'r-')[0],
                'imbalance': self.axes[i, 1].plot([], [], 'b-')[0]
            }
            self.axes[i, 0].set_title(f'{symbol} VWAP')
            self.axes[i, 1].set_title(f'{symbol} Imbalance')
            self.axes[i, 0].legend()
            self.axes[i, 1].legend()
        return [line for symbol_lines in self.lines.values() for line in symbol_lines.values()]

    def update_plot(self, frame, client):
        for i, symbol in enumerate(self.symbols):
            data = client.get_recent_data(symbol)
            self.lines[symbol]['vwap'].set_data(data['timestamps'], data['vwaps'])
            self.lines[symbol]['imbalance'].set_data(data['timestamps'], data['imbalances'])
            self.axes[i, 0].relim()
            self.axes[i, 0].autoscale_view()
            self.axes[i, 1].relim()
            self.axes[i, 1].autoscale_view()
        return [line for symbol_lines in self.lines.values() for line in symbol_lines.values()]

def main():
    client = OrderBookClient(Config.SYMBOLS)
    try:
        visualizer = Visualizer(Config.SYMBOLS)

        client_thread = threading.Thread(target=client.start, daemon=True)
        client_thread.start()

        processing_thread = threading.Thread(target=client.process_messages, daemon=True)
        processing_thread.start()

        ani = FuncAnimation(visualizer.fig, visualizer.update_plot, frames=None,
                            init_func=visualizer.init_plot, fargs=(client,),
                            interval=1000, blit=True)
        plt.show()
        pass

    finally:
        client.data_logger.stop()

if __name__ == "__main__":
    main()
