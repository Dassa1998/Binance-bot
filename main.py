import os
import json
import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from binance.client import Client
from binance.enums import *
import pandas as pd
import ta

logging.basicConfig(level=logging.INFO)

app = FastAPI()

class BinanceFuturesEngine:
    def __init__(self):
        self.api_key = ""
        self.api_secret = ""
        self.testnet = True
        self.client = None
        self.leverage = 10
        self.margin_usdt = 2.0
        self.tp_pct = 0.008
        self.sl_pct = 0.004
        self.timeframe = '1m'
        self.is_running = False
        self.symbols = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
            "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT", "NEARUSDT"
        ]

    def init_client(self, config):
        self.api_key = config.get("apiKey", "")
        self.api_secret = config.get("secretKey", "")
        self.testnet = (config.get("network") == "testnet")
        self.leverage = int(config.get("leverage", 10))
        self.margin_usdt = float(config.get("margin", 2.0))
        self.tp_pct = float(config.get("tp", 0.8)) / 100.0
        self.sl_pct = float(config.get("sl", 0.4)) / 100.0
        self.timeframe = config.get("timeframe", "1m")
        
        try:
            self.client = Client(self.api_key, self.api_secret, testnet=self.testnet)
            if self.testnet:
                self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
            self.is_running = True
            logging.info("Bot Engine Started!")
        except Exception as e:
            logging.error(f"Start error: {e}")
            self.is_running = False

    def stop_engine(self):
        self.is_running = False
        logging.info("Bot Engine Stopped!")

    def panic_close_all(self):
        if not self.client:
            return
        try:
            acc_info = self.client.futures_account()
            for pos in acc_info.get('positions', []):
                amt = float(pos['positionAmt'])
                if amt != 0:
                    symbol = pos['symbol']
                    side = SIDE_SELL if amt > 0 else SIDE_BUY
                    self.client.futures_create_order(
                        symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=abs(amt)
                    )
            logging.info("All positions panic closed!")
        except Exception as e:
            logging.error(f"Panic close error: {e}")

    def get_account_data(self):
        if not self.client:
            return {"balance": 0.0, "positions": []}
        try:
            acc_info = self.client.futures_account()
            usdt_balance = 0.0
            for asset in acc_info.get('assets', []):
                if asset['asset'] == 'USDT':
                    usdt_balance = float(asset['walletBalance'])
                    break
            
            positions = []
            for pos in acc_info.get('positions', []):
                amt = float(pos['positionAmt'])
                if amt != 0:
                    positions.append({
                        "symbol": pos['symbol'],
                        "amount": amt,
                        "entryPrice": float(pos['entryPrice']),
                        "unrealizedProfit": float(pos['unrealizedProfit']),
                        "leverage": pos['leverage']
                    })
            return {"balance": usdt_balance, "positions": positions}
        except Exception as e:
            return {"balance": 0.0, "positions": []}

    def fetch_klines(self, symbol):
        klines = self.client.futures_klines(symbol=symbol, interval=self.timeframe, limit=100)
        df = pd.DataFrame(klines, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'
        ])
        df['close'] = df['close'].astype(float)
        return df

    def analyze_strategy(self, df):
        df['ema_fast'] = ta.trend.ema_indicator(df['close'], window=7)
        df['ema_slow'] = ta.trend.ema_indicator(df['close'], window=25)
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        if prev['ema_fast'] <= prev['ema_slow'] and last['ema_fast'] > last['ema_slow'] and last['rsi'] > 50:
            return "BUY"
        elif prev['ema_fast'] >= prev['ema_slow'] and last['ema_fast'] < last['ema_slow'] and last['rsi'] < 50:
            return "SELL"
        return "HOLD"

    def execute_trade(self, symbol, side):
        try:
            self.client.futures_change_leverage(symbol=symbol, leverage=self.leverage)
            self.client.futures_change_margin_type(symbol=symbol, marginType='ISOLATED')
            
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])
            quantity = round((self.margin_usdt * self.leverage) / price, 3)
            if quantity == 0:
                quantity = 0.001
            
            order = self.client.futures_create_order(
                symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=quantity
            )
            
            tp_price = round(price * (1 + self.tp_pct) if side == 'BUY' else price * (1 - self.tp_pct), 2)
            sl_price = round(price * (1 - self.sl_pct) if side == 'BUY' else price * (1 + self.sl_pct), 2)
            close_side = SIDE_SELL if side == 'BUY' else SIDE_BUY
            
            self.client.futures_create_order(
                symbol=symbol, side=close_side, type='TAKE_PROFIT_MARKET',
                stopPrice=tp_price, closePosition=True
            )
            self.client.futures_create_order(
                symbol=symbol, side=close_side, type='STOP_MARKET',
                stopPrice=sl_price, closePosition=True
            )
            return order
        except Exception as e:
            logging.error(f"Execution error {symbol}: {e}")
            return None

    async def start_loop(self):
        while True:
            if self.is_running and self.client:
                for symbol in self.symbols:
                    if not self.is_running:
                        break
                    try:
                        df = self.fetch_klines(symbol)
                        signal = self.analyze_strategy(df)
                        if signal in ["BUY", "SELL"]:
                            self.execute_trade(symbol, signal)
                    except Exception as e:
                        pass
            await asyncio.sleep(5)

engine = BinanceFuturesEngine()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start_loop())

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    if os.path.exists("index.html"):
        with open("index.html", "r", encoding="utf-8") as f:
            return f.read()
    elif os.path.exists("static/index.html"):
        with open("static/index.html", "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>Index.html not found!</h1>"

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            try:
                raw_data = await asyncio.wait_for(websocket.receive_text(), timeout=0.1)
                payload = json.loads(raw_data)
                action = payload.get("action")
                
                if action == "START":
                    engine.init_client(payload)
                elif action == "STOP":
                    engine.stop_engine()
                elif action == "PANIC_CLOSE":
                    engine.panic_close_all()
            except asyncio.TimeoutError:
                pass
            
            acc_data = engine.get_account_data()
            response = {
                "balance": acc_data["balance"],
                "positions": acc_data["positions"],
                "status": "ACTIVE" if engine.is_running else "STOPPED"
            }
            await websocket.send_text(json.dumps(response))
            await asyncio.sleep(1.5)
    except WebSocketDisconnect:
        logging.info("Client Disconnected")
