import os
import json
import asyncio
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from binance.client import Client
from binance.enums import *
import pandas as pd
import ta

logging.basicConfig(level=logging.INFO)

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

class BinanceFuturesEngine:
    def __init__(self):
        self.api_key = ""
        self.api_secret = ""
        self.testnet = True
        self.client = None
        self.leverage = 10
        self.margin_usdt = 2.0
        self.is_running = False
        self.symbols = [
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", 
            "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "DOTUSDT", "LINKUSDT",
            "NEARUSDT", "APTUSDT", "ARBUSDT", "MATICUSDT", "LTCUSDT"
        ]

    def init_client(self, api_key, api_secret, testnet=True, leverage=10, margin=2.0):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.leverage = leverage
        self.margin_usdt = margin
        
        self.client = Client(self.api_key, self.api_secret, testnet=self.testnet)
        if self.testnet:
            self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
        
        self.is_running = True
        logging.info("Binance Engine Initialized!")

    def get_account_data(self):
        if not self.client:
            return {"balance": 0.0, "positions": []}
        try:
            acc_info = self.client.futures_account()
            usdt_balance = 0.0
            for asset in acc_info['assets']:
                if asset['asset'] == 'USDT':
                    usdt_balance = float(asset['walletBalance'])
                    break
            
            positions = []
            for pos in acc_info['positions']:
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
            logging.error(f"Account data error: {e}")
            return {"balance": 0.0, "positions": []}

    def fetch_klines(self, symbol, interval='1m', limit=100):
        klines = self.client.futures_klines(symbol=symbol, interval=interval, limit=limit)
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
            
            order = self.client.futures_create_order(
                symbol=symbol, side=side, type=ORDER_TYPE_MARKET, quantity=quantity
            )
            
            tp_price = round(price * (1 + 0.008) if side == 'BUY' else price * (1 - 0.008), 2)
            sl_price = round(price * (1 - 0.004) if side == 'BUY' else price * (1 + 0.004), 2)
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
            logging.error(f"Execution failed: {e}")
            return None

    async def start_loop(self):
        while True:
            if self.is_running and self.client:
                for symbol in self.symbols:
                    try:
                        df = self.fetch_klines(symbol)
                        signal = self.analyze_strategy(df)
                        if signal in ["BUY", "SELL"]:
                            self.execute_trade(symbol, signal)
                    except Exception as e:
                        logging.error(f"Loop error {symbol}: {e}")
            await asyncio.sleep(5)

engine = BinanceFuturesEngine()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(engine.start_loop())

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            raw_data = await websocket.receive_text()
            payload = json.loads(raw_data)
            
            if payload.get("action") == "INIT_CONFIG":
                engine.init_client(
                    api_key=payload.get("apiKey"),
                    api_secret=payload.get("secretKey"),
                    testnet=(payload.get("network") == "testnet"),
                    leverage=int(payload.get("leverage", 10)),
                    margin=float(payload.get("margin", 2.0))
                )
            
            acc_data = engine.get_account_data()
            response = {
                "balance": acc_data["balance"],
                "positions": acc_data["positions"],
                "status": "ACTIVE" if engine.is_running else "STOPPED"
            }
            await websocket.send_text(json.dumps(response))
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logging.info("Disconnected")
