# -*- coding: utf-8 -*-
import psycopg2
import pandas as pd
import numpy as np
import random
import logging

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error

import gym
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv

logging.basicConfig(
    filename='trading_strategy.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.info("Starting trading strategy optimization script")

RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

indicator_indices = [5, 6, 7, 8, 9, 11, 13, 15, 17, 19, 22, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36]

# --- Połączenie z bazą ---
try:
    conn = psycopg2.connect(dbname="TradingView", user="postgres", password="postgres", host="localhost", port="5432")
    cur = conn.cursor()
    logging.info("Connected to PostgreSQL database")
except Exception as e:
    logging.exception("Failed to connect to DB")
    raise

# --- Pobranie symboli ---
cur.execute("""
    SELECT DISTINCT id, "Symbol"
    FROM public."tStockSymbols"
    WHERE "enabled" = TRUE AND "UpdatedLongTerm" = '2025-10-01'
    LIMIT 5
""")
symbols = cur.fetchall()
symbols_df = pd.DataFrame(symbols, columns=['id', 'symbol'])
logging.info(f"Fetched {len(symbols_df)} unique symbols")

# --- Przetwarzanie danych ---
all_data = []
for i, (symbol_id, symbol) in enumerate(symbols_df.values, 1):
    logging.info(f"Processing symbol {symbol} ({i}/{len(symbols_df)})")
    cur.execute("""
        SELECT iv."TickerRelative", iv."IndicatorIndex", iv."IndicatorValue", p."high", p."low"
        FROM public."tStock_IndicatorValues_Pifagor_Long" iv
        LEFT JOIN public."tStock_Prices" p 
        ON iv."idSymbol" = p."idSymbol" AND iv."TickerRelative" = p."TickerRelative"
        WHERE iv."idSymbol" = %s
        ORDER BY iv."TickerRelative" ASC
    """, (symbol_id,))
    data = cur.fetchall()
    if not data:
        logging.warning(f"No data for symbol {symbol}")
        continue

    df = pd.DataFrame(data, columns=['tr', 'indicator_idx', 'indicator_val', 'high', 'low'])
    df_pivot = df.pivot(index='tr', columns='indicator_idx', values='indicator_val')
    df_pivot.columns = df_pivot.columns.astype(str)
    desired_cols = [str(idx) for idx in indicator_indices]
    df_pivot = df_pivot.reindex(columns=desired_cols, fill_value=0).reset_index()

    df_prices = df[['tr', 'high', 'low']].groupby('tr').mean().reset_index()
    df_symbol = pd.merge(df_pivot, df_prices, on='tr', how='left')
    df_symbol['high'] = df_symbol['high'].fillna(0)
    df_symbol['low'] = df_symbol['low'].fillna(0)
    df_symbol['avg_price'] = (df_symbol['high'] + df_symbol['low']) / 2
    df_symbol['symbol_id'] = symbol_id
    df_symbol['symbol'] = symbol
    all_data.append(df_symbol)

if not all_data:
    logging.error("No data available")
    raise SystemExit("No data to process.")

df_data = pd.concat(all_data, ignore_index=True).fillna(0)

features = [str(idx) for idx in indicator_indices] + ['avg_price']
df_data['price_change'] = df_data.groupby(['symbol_id', 'symbol'])['avg_price'].pct_change().replace([np.inf, -np.inf], 0).fillna(0)

ma_cols = []
for idx in indicator_indices:
    col_name = str(idx)
    ma_name = f'ma_{idx}'
    ma_cols.append(ma_name)
    df_data[ma_name] = df_data.groupby(['symbol_id', 'symbol'])[col_name].transform(
        lambda x: x.rolling(window=5, min_periods=1).mean()
    ).replace([np.inf, -np.inf], 0).fillna(0)

X_df = df_data[features + ['price_change'] + ma_cols].replace([np.inf, -np.inf], 0).fillna(0)
y = df_data.groupby(['symbol_id', 'symbol'])['avg_price'].shift(-5)
X_df = X_df.iloc[:-5].reset_index(drop=True)
y = y.iloc[:-5].reset_index(drop=True)

scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X_df)

X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, shuffle=False)
mask_train = ~y_train.isna()

gbr = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1, random_state=RANDOM_SEED)
gbr.fit(X_train[mask_train.values], y_train[mask_train])
y_pred = gbr.predict(X_test)
rmse = mean_squared_error(y_test.fillna(0), y_pred, squared=False)
logging.info(f"Supervised model RMSE: {rmse:.4f}")

# --- Środowisko RL ---
class TradingEnv(gym.Env):
    metadata = {"render.modes": ["human"]}

    def __init__(self, df, features, ma_cols, buy_thresholds, sell_thresholds, seed=None):
        super(TradingEnv, self).__init__()
        self.df = df.reset_index(drop=True)
        self.features = features
        self.ma_cols = ma_cols
        self.buy_thresholds = buy_thresholds
        self.sell_thresholds = sell_thresholds

        self.current_step = 0
        self.position = 0
        self.total_shares = 0.0
        self.invested = 0.0
        self.profit_history = []

        obs_shape = len(self.features)
        self.observation_space = gym.spaces.Box(low=-np.inf, high=np.inf, shape=(obs_shape,), dtype=np.float32)
        self.action_space = gym.spaces.Discrete(4)  # Hold, Buy small, Buy large, Sell

    def seed(self, seed=None):
        np.random.seed(seed)
        random.seed(seed)
        return [seed]

    def reset(self):
        self.current_step = 0
        self.position = 0
        self.total_shares = 0.0
        self.invested = 0.0
        self.profit_history = []
        return self._next_observation()



    def step(self, action):
        price = self.df.loc[self.current_step, 'avg_price']
        reward = 0
        done = False

        if self.current_step >= len(self.df) - 1:
            done = True

        state_series = self._next_observation()
        buy_condition = all(state_series[i] > v for i, v in enumerate(self.buy_thresholds.values()))
        sell_condition = all(state_series[i] < v for i, v in enumerate(self.sell_thresholds.values()))

        if action == 1 and self.position == 0 and buy_condition:
            shares = 1.0 / (price if price > 0 else 1.0)
            self.total_shares += shares
            self.invested += 1.0
            self.position = 1
            reward = -0.001
        elif action == 2 and self.position == 0 and buy_condition:
            shares = 3.0 / (price if price > 0 else 1.0)
            self.total_shares += shares
            self.invested += 3.0
            self.position = 1
            reward = -0.003
        elif action == 3 and self.position == 1 and (sell_condition or not buy_condition):
            current_value = self.total_shares * price
            profit = current_value - self.invested
            reward = profit - 0.001
            self.profit_history.append(profit)
            self.position = 0
            self.invested = 0.0
            self.total_shares = 0.0

        self.current_step += 1
        obs = self._next_observation() if not done else np.zeros(self.observation_space.shape, dtype=np.float32)

        return obs, reward, done, {}

    def _next_observation(self):
        row = self.df.iloc[self.current_step]
        obs = row[self.features].to_numpy(dtype=np.float32)
        return obs


    def render(self, mode="human"):
        print(f"Step: {self.current_step}, Position: {self.position}, Invested: {self.invested}, Shares: {self.total_shares}")

# --- Przygotowanie środowiska ---
buy_thresholds = {i: 0.5 for i in ma_cols}
sell_thresholds = {i: 0.5 for i in ma_cols}

env_df = df_data.copy().reset_index(drop=True)

env = DummyVecEnv([
    lambda: TradingEnv(env_df, features, ma_cols, buy_thresholds, sell_thresholds, seed=RANDOM_SEED)
])

# --- Uczenie PPO ---
model = PPO(
    "MlpPolicy",
    env,
    verbose=1,
    n_steps=2048,
    batch_size=64,
    n_epochs=10,
    learning_rate=3e-4,
    seed=RANDOM_SEED
)

logging.info("Starting PPO training")
model.learn(total_timesteps=10000)
logging.info("PPO training completed")

# --- Testowanie modelu ---
env.seed(RANDOM_SEED)  # ustaw seed przed resetem
obs = env.reset()       # reset bez argumentu seed

done = False
total_reward = 0

while not done:
    action, _states = model.predict(obs, deterministic=True)
    obs, reward, done, info = env.step(action)
    total_reward += reward

logging.info(f"Total reward from test run: {total_reward}")

# --- Zapis modelu ---
model.save("ppo_trading_model")
logging.info("Model saved as ppo_trading_model.zip")

print("Training complete. Model saved.")

