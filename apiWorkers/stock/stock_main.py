import time
import requests
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Założenie: API działa lokalnie na porcie 8000
API_URL = "http://localhost:8000/api/stock"

# Initialize TvDatafeed
tv = TvDatafeed()

def fetch_enabled_symbols():
    """Fetch symbols where (enabled=True or status='open') and requestStateCheck=True and UpdatedShortTerm=today."""
    try:
        response = requests.get(f"{API_URL}/symbols/with-short-state")
        response.raise_for_status()
        symbols = response.json()
        logger.info(f"Fetched {len(symbols)} symbols with short-state criteria")
        return symbols
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching symbols from API: {e}")
        return []

def get_state_data(symbol_id):
    """Fetch state data for a given symbol_id from API."""
    try:
        response = requests.get(f"{API_URL}/state/{symbol_id}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching state data for symbol_id {symbol_id}: {e}")
        return None

def update_prices(symbol_id, data):
    """Update or insert real-time price data via API."""
    try:
        response = requests.post(
            f"{API_URL}/prices/real",  # Zaktualizowana ścieżka
            json=data,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        logger.info(f"Updated prices for symbol_id {symbol_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating prices for symbol_id {symbol_id}: {e}. Response: {response.text if 'response' in locals() else 'No response'}")

def update_state(symbol_id, data):
    """Update state data via API."""
    try:
        # Dodaj idSymbol do danych, aby spełnić wymagania modelu
        full_data = {"idSymbol": symbol_id, **data}
        response = requests.post(
            f"{API_URL}/state/{symbol_id}",
            json=full_data,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        logger.info(f"Updated state for symbol_id {symbol_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating state for symbol_id {symbol_id}: {e}. Response: {response.text if 'response' in locals() else 'No response'}")
        return None

while True:
    symbols = fetch_enabled_symbols()
    if not symbols:
        logger.info("No symbols found, waiting 2 seconds")
        time.sleep(2)
        continue

    for symbol_data in symbols[:5]:  # Limit to 5 symbols
        current_date = datetime.now().date()
        symbol_id = symbol_data["id"]
        symbol = symbol_data["Symbol"]
        logger.info(f"\n=== Processing symbol: {symbol} (ID: {symbol_id}) ===")

        # Fetch indicator data via API
        try:
            response = requests.get(f"{API_URL}/indicators/short/{symbol_id}")
            response.raise_for_status()
            ind_data = response.json()  # ind_data to już lista obiektów
            if not ind_data:
                logger.warning(f"No indicator data for symbol {symbol}")
                continue
            df_ind = pd.DataFrame(ind_data)  # Bez "values", bo dane są na wierzchu
            df_ind_pivot = df_ind.pivot(index='TickerRelative', columns='IndicatorIndex',
                                        values='IndicatorValue').reset_index()
            df_ind_pivot.columns = ['TickerRelative', 'ind_5', 'ind_7', 'ind_22', 'ind_24']
            logger.info(f"Indicator data for {symbol}:\n{df_ind_pivot}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching indicator data for {symbol}: {e}")
            continue

        # Fetch state data
        df_state = get_state_data(symbol_id)
        if df_state is None:
            # Default data if no state exists
            df_state = {
                'idSymbol': symbol_id,
                'status': 'close',
                'buy': False,
                'shouldSell': False,
                'sell': False,
                'checked': current_date,
                'lastAction': datetime(1990, 1, 1, 0, 0, 0),
                'invested': 0,
                'shares': 0,
                'maxValue': 0,
                'amountBuySell': 0
            }
        else:
            df_state = pd.DataFrame([df_state])  # Convert to DataFrame for consistency

        last_action = df_state['lastAction'].iloc[0]
        # Konwersja last_action na datetime, jeśli jest stringiem
        if isinstance(last_action, str):
            try:
                last_action = datetime.strptime(last_action, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                logger.warning(f"Invalid lastAction format for symbol_id {symbol_id}: {last_action}. Using default.")
                last_action = datetime(1990, 1, 1, 0, 0, 0)

        logger.info(f'LAST ACTION: {last_action}')
        if pd.notna(last_action) and last_action.date() == current_date:
            logger.info(f"lastAction for {symbol} is today ({last_action.date()}). Skipping condition checks.")
            continue

        # Fetch latest 1-minute bar from TradingView
        if ':' not in symbol:
            logger.error(f"Invalid symbol format for {symbol_id}: {symbol}. Skipping.")
            continue
        exchange, clean_symbol = symbol.split(':', 1)

        try:
            data = tv.get_hist(
                symbol=clean_symbol,
                exchange=exchange,
                interval=Interval.in_1_minute,
                n_bars=1
            )
            if data is None or data.empty:
                logger.warning(f"No data available for {symbol}. Skipping.")
                continue
            latest_data = data.iloc[-1]
            open_price = float(latest_data['open'])
            high_price = float(latest_data['high'])
            low_price = float(latest_data['low'])
            close_price = float(latest_data['close'])
            volume = int(latest_data['volume']) if pd.notna(latest_data['volume']) else 0
            timestamp = latest_data.name.to_pydatetime().replace(tzinfo=None)
            current_time = datetime.now().replace(tzinfo=None)

            # Update prices
            update_prices(symbol_id, {
                "idSymbol": symbol_id,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "timestamp": timestamp.isoformat(),
                "updated": current_time.isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching or updating data for {symbol}: {e}")
            continue

        current_price = close_price
        position = df_state['status'].iloc[0]
        buy = df_state['buy'].iloc[0]
        should_sell = df_state['shouldSell'].iloc[0]
        sell = df_state['sell'].iloc[0]
        total_invested_symbol = float(df_state['invested'].iloc[0]) if pd.notna(df_state['invested'].iloc[0]) else 0
        total_shares = float(df_state['shares'].iloc[0]) if pd.notna(df_state['shares'].iloc[0]) else 0
        recorded_max_value = float(df_state['maxValue'].iloc[0]) if pd.notna(df_state['maxValue'].iloc[0]) else 0
        amount_buysell = float(df_state['amountBuySell'].iloc[0]) if pd.notna(df_state['amountBuySell'].iloc[0]) else 0

        if buy or sell:
            continue

        df_ind_sorted = df_ind_pivot.sort_values(by='TickerRelative', ascending=False)
        if not df_ind_sorted.empty:
            latest_ind = df_ind_sorted.iloc[0]
            ind_5 = float(latest_ind['ind_5']) if pd.notna(latest_ind['ind_5']) else 0
            ind_7 = float(latest_ind['ind_7']) if pd.notna(latest_ind['ind_7']) else 0
            ind_22 = float(latest_ind['ind_22']) if pd.notna(latest_ind['ind_22']) else 0
            ind_24 = float(latest_ind['ind_24']) if pd.notna(latest_ind['ind_24']) else 0
            logger.info(f"Wskaźniki dla {symbol}: ind_5={ind_5}, ind_7={ind_7}, ind_22={ind_22}, ind_24={ind_24}")
        else:
            ind_5 = ind_7 = ind_22 = ind_24 = 0
            logger.warning(f"Brak danych wskaźników dla {symbol} po pivotowaniu.")

        last_10_ind_5 = df_ind_sorted.head(10)['ind_5'].tolist()
        last_3_ind_5 = df_ind_sorted.head(3)['ind_5'].tolist()
        logger.info(f"10 ostatnich ind_5 dla {symbol}: {last_10_ind_5}")
        logger.info(f"3 ostatnich ind_5 dla {symbol}: {last_3_ind_5}")
        logger.info(f"Position: {position}")

        if position == 'open':
            current_value = total_shares * current_price
            zysk_strata = current_value - total_invested_symbol

            if not should_sell:
                should_sell_trigger = False
                if zysk_strata >= 0:
                    valid_vals_for_minus3 = [v for v in last_10_ind_5 if v is not None]
                    if len(valid_vals_for_minus3) == 10:
                        below_zero_count = sum(1 for v in valid_vals_for_minus3 if v < 0)
                        if below_zero_count >= 6:
                            should_sell_trigger = True
                    valid_vals_for_minus5 = [v for v in last_3_ind_5 if v is not None]
                    if len(valid_vals_for_minus5) == 3:
                        below_minus5_count = sum(1 for v in valid_vals_for_minus5 if v < -5)
                        if below_minus5_count == 3:
                            should_sell_trigger = True
                    if ind_5 < -7:
                        should_sell_trigger = True
                if ind_5 < -10:
                    should_sell_trigger = True
                if should_sell_trigger:
                    recorded_max_value = max(recorded_max_value, current_value)
                    should_sell = True

            if should_sell:
                recorded_max_value = max(recorded_max_value, current_value)
                if current_value <= recorded_max_value * 0.915:
                    sell = True
                    buy = False
                    amount_buysell = 0

        if ind_22 > 3 or ind_7 > 0:
            buy = True
            amount_buysell = 10.0

        # Update state
        update_state(symbol_id, {
            "status": position,
            "buy": bool(buy),
            "shouldSell": bool(should_sell),
            "sell": bool(sell),
            "checked": current_time.isoformat(),
            "lastAction": last_action.isoformat(),
            "invested": total_invested_symbol,
            "shares": total_shares,
            "maxValue": recorded_max_value,
            "amountBuySell": amount_buysell
        })