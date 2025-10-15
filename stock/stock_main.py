import psycopg2
import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime, timedelta
import numpy as np


# Placeholder for database connection - adjust with your credentials
conn = psycopg2.connect(
    dbname="TradingView",  # e.g., 'postgres'
    user="postgres",         # e.g., 'postgres'
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Initialize TvDatafeed (no credentials for basic usage)
tv = TvDatafeed()

# Step 1: Fetch and filter symbols where enabled=True or status = 'open'
cur.execute("""
SELECT s.id, s."Symbol", s."UpdatedShortTerm", s."enabled"
FROM public."tStockSymbols" s
LEFT JOIN public."tStockState" st ON s.id = st."idSymbol"
WHERE s."enabled" = TRUE 
  AND (s."UpdatedShortTerm" = '2025-10-11' OR st.status = 'open') LIMIT 5
""")
symbols_rows = cur.fetchall()
df_symbols = pd.DataFrame(symbols_rows, columns=['id', 'symbol', 'updatedShortTerm', 'enabled'])

if df_symbols.empty:
    print("No symbols meet the criteria.")
    cur.close()
    conn.close()
    exit()

for _, symbol_row in df_symbols.iterrows():
    current_date = datetime.now().date()
    symbol_id = symbol_row['id']
    symbol = symbol_row['symbol']
    print(f"\n=== Processing symbol: {symbol} (ID: {symbol_id}) ===")

    # Fetch indicators for indicatorIndex=5,7,22,24, ordered by TickerRelative ASC
    cur.execute("""
        SELECT "TickerRelative", "IndicatorIndex", "IndicatorValue"
        FROM public."tStock_IndicatorValues_Pifagor_Short"
        WHERE "idSymbol" = %s AND "IndicatorIndex" IN (5, 7, 22, 24) AND "TickerRelative" > -20
        ORDER BY "TickerRelative" ASC, "IndicatorIndex" ASC 
    """, (symbol_id,))
    ind_rows = cur.fetchall()
    if not ind_rows:
        print(f"No indicator data for symbol {symbol}.")
        continue
    df_ind = pd.DataFrame(ind_rows, columns=['TickerRelative', 'indicatorIndex', 'indicatorValue'])
    # Pivot to have columns for ind_5, ind_7, ind_22, and ind_24
    df_ind_pivot = df_ind.pivot(index='TickerRelative', columns='indicatorIndex', values='indicatorValue').reset_index()
    df_ind_pivot.columns = ['TickerRelative', 'ind_5', 'ind_7', 'ind_22', 'ind_24']
    print(df_ind_pivot)

    # Step 2: Fetch additional data from tStockState
    cur.execute("""
            SELECT "idSymbol", "status", "buy", "shouldSell", "sell", "checked", "lastAction", "invested", "shares", 
            "maxValue", "amountBuySell"
            FROM public."tStockState"
            WHERE "idSymbol" = %s
        """, (symbol_id,))
    state_rows = cur.fetchall()
    if state_rows:
        df_state = pd.DataFrame(state_rows,
                                columns=['idSymbol', 'status', 'buy', 'shouldSell', 'sell', 'checked', 'lastAction',
                                         'invested', 'shares', 'maxValue', 'amountBuySell'])
        print(f"\nState data for symbol {symbol}:")
        print(df_state)


    else:
        # Jeśli brak rekordu, tworzymy domyślny DataFrame z jedną wierszą i zadanymi wartościami
        default_data = {
            'idSymbol': [symbol_id],  # Zakładamy, że idSymbol ma być z symbol_id
            'status': ['close'],
            'buy': [False],
            'shouldSell': [False],  # Poprawiona literówka z zapytania użytkownika (souldSell -> shouldSell)
            'sell': [False],
            'checked': [current_date],  # current_date
            'lastAction': [datetime(1990, 1, 1, 0, 0, 0)],  # Timestamp z 1990-01-01 (użyłem datetime dla timestamp)
            'invested': [0],
            'shares': [0],
            'maxValue': [0],
            'amountBuySell': [0]
        }
        df_state = pd.DataFrame(default_data)

    # Check if lastAction is today (comparing dates from timestamps)
    last_action = df_state['lastAction'].iloc[0]
    print('LAST ACTION', last_action)
    if pd.notna(last_action) and last_action.date() == current_date:
        print(f"lastAction for {symbol} is today ({last_action.date()}). Skipping condition checks.")
        continue  # Skip to the next symbol


    """
    Updates or inserts stock price data for the given symbol_id and symbol.
    Skips if data was updated within the last minute.
    """
    # Fetch the updated timestamp to check existence and freshness
    cur.execute("""
        SELECT "updated"
        FROM public."tStock_PricesReal"
        WHERE "idSymbol" = %s
    """, (symbol_id,))

    result = cur.fetchone()
    current_time = datetime.now().replace(tzinfo=None)  # Naive datetime for consistency

    # Skip if row exists and updated time is within the last minute
    if result and result[0] >= (current_time - timedelta(minutes=1)):
        print(f"Data for {symbol} is fresh. Skipping update.")

    # Parse symbol format (expected: EXCHANGE:SYMBOL)
    if ':' not in symbol:
        print(f"Invalid symbol format for {symbol_id}: {symbol}. Skipping.")

    try:
        exchange, clean_symbol = symbol.split(':', 1)  # Split only once
    except ValueError:
        print(f"Failed to parse symbol: {symbol}. Skipping.")

    # Fetch latest 1-minute bar from TradingView
    try:
        data = tv.get_hist(
            symbol=clean_symbol,
            exchange=exchange,
            interval=Interval.in_1_minute,
            n_bars=1
        )
        print(f"Fetched data for {symbol}: {data}")  # Optional logging
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

    # Validate fetched data
    if data is None or data.empty:
        print(f"No data available for {symbol}. Skipping.")

    # Extract the latest bar (assuming data is sorted by time, ascending)
    latest_data = data.iloc[-1]

    # Extract values (handle potential NaN or invalid data)
    try:
        open_price = float(latest_data['open'])
        high_price = float(latest_data['high'])
        low_price = float(latest_data['low'])
        close_price = float(latest_data['close'])
        volume = int(latest_data['volume']) if pd.notna(latest_data['volume']) else 0
        timestamp = latest_data.name.to_pydatetime().replace(tzinfo=None)
        updated = current_time
    except (ValueError, KeyError, AttributeError) as e:
        print(f"Invalid data format for {symbol}: {e}. Skipping.")

    # Perform UPDATE or INSERT
    try:
        if result:
            # Update existing row
            cur.execute("""
                UPDATE public."tStock_PricesReal"
                SET "open" = %s, "high" = %s, "low" = %s, "close" = %s,
                    "volume" = %s, "timestamp" = %s, "updated" = %s
                WHERE "idSymbol" = %s
            """, (open_price, high_price, low_price, close_price, volume, timestamp, updated, symbol_id))
            print(f"Updated data for {symbol}.")
        else:
            # Insert new row (fixed: 8 placeholders and values)
            cur.execute("""
                INSERT INTO public."tStock_PricesReal"
                ("idSymbol", "open", "high", "low", "close", "volume", "timestamp", "updated")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (symbol_id, open_price, high_price, low_price, close_price, volume, timestamp, updated))
            print(f"Inserted new data for {symbol}.")

        conn.commit()
    except Exception as e:
        print(f"Database error for {symbol}: {e}")
        conn.rollback()  # Rollback on error

    current_price = close_price
    position = df_state['status'].iloc[0]
    buy = False if pd.isna(df_state['buy'].iloc[0]) else df_state['buy'].iloc[0]
    should_sell = False if pd.isna(df_state['shouldSell'].iloc[0]) else df_state['shouldSell'].iloc[0]
    sell = False if pd.isna(df_state['sell'].iloc[0]) else df_state['sell'].iloc[0]
    total_invested_symbol = float(df_state['invested'].iloc[0]) \
        if pd.notna(df_state['invested'].iloc[0]) else 0
    total_shares = float(df_state['shares'].iloc[0]) if pd.notna(df_state['shares'].iloc[0]) else 0
    recorded_max_value = float(df_state['maxValue'].iloc[0]) if pd.notna(df_state['maxValue'].iloc[0]) else 0
    amount_buysell = float(df_state['amountBuySell'].iloc[0]) if pd.notna(df_state['maxValue'].iloc[0]) else 0

    if buy or sell:
        continue
    print('GET DATA FOR INDICATORS')

    # Pobieranie zmiennych ind_5, ind_7, ind_22, ind_24 dla najnowszego TickerRelative
    df_ind_sorted = df_ind_pivot.sort_values(by='TickerRelative', ascending=False)
    if not df_ind_sorted.empty:
        latest_ind = df_ind_sorted.iloc[0]
        ind_5 = float(latest_ind['ind_5']) if pd.notna(latest_ind['ind_5']) else 0
        ind_7 = float(latest_ind['ind_7']) if pd.notna(latest_ind['ind_7']) else 0
        ind_22 = float(latest_ind['ind_22']) if pd.notna(latest_ind['ind_22']) else 0
        ind_24 = float(latest_ind['ind_24']) if pd.notna(latest_ind['ind_24']) else 0
        print(f"Wskaźniki dla {symbol}: ind_5={ind_5}, ind_7={ind_7}, ind_22={ind_22}, ind_24={ind_24}")
    else:
        ind_5 = ind_7 = ind_22 = ind_24 = 0
        print(f"Brak danych wskaźników dla {symbol} po pivotowaniu.")

    # Pobieranie 10 i 5 ostatnich wartości ind_5
    last_10_ind_5 = df_ind_sorted.head(10)['ind_5'].tolist()
    last_3_ind_5 = df_ind_sorted.head(3)['ind_5'].tolist()
    print(f"10 ostatnich ind_5 dla {symbol}: {last_10_ind_5}")
    print(f"5 ostatnich ind_5 dla {symbol}: {last_3_ind_5}")
    print(position)
    if position == 'open':
        print(position)
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
                amount_buysell = -current_value

    if ind_22 > 3 or ind_7 > 0:
        buy = True
        amount_buysell = 10.0

    # Before the INSERT/UPDATE block, ensure conversions
    buy_py = bool(buy) if isinstance(buy, (np.bool_, bool)) else buy
    should_sell_py = bool(should_sell) if isinstance(should_sell, (np.bool_, bool)) else should_sell
    sell_py = bool(sell) if isinstance(sell, (np.bool_, bool)) else sell
    # Update or insert into tStockState
    try:
        cur.execute("SELECT COUNT(*) FROM public.\"tStockState\" WHERE \"idSymbol\" = %s", (symbol_id,))
        if cur.fetchone()[0] == 0:
            cur.execute("""
                INSERT INTO public."tStockState"
                ("idSymbol", "status", "buy", "shouldSell", "sell", "checked", "lastAction", "invested", "shares", "maxValue", "amountBuySell")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
            symbol_id, position, buy_py, should_sell_py, sell_py, current_time, last_action, total_invested_symbol,
            total_shares, recorded_max_value, amount_buysell))
        else:
            cur.execute("""
                UPDATE public."tStockState"
                SET "buy" = %s, "shouldSell" = %s, "sell" = %s, "checked" = %s, "maxValue" = %s, "amountBuySell" = %s
                WHERE "idSymbol" = %s
            """, (buy_py, should_sell_py, sell_py, current_time, recorded_max_value, amount_buysell, symbol_id))
        conn.commit()
        print(f"Updated tStockState for {symbol}.")
    except Exception as e:
        print(f"Database error updating tStockState for {symbol}: {e}")
        conn.rollback()






