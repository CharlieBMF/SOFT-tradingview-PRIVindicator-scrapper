import psycopg2
import pandas as pd
import time
from datetime import date
from collections import deque, Counter

# Placeholder for database connection - adjust with your credentials
conn = psycopg2.connect(
    dbname="TradingView",  # e.g., 'postgres'
    user="postgres",         # e.g., 'postgres'
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Step 1: Fetch and filter symbols where enabled=True and updatedLongTerm='2025-10-01'
cur.execute("""
    SELECT id, "Symbol", "UpdatedLongTerm", "enabled" 
    FROM public."tCryptoSymbols"
    WHERE "enabled" = TRUE AND "UpdatedLongTerm" = '2025-10-05'
""")
symbols_rows = cur.fetchall()
df_symbols = pd.DataFrame(symbols_rows, columns=['id', 'symbol', 'updatedLongTerm', 'enabled'])

if df_symbols.empty:
    print("No symbols meet the criteria.")
    cur.close()
    conn.close()
    exit()

# Global stats
global_invested = 0
global_zysk = 0
global_positions = []
global_invested_data = []  # To track invested per TR per symbol for aggregation

for _, symbol_row in df_symbols.iterrows():
    symbol_id = symbol_row['id']
    symbol = symbol_row['symbol']
    print(f"\n=== Processing symbol: {symbol} (ID: {symbol_id}) ===")

    # Check for any invalid prices (high or low <= 0) for this symbol
    cur.execute("""
        SELECT COUNT(*) 
        FROM public."tCrypto_Prices"
        WHERE "idSymbol" = %s AND ("high" <= 0 OR "low" <= 0)
    """, (symbol_id,))
    invalid_price_count = cur.fetchone()[0]
    if invalid_price_count > 0:
        print(f"Skipping symbol {symbol}: Found {invalid_price_count} rows with high or low <= 0 in tCrypto_Prices.")
        continue

    # Fetch indicators for indicatorIndex=5,7,22,24, ordered by TickerRelative ASC
    cur.execute("""
        SELECT "TickerRelative", "IndicatorIndex", "IndicatorValue"
        FROM public."tCrypto_IndicatorValues_Pifagor_Long"
        WHERE "idSymbol" = %s AND "IndicatorIndex" IN (5, 7, 22, 24) AND "TickerRelative" > -50
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

    # Fetch prices, ordered by TickerRelative ASC, ensuring valid prices
    cur.execute("""
        SELECT "TickerRelative", "high", "low"
        FROM public."tCrypto_Prices"
        WHERE "idSymbol" = %s AND "high" > 0 AND "low" > 0
        ORDER BY "TickerRelative" ASC
    """, (symbol_id,))
    prices_rows = cur.fetchall()
    if not prices_rows:
        print(f"No price data for symbol {symbol}.")
        continue
    df_prices = pd.DataFrame(prices_rows, columns=['TickerRelative', 'high', 'low'])
    df_prices['avg_price'] = (df_prices['high'] + df_prices['low']) / 2

    # Merge on TickerRelative to ensure alignment
    df_data = pd.merge(df_ind_pivot, df_prices[['TickerRelative', 'avg_price']], on='TickerRelative', how='inner')
    if df_data.empty:
        print(f"No aligned data for symbol {symbol}.")
        continue

    # Additional check for invalid prices in df_data (redundant but for safety)
    invalid_prices = df_data[(df_data['avg_price'].isna()) | (df_data['avg_price'] <= 0)]
    if not invalid_prices.empty:
        print(f"Warning: Found {len(invalid_prices)} rows with invalid prices for {symbol}:")
        print(invalid_prices[['TickerRelative', 'avg_price']])
        continue  # Skip symbol if any invalid prices slip through

    # Simulation variables for this symbol
    positions = []
    position_open = False
    total_shares = 0
    total_invested_symbol = 0
    num_purchases = 0
    open_tr = None
    max_value = 0
    daily_states = []
    trailing_stop = 0.0
    trailing_active = False
    ind_5_below_minus_5_count = 0
    ind_5_last_values = deque(maxlen=10)
    sell_condition_triggered = False
    max_value_after_trigger = 0.0

    for _, row in df_data.iterrows():
        tr = row['TickerRelative']
        ind_22 = row['ind_22']
        ind_5 = row['ind_5']
        ind_7 = row['ind_7']
        ind_24 = row['ind_24']
        current_price = row['avg_price']

        # Safety check for current_price
        if pd.isna(current_price) or current_price <= 0:
            print(f"Skipping TR {tr} for {symbol}: Invalid price (current_price={current_price})")
            continue

        # Add ind_5 to deque (handle NaN as None)
        ind_5_last_values.append(ind_5 if pd.notna(ind_5) else None)

        if position_open:
            current_value = total_shares * current_price
            zysk_strata = current_value - total_invested_symbol
            daily_states.append((tr, zysk_strata))
            max_value = max(max_value, current_value)

            # --- SPRAWDZANIE WARUNKÓW SPRZEDAŻY ---
            should_trigger_sell = False
            sell_reason = ""

            # Count ind_5 < 0 in last 10 values
            valid_vals = [v for v in ind_5_last_values if v is not None]
            if len(valid_vals) >= 10:
                below_zero_count = sum(1 for v in valid_vals if v < 0)
                if below_zero_count >= 6 and zysk_strata >= 0:
                    should_trigger_sell = True
                    sell_reason = "ind_5 < 0 in at least 6 of last 10 TR"

            # Check if ind_5 < -5 for consecutive counts
            if pd.notna(ind_5):
                if ind_5 < -5:
                    ind_5_below_minus_5_count += 1
                else:
                    ind_5_below_minus_5_count = 0
            else:
                ind_5_below_minus_5_count = 0

            if pd.notna(ind_5):
                if zysk_strata >= 0:
                    if ind_5 < -7:
                        should_trigger_sell = True
                        sell_reason = "ind_5 < -7"
                    elif ind_5_below_minus_5_count >= 3:
                        should_trigger_sell = True
                        sell_reason = "ind_5 < -5 for three consecutive rows"
                else:
                    if ind_5 < -10:
                        should_trigger_sell = True
                        sell_reason = "ind_5 < -10 (at a loss)"

            if should_trigger_sell and not sell_condition_triggered:
                sell_condition_triggered = True
                max_value_after_trigger = current_value
                print(f"Sell condition triggered: {sell_reason}")
                continue

            if sell_condition_triggered:
                max_value_after_trigger = max(max_value_after_trigger, current_value)
                # Check if current value has dropped by at least 3% from max_value_after_trigger
                if current_value <= max_value_after_trigger * 0.915:
                    zysk = current_value - total_invested_symbol
                    length = tr - open_tr
                    positions.append({
                        'open_tr': open_tr,
                        'close_tr': tr,
                        'length': length,
                        'zysk': zysk,
                        'percent_zysk': (zysk / total_invested_symbol) * 100 if total_invested_symbol > 0 else 0,
                        'num_purchases': num_purchases,
                        'max_value': max_value,
                        'final_invested': total_invested_symbol,
                        'symbol': symbol,
                        'sell_reason': sell_reason
                    })
                    global_zysk += zysk
                    global_invested += total_invested_symbol
                    global_positions.append(positions[-1])
                    print(f"Sold due to {sell_reason} and 3% drop from max value after trigger: zysk={zysk:.2f} ({length} days)")
                    position_open = False
                    total_shares = 0
                    total_invested_symbol = 0
                    num_purchases = 0
                    open_tr = None
                    max_value = 0
                    daily_states = []
                    ind_5_below_minus_5_count = 0
                    ind_5_last_values.clear()
                    sell_condition_triggered = False
                    max_value_after_trigger = 0.0
                    continue

        # Check for buy (open or add to position)
        if (pd.notna(ind_22) and ind_22 > 3) or (pd.notna(ind_7) and ind_7 > 0):
            if ind_22 == 6:
                amount = 10.0
            elif ind_22 == 9:
                amount = 50.0  # Aligned with verbal summary
            elif ind_7 == 1:
                amount = 10.0
            else:
                continue

            buy_price = current_price
            # Safety check for buy_price
            if buy_price <= 0:
                print(f"Skipping buy at TR {tr} for {symbol}: Invalid buy_price={buy_price}")
                continue
            shares_bought = amount / buy_price
            total_shares += shares_bought
            total_invested_symbol += amount
            if not position_open:
                position_open = True
                num_purchases = 1
                open_tr = tr
                ind_5_below_minus_5_count = 0
                sell_condition_triggered = False
                max_value_after_trigger = 0.0
            else:
                num_purchases += 1
            current_value = total_shares * current_price
            max_value = max(max_value, current_value)
            zysk_strata = current_value - total_invested_symbol
            if position_open:
                daily_states.append((tr, zysk_strata))
            print(f"{'Opened' if num_purchases == 1 else 'Dokup'}: amount={amount}, price={buy_price:.2f}, shares={shares_bought:.4f}")
            print(f"Value={current_value:.2f}, invested={total_invested_symbol:.2f}, zysk={zysk_strata:.2f}")

        # Track invested if position open
        if position_open:
            global_invested_data.append({'tr': tr, 'invested': total_invested_symbol, 'symbol': symbol})

    # If position still open at end
    if position_open:
        last_tr = df_data['TickerRelative'].max()
        last_price_row = df_data[df_data['TickerRelative'] == last_tr]['avg_price']
        if last_price_row.empty or pd.isna(last_price_row.iloc[0]) or last_price_row.iloc[0] <= 0:
            print(f"Warning: Invalid last price for {symbol} at TR {last_tr}. Closing position without value.")
            current_value = 0
            zysk_strata = current_value - total_invested_symbol
        else:
            last_price = last_price_row.iloc[0]
            current_value = total_shares * last_price
            zysk_strata = current_value - total_invested_symbol
        length = last_tr - open_tr
        position = {
            'open_tr': open_tr,
            'close_tr': last_tr,
            'length': length,
            'zysk': zysk_strata,
            'percent_zysk': (zysk_strata / total_invested_symbol) * 100 if total_invested_symbol > 0 else 0,
            'num_purchases': num_purchases,
            'max_value': max_value,
            'final_invested': total_invested_symbol,
            'symbol': symbol,
            'status': 'open'
        }
        positions.append(position)
        global_positions.append(position)
        print(f"Open position at end: value={current_value:.2f}, invested={total_invested_symbol:.2f}, zysk={zysk_strata:.2f}")

    # Symbol summary
    print(f"\nSummary for {symbol}:")
    total_zysk_symbol = sum(p['zysk'] for p in positions if p.get('status') != 'open')
    print(f"Total zysk/strata (closed): {total_zysk_symbol:.2f}")
    for p in positions:
        print(p)

# Global summary
print("\n=== Global Summary ===")
total_realized_zysk = global_zysk
total_unrealized_zysk = sum(p['zysk'] for p in global_positions if p.get('status') == 'open')
total_zysk = total_realized_zysk + total_unrealized_zysk
print(f"Sumaryczny zrealizowany zysk: {total_realized_zysk:.2f} $")
print(f"Sumaryczny niezrealizowany zysk: {total_unrealized_zysk:.2f} $")
print(f"Sumaryczny całkowity zysk: {total_zysk:.2f} $")

# Summary of Profit/Loss for Open Positions
print("\n=== Podsumowanie Zysku/Straty Otwartych Pozycji ===")
open_positions = [p for p in global_positions if p.get('status') == 'open']
total_invested_open = sum(p['final_invested'] for p in open_positions)
percent_unrealized_zysk = (total_unrealized_zysk / total_invested_open) * 100 if total_invested_open > 0 else 0
print(f"Całkowity niezrealizowany zysk/strata (USD): {total_unrealized_zysk:.2f} $")
print(f"Całkowity procentowy zysk/strata otwartych pozycji: {percent_unrealized_zysk:.2f}%")
if open_positions:
    print("Szczegóły otwartych pozycji:")
    for p in open_positions:
        print(f"Symbol: {p['symbol']}, Zysk/Strata: {p['zysk']:.2f} USD, Procent: {p['percent_zysk']:.2f}%")
else:
    print("Brak otwartych pozycji.")

# List all open positions
if open_positions:
    print("\n=== Lista Otwartych Pozycji ===")
    for p in open_positions:
        print(f"dla symbolu {p['symbol']} pozycja otwarta przez {p['length']} dni o łącznej wartości {p['max_value']:.2f}, aktualny zysk/strata pozycji {p['zysk']:.2f} usd czyli {p['percent_zysk']:.2f}%")
else:
    print("\n=== Lista Otwartych Pozycji ===")
    print("Brak otwartych pozycji.")

# Count of positions by duration
print("\n=== Licznik Pozycji Według Czasu Otwarcia ===")
length_counts = Counter(p['length'] for p in global_positions)
for length, count in sorted(length_counts.items()):
    print(f"Pozycja była otwarta przez {length} dni: {count} razy")

# Top 10 longest open positions
print("\n=== Top 10 Najdłużej Otwartych Pozycji ===")
top_10_positions = sorted(global_positions, key=lambda p: p['length'], reverse=True)[:10]
if top_10_positions:
    for p in top_10_positions:
        print(f"pozycja na symbolu o nazwie {p['symbol']} była otwarta przez {p['length']} dni od TickerRelative {p['open_tr']} do TickerRelative {p['close_tr']} z liczbą dokupień {p['num_purchases']}")
else:
    print("Brak pozycji do wyświetlenia.")

closed_positions = [p for p in global_positions if 'status' not in p]
if closed_positions:
    avg_length = sum(p['length'] for p in closed_positions) / len(closed_positions)
    max_length = max(p['length'] for p in closed_positions)
    max_value_pos = max(closed_positions, key=lambda p: p['max_value'])
    profitable_closes = sum(1 for p in closed_positions if p['zysk'] > 0)
    loss_closes = sum(1 for p in closed_positions if p['zysk'] < 0)
    print(f"Sredni czas otwarcia pozycji: {avg_length:.2f} days")
    print(f"Najdluzszy czas otwarcia pozycji: {max_length} days (symbol: {max_value_pos['symbol']})")
    print(f"Najwieksza wartosc pozycji: {max_value_pos['max_value']:.2f} $ (symbol: {max_value_pos['symbol']})")
    print(f"Liczba pozycji zamknietych na zysku: {profitable_closes}")
    print(f"Liczba pozycji zamknietych na stracie: {loss_closes}")
else:
    print("No closed positions.")

# Calculate total invested and max invested for percentage profits
total_invested_all = sum(p['final_invested'] for p in global_positions)
# Calculate maximum invested capital across all TRs
df_invested = pd.DataFrame(global_invested_data)
if not df_invested.empty:
    max_invested_series = df_invested.groupby('tr')['invested'].sum()
    max_invested_capital = max_invested_series.max()
    max_invested_tr = max_invested_series.idxmax()
else:
    max_invested_capital = 0
    max_invested_tr = None
# Existing percentage profit (total profit / total invested)
if total_invested_all > 0:
    percent_zysk = (total_zysk / total_invested_all) * 100
else:
    percent_zysk = 0
print(f"Procentowy zysk (całkowity zysk / całkowity zainwestowany kapitał): {percent_zysk:.2f}%")
# New percentage profit (total profit / max invested capital)
if max_invested_capital > 0:
    percent_zysk_max_invested = (total_zysk / max_invested_capital) * 100
else:
    percent_zysk_max_invested = 0
print(f"Procentowy zysk (całkowity zysk / max zainwestowany kapitał): {percent_zysk_max_invested:.2f}%")
# New requested metrics
if max_invested_capital > 0:
    percent_realized_zysk_max_invested = (total_realized_zysk / max_invested_capital) * 100
else:
    percent_realized_zysk_max_invested = 0
print(f"Procentowy zysk (całkowity zrealizowany zysk / max zainwestowany kapitał): {percent_realized_zysk_max_invested:.2f}%")
print(f"Największy łączny koszt otwartych pozycji dla wszystkich symboli: {max_invested_capital:.2f} dla TickerRelative = {max_invested_tr if max_invested_tr is not None else 'N/A'}")

# Verbal summary and statistics
print("\n=== Summary of Script Operation ===")
print("The script analyzed trading performance based on historical data from the 'tCryptoSymbols', 'tCrypto_IndicatorValues_Pifagor_Long', and 'tCrypto_Prices' tables. It selected symbols with 'enabled=True' and 'updatedLongTerm' set to October 5, 2025. Symbols with any high or low prices equal to zero in the tCrypto_Prices table were skipped entirely to avoid invalid calculations. For each valid symbol, it monitored the indicator values for 'indicatorIndex=22', 'indicatorIndex=5', 'indicatorIndex=7', and 'indicatorIndex=24'. A position was opened or additional shares were bought whenever 'indicatorIndex=22' exceeded 3 or 'indicatorIndex=7' exceeded 0, for an amount determined by specific values: $10 if ind_22=6 or ind_7=1, $50 if ind_22=9, regardless of an existing open position. A sell condition was triggered when either: (1) the position was at a profit or break-even (profit/loss >= 0) and 'indicatorIndex=5' was less than -7 or less than -5 for three consecutive periods, or (2) the position was at a loss (profit/loss < 0) and 'indicatorIndex=5' was less than -10, or (3) 'indicatorIndex=5' was less than 0 in at least 6 of the last 10 periods and the position was at a profit or break-even. The actual sale only occurred when the position's value dropped by at least 3% from the maximum value recorded after the sell condition was triggered. The script tracked daily profit/loss, total invested capital, number of purchases, and maximum position value, closing positions when the sell condition and 3% drop were met or leaving them open if data ended. Unrealized gains were calculated for open positions at the last 'TickerRelative'. The global summary provided total realized profit, unrealized profit, total profit, percentage gain calculated as (total profit / total invested capital) * 100 where total invested capital is the sum of invested amounts across all positions, and percentage gain calculated as (total profit / maximum invested capital) * 100 where maximum invested capital is the peak sum of invested amounts across all symbols at any TickerRelative. It also included a count of positions by their duration (days open) and listed the top 10 longest open positions with their symbol, duration, TickerRelative range, and number of additional purchases.")

# Close connection
cur.close()
conn.close()