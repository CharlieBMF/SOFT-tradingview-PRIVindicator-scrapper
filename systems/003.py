import psycopg2
import pandas as pd
import time
from datetime import date

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
    FROM public."tStockSymbols"
    WHERE "enabled" = TRUE AND "UpdatedLongTerm" = '2025-10-01'
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

    # Fetch indicators for indicatorIndex=5, 22, and 24, ordered by TickerRelative ASC (oldest to newest)
    cur.execute("""
        SELECT "TickerRelative", "IndicatorIndex", "IndicatorValue"
        FROM public."tStock_IndicatorValues_Pifagor_Long"
        WHERE "idSymbol" = %s AND "IndicatorIndex" IN (5, 22, 24) AND "TickerRelative" > -250
        ORDER BY "TickerRelative" ASC, "IndicatorIndex"
    """, (symbol_id,))
    ind_rows = cur.fetchall()
    if not ind_rows:
        print(f"No indicator data for symbol {symbol}.")
        continue
    df_ind = pd.DataFrame(ind_rows, columns=['TickerRelative', 'indicatorIndex', 'indicatorValue'])
    # Pivot to have columns for ind_5, ind_22, and ind_24
    df_ind_pivot = df_ind.pivot(index='TickerRelative', columns='indicatorIndex', values='indicatorValue').reset_index()
    df_ind_pivot.columns = ['TickerRelative', 'ind_5', 'ind_22', 'ind_24']  # Rename for clarity

    # Fetch prices, ordered by TickerRelative ASC
    cur.execute("""
        SELECT "TickerRelative", "high", "low"
        FROM public."tStock_Prices"
        WHERE "idSymbol" = %s
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

    # Simulation variables for this symbol
    positions = []
    position_open = False
    total_shares = 0
    total_invested_symbol = 0
    num_purchases = 0
    open_tr = None
    max_value = 0
    daily_states = []  # (tr, zysk_strata)
    trailing_stop = 0.0
    trailing_active = False

    for _, row in df_data.iterrows():
        tr = row['TickerRelative']
        ind_22 = row['ind_22']
        ind_5 = row['ind_5']
        ind_24 = row['ind_24']
        current_price = row['avg_price']

        if position_open:
            current_value = total_shares * current_price
            zysk_strata = current_value - total_invested_symbol
            daily_states.append((tr, zysk_strata))

            # Check if sell triggered
            if ind_5 < -5:
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
                    'symbol': symbol
                })
                global_zysk += zysk
                global_invested += total_invested_symbol
                global_positions.append(positions[-1])
                print(f"Sold due to ind_5 < -5: zysk={zysk:.2f} ({length} days)")
                position_open = False
                total_shares = 0
                total_invested_symbol = 0
                num_purchases = 0
                open_tr = None
                max_value = 0
                daily_states = []
                continue

        # Check for buy (open or add to position)
        if ind_22 > 3:
            # Determine amount based on ind_24
            if ind_22 == 6:
                amount = 1.0
            elif ind_22 == 9:
                amount = 3.0
            else:
                continue  # Skip buy if ind_24 not in ranges

            buy_price = current_price
            shares_bought = amount / buy_price
            total_shares += shares_bought
            total_invested_symbol += amount
            if not position_open:
                position_open = True
                num_purchases = 1
                open_tr = tr
                trailing_stop = 0.0
                trailing_active = False
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
        # Assume last tr is 0
        last_tr = df_data['TickerRelative'].max()
        last_price = df_data[df_data['TickerRelative'] == last_tr]['avg_price'].item()
        current_value = total_shares * last_price
        zysk_strata = current_value - total_invested_symbol
        length = last_tr - open_tr
        positions.append({
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
        })
        print(f"Open position at end: value={current_value:.2f}, invested={total_invested_symbol:.2f}, zysk={zysk_strata:.2f}")

    # Symbol summary
    # print(f"\nSummary for {symbol}:")
    # total_zysk_symbol = sum(p['zysk'] for p in positions if p.get('status') != 'open')
    # print(f"Total zysk/strata (closed): {total_zysk_symbol:.2f}")
    # for p in positions:
    #     print(p)
    # if daily_states:
    #     print("Daily states:")
    #     for state in daily_states:
    #         print(state)

# Global summary
print("\n=== Global Summary ===")
total_realized_zysk = global_zysk
total_unrealized_zysk = sum(p['zysk'] for p in global_positions if p.get('status') == 'open')
total_zysk = total_realized_zysk + total_unrealized_zysk
print(f"Sumaryczny zrealizowany zysk: {total_realized_zysk:.2f} $")
print(f"Sumaryczny niezrealizowany zysk: {total_unrealized_zysk:.2f} $")
print(f"Sumaryczny całkowity zysk: {total_zysk:.2f} $")

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

open_positions = [p for p in global_positions if p.get('status') == 'open']
# if open_positions:
#     print("Open positions:")
#     for p in open_positions:
#         print(p)

# Additional summary: Max total invested across all symbols per TickerRelative
max_capital_used = 0.0
if global_invested_data:
    df_global_invested = pd.DataFrame(global_invested_data)
    grouped = df_global_invested.groupby('tr')['invested'].sum().reset_index()
    max_row = grouped.loc[grouped['invested'].idxmax()]
    max_capital_used = max_row['invested']
    print(f"\n=== Additional Summary ===")
    print(f"Największy łączny koszt otwartych pozycji dla wszystkich symboli: {max_capital_used:.2f} $ dla TickerRelative = {max_row['tr']}")
else:
    print("\n=== Additional Summary ===")
    print("No invested data available.")

if max_capital_used > 0:
    percent_zysk = (total_zysk / max_capital_used) * 100
else:
    percent_zysk = 0
print(f"Procentowy zysk (całkowity zysk / max zainwestowany kapitał): {percent_zysk:.2f}%")

# New summary: Invested per TickerRelative in order
if global_invested_data:
    print("\n=== Podsumowanie zainwestowanego kapitału po TickerRelative ===")
    grouped_sorted = grouped.sort_values('tr')
    for _, row in grouped_sorted.iterrows():
        print(f"TickerRelative {row['tr']}: zainwestowano {row['invested']:.2f} $")
else:
    print("\n=== Podsumowanie zainwestowanego kapitału po TickerRelative ===")
    print("No invested data available.")

# Verbal summary and statistics