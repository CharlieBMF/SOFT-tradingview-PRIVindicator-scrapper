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

    # Fetch indicators for indicatorIndex=5,7,22,24, ordered by TickerRelative ASC (oldest to newest)
    cur.execute("""
        SELECT "TickerRelative", "IndicatorIndex", "IndicatorValue"
        FROM public."tStock_IndicatorValues_Pifagor_Long"
        WHERE "idSymbol" = %s AND "IndicatorIndex" IN (5, 7, 22, 24) AND "TickerRelative" > -250
        ORDER BY "TickerRelative" ASC, "IndicatorIndex" ASC 
    """, (symbol_id,))
    ind_rows = cur.fetchall()
    if not ind_rows:
        print(f"No indicator data for symbol {symbol}.")
        continue
    df_ind = pd.DataFrame(ind_rows, columns=['TickerRelative', 'indicatorIndex', 'indicatorValue'])
    # Pivot to have columns for ind_5, ind_7, ind_22 and ind_24
    df_ind_pivot = df_ind.pivot(index='TickerRelative', columns='indicatorIndex', values='indicatorValue').reset_index()
    df_ind_pivot.columns = ['TickerRelative', 'ind_5', 'ind_7', 'ind_22', 'ind_24']  # Rename for clarity

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
    shares_from_22 = 0
    invested_from_22 = 0
    shares_from_7 = 0
    invested_from_7 = 0
    # NOWE: Zmienne dla drobnych zakupów za 2$
    shares_from_small = 0
    invested_from_small = 0
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
        ind_7 = row['ind_7']
        ind_24 = row['ind_24']
        current_price = row['avg_price']

        # total_shares obejmuje wszystkie typy akcji
        total_shares = shares_from_22 + shares_from_7 + shares_from_small
        zysk_strata = 0  # Inicjalizacja
        if position_open:
            current_value = total_shares * current_price
            zysk_strata = current_value - total_invested_symbol
            daily_states.append((tr, zysk_strata))

            # Check sell conditions
            if ind_5 < -5:
                # Sell all (w tym small)
                current_value = total_shares * current_price
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
                    'sold_type': 'all_ind5_lt_minus5'
                })
                global_zysk += zysk
                global_invested += total_invested_symbol
                global_positions.append(positions[-1])
                print(f"Sold ALL (incl. small) due to ind_5 < -5: zysk={zysk:.2f} ({length} days)")
                position_open = False
                shares_from_22 = 0
                invested_from_22 = 0
                shares_from_7 = 0
                invested_from_7 = 0
                shares_from_small = 0
                invested_from_small = 0
                total_invested_symbol = 0
                num_purchases = 0
                open_tr = None
                max_value = 0
                daily_states = []
                continue
            elif ind_5 < 0 and ind_5 > -4:
                # POPRAWKA: Sprzedaż shares_from_7 (jak w oryginalnym skrypcie)
                if shares_from_7 > 0:
                    value_from_7 = shares_from_7 * current_price
                    zysk_from_7 = value_from_7 - invested_from_7
                    length = tr - open_tr
                    positions.append({
                        'open_tr': open_tr,
                        'close_tr': tr,
                        'length': length,
                        'zysk': zysk_from_7,
                        'percent_zysk': (zysk_from_7 / invested_from_7) * 100 if invested_from_7 > 0 else 0,
                        'num_purchases': num_purchases,
                        'max_value': max_value,
                        'final_invested': invested_from_7,
                        'symbol': symbol,
                        'sold_type': 'partial_ind7_ind5_bt_minus4_0'
                    })
                    global_zysk += zysk_from_7
                    global_invested += invested_from_7
                    global_positions.append(positions[-1])
                    print(f"Sold PARTIAL (ind_7) due to -4 < ind_5 < 0: zysk={zysk_from_7:.2f} ({length} days)")
                    shares_from_7 = 0
                    invested_from_7 = 0
                    total_invested_symbol = invested_from_22 + invested_from_small
                    # Check if position still open
                    if shares_from_22 == 0 and shares_from_small == 0:
                        position_open = False
                        num_purchases = 0
                        open_tr = None
                        max_value = 0
                        daily_states = []
                    # continue  # Usunięte, by sprawdzić small sell w tej samej iteracji

                # POPRAWKA: Sprzedaż shares_from_small (niezależna)
                if shares_from_small > 0:
                    value_from_small = shares_from_small * current_price
                    zysk_from_small = value_from_small - invested_from_small
                    length = tr - open_tr
                    positions.append({
                        'open_tr': open_tr,
                        'close_tr': tr,
                        'length': length,
                        'zysk': zysk_from_small,
                        'percent_zysk': (zysk_from_small / invested_from_small) * 100 if invested_from_small > 0 else 0,
                        'num_purchases': num_purchases,
                        'max_value': max_value,
                        'final_invested': invested_from_small,
                        'symbol': symbol,
                        'sold_type': 'partial_small_ind5_bt_minus4_0'
                    })
                    global_zysk += zysk_from_small
                    global_invested += invested_from_small
                    global_positions.append(positions[-1])
                    print(f"Sold PARTIAL (small) due to -4 < ind_5 < 0: zysk={zysk_from_small:.2f} ({length} days)")
                    shares_from_small = 0
                    invested_from_small = 0
                    total_invested_symbol = invested_from_22 + invested_from_7
                    # Check if position still open
                    if shares_from_22 == 0 and shares_from_7 == 0:
                        position_open = False
                        num_purchases = 0
                        open_tr = None
                        max_value = 0
                        daily_states = []
                continue  # Po sprzedaży partial (7 i/lub small), pomiń kupno

        # Check for main buy (open or add to position)
        main_buy_trigger = ind_22 > 3 or ind_7 > 0
        if main_buy_trigger:
            amount = 0.0
            buy_type = None
            if ind_22 == 6:
                amount = 10.0
                buy_type = 'ind_22'
            elif ind_22 == 9:
                amount = 10.0
                buy_type = 'ind_22'
            elif ind_7 == 1:
                amount = 10.0
                buy_type = 'ind_7'
            else:
                main_buy_trigger = False  # Jeśli nie pasuje do kwot

            if amount > 0:
                buy_price = current_price
                shares_bought = amount / buy_price
                if buy_type == 'ind_22':
                    shares_from_22 += shares_bought
                    invested_from_22 += amount
                elif buy_type == 'ind_7':
                    shares_from_7 += shares_bought
                    invested_from_7 += amount
                total_invested_symbol += amount
                if not position_open:
                    position_open = True
                    num_purchases = 1
                    open_tr = tr
                    trailing_stop = 0.0
                    trailing_active = False
                else:
                    num_purchases += 1
                current_value = (shares_from_22 + shares_from_7 + shares_from_small) * current_price
                max_value = max(max_value, current_value)
                zysk_strata = current_value - total_invested_symbol  # Aktualizacja po kupnie
                if position_open:
                    daily_states.append((tr, zysk_strata))
                print(f"{'Opened' if num_purchases == 1 else 'Added'} ({buy_type}): amount={amount}, price={buy_price:.2f}, shares={shares_bought:.4f}")
                print(f"Value={current_value:.2f}, invested={total_invested_symbol:.2f}, zysk={zysk_strata:.2f}")

        # NOWE: Small buy - jeśli pozycja otwarta, NIE ma głównego triggera i jesteśmy stratni (zysk_strata < 0)
        if position_open and not main_buy_trigger and zysk_strata < 0:
            amount = 15.0
            buy_type = 'small'
            buy_price = current_price
            shares_bought = amount / buy_price
            shares_from_small += shares_bought
            invested_from_small += amount
            total_invested_symbol += amount
            num_purchases += 1
            current_value = (shares_from_22 + shares_from_7 + shares_from_small) * current_price
            max_value = max(max_value, current_value)
            zysk_strata = current_value - total_invested_symbol
            daily_states.append((tr, zysk_strata))
            print(f"Added ({buy_type}): amount={amount}, price={buy_price:.2f}, shares={shares_bought:.4f}")
            print(f"Value={current_value:.2f}, invested={total_invested_symbol:.2f}, zysk={zysk_strata:.2f}")

        # Track invested if position open
        if position_open:
            global_invested_data.append({'tr': tr, 'invested': total_invested_symbol})

    # If position still open at end
    if position_open:
        # Assume last tr is 0
        last_tr = df_data['TickerRelative'].max()
        last_price = df_data[df_data['TickerRelative'] == last_tr]['avg_price'].item()
        total_shares = shares_from_22 + shares_from_7 + shares_from_small
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
        print(f"Open position at end (incl. small): value={current_value:.2f}, invested={total_invested_symbol:.2f}, zysk={zysk_strata:.2f}")

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
if open_positions:
    print("Open positions:")
    # for p in open_positions:
    #     print(p)

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

# POPRAWKA: Zaktualizowany opis werbalny
print("\n=== Summary of Script Operation ===")
print("The script analyzed trading performance based on historical data from the 'tStockSymbols', 'tStock_IndicatorValues_Pifagor_Long', and 'tStock_Prices' tables. It selected symbols with 'enabled=True' and 'updatedLongTerm' set to October 1, 2025. For each symbol, it monitored the indicator values for 'indicatorIndex=22', 'indicatorIndex=5', 'indicatorIndex=7', and 'indicatorIndex=24'. Positions are opened or added to when ind_22 > 3 or ind_7 > 0, with amounts based on specific values: $10 for ind_22=6, $30 for ind_22=9, $10 for ind_7=1. If a position is open, no main buy trigger occurs in subsequent TickerRelative, and the overall position is at a loss (zysk_strata < 0), add $2 small buy. Purchases are tracked separately based on whether triggered by ind_22, ind_7, or small. For selling: if ind_5 < -5, sell all shares (incl. small); if -4 < ind_5 < 0, sell shares bought via ind_7 (as in original) and separately sell small buys, each as distinct transactions. The script tracks daily profit/loss, total invested capital, number of purchases, and maximum position value, closing positions fully or partially based on sell triggers or leaving them open if data ended. Unrealized gains were calculated for open positions at the last 'TickerRelative=0'. The global summary provided total realized profit, unrealized profit, total profit, percentage gain calculated as (total profit / maximum capital used) * 100 where maximum capital used is the highest concurrent invested amount across all symbols at any TickerRelative, average position duration, longest position duration with its symbol, and the highest position value with its symbol. Additionally, it computed the maximum total cost of open positions across all symbols for each TickerRelative and reported the TickerRelative with the highest such cost. Finally, it provides a sequential summary of the total invested capital for each TickerRelative in ascending order.")

# Close connection
cur.close()
conn.close()