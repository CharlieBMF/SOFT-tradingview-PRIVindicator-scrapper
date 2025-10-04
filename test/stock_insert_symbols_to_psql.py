import psycopg2
from datetime import date

# Parametry połączenia z bazą danych PostgreSQL
db_params = {
    'dbname': 'TradingView',
    'user': 'postgres',  # Replace with your PostgreSQL username
    'password': 'postgres',  # Replace with your PostgreSQL password
    'host': 'localhost',  # Adjust if your database is hosted elsewhere
    'port': '5432'  # Default PostgreSQL port
}

# Ładuj symbole z pliku
with open('stock_symbols_raw_list.txt', 'r') as file:
    symbols = [line.strip() for line in file if line.strip()]

# Nawiąż połączenie z bazą danych
try:
    conn = psycopg2.connect(**db_params)
    print("Połączenie z bazą danych nawiązane pomyślnie!")
except Exception as e:
    print(f"Błąd połączenia z bazą danych: {e}")
    exit(1)

try:
    cursor = conn.cursor()

    for symbol in symbols:
        try:
            # Sprawdź, czy symbol istnieje w tabeli
            check_query = """
            SELECT COUNT(*) FROM public."tTestSymbols" WHERE "Symbol" = %s
            """
            cursor.execute(check_query, (symbol,))
            count = cursor.fetchone()[0]

            if count == 0:
                # Symbol nie istnieje: Wstaw nowy wiersz z enabled = 1
                insert_query = """
                INSERT INTO public."tTestSymbols" ("Symbol", "UpdatedShortTerm", "UpdatedLongTerm", enabled)
                VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (symbol, date(1990, 1, 1), date(1990, 1, 1), True))
                print(f"Dodano symbol: {symbol} z datami 1990-01-01 i enabled=1")
            else:
                # Symbol istnieje: Aktualizuj enabled na 1
                update_query = """
                UPDATE public."tTestSymbols"
                SET enabled = %s
                WHERE "Symbol" = %s
                """
                cursor.execute(update_query, (True, symbol))
                print(f"Symbol istnieje: {symbol}, zaktualizowano enabled=1")

            conn.commit()
        except (Exception, psycopg2.Error) as error:
            print(f"Błąd operacji dla symbolu {symbol}: {error}")
            conn.rollback()
    # Sprawdzenie wszystkich symboli w tabeli i ustawienie enabled = 0 dla nieobecnych w pliku
    try:
        # Pobierz wszystkie symbole z tabeli
        cursor.execute('SELECT "Symbol" FROM public."tTestSymbols"')
        db_symbols = [row[0] for row in cursor.fetchall()]

        # Znajdź symbole, które są w tabeli, ale nie ma ich w pliku
        symbols_set = set(symbols)  # Konwersja na zbiór dla szybszego wyszukiwania
        symbols_to_disable = [db_symbol for db_symbol in db_symbols if db_symbol not in symbols_set]

        # Aktualizuj enabled na 0 dla symboli nieobecnych w pliku
        if symbols_to_disable:
            update_disabled_query = """
            UPDATE public."tTestSymbols"
            SET enabled = %s
            WHERE "Symbol" = %s
            """
            for symbol in symbols_to_disable:
                try:
                    cursor.execute(update_disabled_query, (False, symbol))
                    updated_rows = cursor.rowcount
                    print(f"Symbol nieobecny w pliku: {symbol}, zaktualizowano enabled=0 ({updated_rows} wierszy)")
                    conn.commit()
                except (Exception, psycopg2.Error) as error:
                    print(f"Błąd aktualizacji enabled dla symbolu {symbol}: {error}")
                    conn.rollback()
        else:
            print("Wszystkie symbole z tabeli znajdują się w pliku, brak symboli do wyłączenia (enabled=0)")

    except (Exception, psycopg2.Error) as error:
        print(f"Błąd podczas sprawdzania symboli w tabeli: {error}")
        conn.rollback()

finally:
    cursor.close()
    conn.close()
    print("Połączenie z bazą danych zamknięte.")