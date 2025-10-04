import time
import json
import os
import sys
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import psycopg2
from psycopg2.extras import execute_values
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
import keyboard

# Dostosuj te ścieżki do swoich lokalizacji
OPERA_BINARY_PATH = r'/snap/opera/401/usr/lib/x86_64-linux-gnu/opera/opera'  # Przykład ścieżki do Opera.exe
OPERADRIVER_PATH = r'/home/czarli/Documents/operadriver_linux64/operadriver'  # Pobierz z https://github.com/operasoftware/operachromiumdriver/releases i rozpakuj
OPERA_PROFILE_PATH = r'/home/czarli/snap/opera/399/.config/opera/Default'

# Ustawienia dla Opera (używa ChromeDriver z binary Opera, bo Opera jest Chromium-based)
options = Options()
options.binary_location = OPERA_BINARY_PATH
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-cache')  # Ignoruje cache, jeśli problem z uprawnieniami
options.add_argument(f'--user-data-dir={OPERA_PROFILE_PATH}')  # Ładuje Twój profil
options.add_argument('--profile-directory=Default')  # Domyślny profil (zmień, jeśli używasz innego)
options.add_experimental_option('w3c', True)
options.add_argument('--disable-extensions')  # Wyłącz rozszerzenia, jeśli kolidują

service = Service(executable_path=OPERADRIVER_PATH)

# Parametry połączenia z bazą danych PostgreSQL
db_params = {
    'dbname': 'TradingView',
    'user': 'postgres',  # Replace with your PostgreSQL username
    'password': 'postgres',  # Replace with your PostgreSQL password
    'host': 'localhost',  # Adjust if your database is hosted elsewhere
    'port': '5432'  # Default PostgreSQL port
}

try:
    conn = psycopg2.connect(**db_params)
    print("Połączenie z bazą danych nawiązane pomyślnie!")
except Exception as e:
    print(f"Błąd połączenia z bazą danych: {e}")
    exit(1)

try:
    conn_temp = psycopg2.connect(**db_params)
    cursor_temp = conn_temp.cursor()
    query = """
    SELECT "Symbol"
    FROM public."tStockSymbols"
    WHERE "enabled" = TRUE
    AND "UpdatedShortTerm" != CURRENT_DATE
    ORDER BY "UpdatedShortTerm" ASC
    """
    cursor_temp.execute(query)
    symbols = [row[0] for row in cursor_temp.fetchall()]
    cursor_temp.close()
    conn_temp.close()
    print(f"Pobrano {len(symbols)} symboli z tabeli tStockSymbols, posortowane rosnąco po UpdatedShortTerm")
    print(symbols)
except Exception as e:
    print(f"Błąd pobierania symboli z bazy danych: {e}")
    exit(1)

try:
    # Uruchom przeglądarkę z selenium-wire
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(100, 100)  # Ustawia rozmiar okna na 800x600 pikseli
    print("Przeglądarka Opera uruchomiona pomyślnie!")
except Exception as e:
    print(f"Błąd uruchamiania: {e}")
    exit(1)


# Monitorowanie WebSocket z filtrem na prodata.tradingview.com/socket.io
seen_messages = set()
iteration = 0
previous_request_count = 0
# Valid indices without 'NO' from the provided table
valid_indices = [5, 6, 7, 8, 9, 11, 13, 15, 17, 19, 22, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36]
#valid_indices = [i for i in range(0,100)]
restart_after_iterations = 50

while True:
    # Check for restart condition
    if iteration > 0 and iteration % restart_after_iterations == 0:
        print(f"Reached {iteration} iterations, restarting script...")
        # Close browser and database
        try:
            driver.close()
        except Exception as e:
            print(f"Error closing browser windows: {e}")
        # Quit WebDriver
        try:
            driver.quit()
            print("WebDriver closed.")
        except Exception as e:
            print(f"Error closing WebDriver: {e}")
        # Close database connection
        try:
            conn.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing database: {e}")
        # Restart the script
        os.execv(sys.executable, ['python3'] + sys.argv)

    # Flaga do ograniczenia przetwarzania tylko jednego study_loading na iterację
    found_study_loading = False
    # Get the current symbol (cycle through the list using modulo)
    current_symbol = symbols[iteration % len(symbols)]
    url = f'https://www.tradingview.com/chart/?symbol={current_symbol}'

    #Obsługa bazy danych
    cursor = conn.cursor()
    current_symbol_id = None  # Zmienna do przechowywania id bieżącego symbolu
    try:
        # Sprawdź, czy symbol istnieje w tabeli i pobierz id
        check_query = """
            SELECT id FROM public."tStockSymbols" WHERE "Symbol" = %s
            """
        cursor.execute(check_query, (current_symbol,))
        result = cursor.fetchone()

        if result:
            # Symbol istnieje: Pobierz id
            current_symbol_id = result[0]
            #print(f"Symbol istnieje: {current_symbol}, id: {current_symbol_id}")
        else:
            continue
    except (Exception, psycopg2.Error) as error:
        print(f"Błąd operacji na bazie danych dla symbolu {current_symbol}: {error}")
        continue
    finally:
        cursor.close()

    # Możesz użyć current_symbol_id w dalszej części pętli, np.:
    #print(f"Bieżące id dla symbolu {current_symbol}: {current_symbol_id}")


    try:
        # Open the chart page with the current symbol
        driver.get(url)
        wait = WebDriverWait(driver, 20)  # Wait time
    except TimeoutException as e:
        print(f"Login error (Timeout) for {url}: {e}")
    except Exception as e:
        print(f"Unexpected error for {url}: {e}")

    #time.sleep(2)

    iteration += 1
    print(f"\n--- Iteration {iteration} (Symbol: {current_symbol}) ---")
    total_requests = len(driver.requests)
    #print(f"Total requests: {total_requests}")

    # Process only new requests since the previous iteration
    new_requests = driver.requests[previous_request_count:]
    #print(f"Number of new requests: {len(new_requests)}")


    ws_requests = [r for r in new_requests if r.url.lower().startswith('wss://prodata.tradingview.com/socket.io')]
    #print(f"Liczba WS requestów z prodata.tradingview.com/socket.io: {len(ws_requests)}")

    for request in ws_requests:
        if request.url.lower().startswith('wss://prodata.tradingview.com/socket.io'):
            #print(f"WS Request URL: {request.url}")
            if hasattr(request, 'ws_messages'):
                #print(f"Liczba WS messages: {len(request.ws_messages)}")
                for msg in request.ws_messages:
                    # Przetwarzaj tylko, jeśli nie znaleziono jeszcze study_loading w tej iteracji
                    if found_study_loading:
                        continue
                    payload = msg.data if hasattr(msg, 'data') else str(msg)  # Poprawiony dostęp
                    #print(f"Surowy payload WS: {payload[:500]}...")  # Zwiększ do 500 znaków
                    if payload and '"m":"du","p":["cs' in str(payload) and payload not in seen_messages:  # Szukaj w stringu
                        seen_messages.add(payload)
                        #print(f"\nZnaleziono study_loading w payload!")
                        parts = payload.split('~m~')
                        #print(payload)
                        i = 0
                        while i < len(parts):
                            if parts[i].isdigit():
                                msg_len = int(parts[i])
                                json_str = parts[i + 1]
                                if len(json_str) >= msg_len:
                                    json_part = json_str[:msg_len]
                                    try:
                                        data = json.loads(json_part)
                                        if data.get('m') == 'du':
                                            try:
                                                st_data = data['p'][1].get(list(data['p'][1].keys())[0], {}).get('st', [])
                                                has_valid_v = any(len(item.get('v', [])) == 37 for item in st_data)
                                                if has_valid_v:
                                                    # Usuwanie istniejących wierszy z tStock_IndicatorValues_Pifagor_Short dla current_symbol_id
                                                    cursor = conn.cursor()
                                                    try:
                                                        delete_query = """
                                                        DELETE FROM public."tStock_IndicatorValues_Pifagor_Short"
                                                        WHERE "idSymbol" = %s
                                                        """
                                                        cursor.execute(delete_query, (current_symbol_id,))
                                                        deleted_rows = cursor.rowcount
                                                        print(f"Usunięto {deleted_rows} wierszy z tStock_IndicatorValues_Pifagor_Short dla idSymbol={current_symbol_id}")
                                                        conn.commit()
                                                    except (Exception, psycopg2.Error) as error:
                                                        print(f"Błąd usuwania wierszy z tStock_IndicatorValues_Pifagor_Short: {error}")
                                                        conn.rollback()
                                                    finally:
                                                        cursor.close()

                                                        # Filtrowanie st_data do elementów spełniających warunki
                                                        filtered_st_data = [
                                                            item for item in st_data
                                                            if len(item.get('v', [])) == 37
                                                               and isinstance(item.get('i'), (int, float))
                                                               and 0 <= item.get('i') <= 299
                                                        ]
                                                        inserted_data = 0
                                                        # Przetwarzanie wyfiltrowanych elementów
                                                        for item in reversed(filtered_st_data):
                                                            v_list = item.get('v', [])
                                                            i_value = item.get('i')

                                                            # Przygotowanie danych do wstawienia do tStock_IndicatorValues_Pifagor_Short
                                                            insert_data = []
                                                            for idx, value in enumerate(v_list):
                                                                if idx in valid_indices:  # Only include valid indices
                                                                    try:
                                                                        # Konwersja na float i sprawdzenie zakresu dla double precision
                                                                        value_float = float(value)
                                                                        value_float = round(value_float, 2)
                                                                        if abs(value_float) > 1e10:
                                                                            value_float = 1234.5678  # Zastąp wartości spoza zakresu na NULL
                                                                        insert_data.append((
                                                                            current_symbol_id,  # idSymbol
                                                                            int(i_value - len(filtered_st_data) + 1),
                                                                            # TickerRelative
                                                                            idx,  # IndicatorIndex
                                                                            value_float  # IndicatorValue
                                                                        ))
                                                                    except (ValueError, OverflowError):
                                                                        print(
                                                                            f"Błąd konwersji wartości {value} dla i={i_value}, idx={idx}, zapisano jako NULL")
                                                                        insert_data.append((
                                                                            current_symbol_id,
                                                                            i_value - len(filtered_st_data) + 1,
                                                                            idx,
                                                                            None
                                                                        ))

                                                            # Wstawianie wszystkich wierszy dla danego item
                                                            cursor = conn.cursor()
                                                            try:
                                                                insert_query = """
                                                                INSERT INTO public."tStock_IndicatorValues_Pifagor_Short" 
                                                                ("idSymbol", "TickerRelative", "IndicatorIndex", "IndicatorValue")
                                                                VALUES %s
                                                                """
                                                                execute_values(cursor, insert_query, insert_data)
                                                                conn.commit()
                                                                inserted_data = inserted_data + len(insert_data)
                                                            except (Exception, psycopg2.Error) as error:
                                                                print(
                                                                    f"Błąd wstawiania wierszy dla i={i_value}: {error}")
                                                                conn.rollback()
                                                            finally:
                                                                cursor.close()

                                                        print(f'Wstawiono {inserted_data} wierszy')
                                                        # Aktualizacja UpdatedShortTerm dla bieżącego symbolu
                                                        if current_symbol_id is not None:
                                                            cursor = conn.cursor()
                                                            try:
                                                                update_query = """
                                                                UPDATE public."tStockSymbols"
                                                                SET "UpdatedShortTerm" = CURRENT_DATE
                                                                WHERE id = %s
                                                                """
                                                                cursor.execute(update_query, (current_symbol_id,))
                                                                print(
                                                                    f"Zaktualizowano UpdatedShortTerm dla symbolu: {current_symbol}, id: {current_symbol_id}")
                                                                conn.commit()
                                                            except (Exception, psycopg2.Error) as error:
                                                                print(
                                                                    f"Błąd aktualizacji UpdatedShortTerm dla symbolu {current_symbol}: {error}")
                                                                conn.rollback()
                                                            finally:
                                                                cursor.close()


                                                    found_study_loading = True  # Znaleziono pasujące study_loading, pomiń kolejne wiadomości
                                                    break  # Przerwij pętlę while, bo mamy już pasujące dane
                                            except (KeyError, IndexError):
                                                print("Nieprawidłowa struktura danych w 'p' lub 'st'.")
                                    except json.JSONDecodeError:
                                        print(f"Błąd parsowania: {json_str}")
                                i += 2
                            else:
                                i += 1
                    else:
                        #print("Brak study_loading w tej wiadomości.")
                        pass
            else:
                #print("Brak atrybutu ws_messages w tym request.")
                pass

    if len(ws_requests) == 0:
        print("Brak WS requestów z prodata.tradingview.com/socket.io – upewnij się, że chart/study jest załadowany.")




    # Update previous_request_count for the next iteration
    previous_request_count = len(driver.requests)

