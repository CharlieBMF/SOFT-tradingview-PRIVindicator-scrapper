import time
import json
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Dostosuj te ścieżki do swoich lokalizacji
OPERA_BINARY_PATH = r'/snap/opera/399/usr/lib/x86_64-linux-gnu/opera/opera'  # Przykład ścieżki do Opera.exe
OPERADRIVER_PATH = r'/home/czarli/Documents/operadriver_linux64/operadriver'  # Pobierz z https://github.com/operasoftware/operachromiumdriver/releases i rozpakuj
OPERA_PROFILE_PATH = r'/home/czarli/snap/opera/399/.config/opera/Default'

# Ustawienia dla Opera (używa ChromeDriver z binary Opera, bo Opera jest Chromium-based)
options = Options()
options.binary_location = OPERA_BINARY_PATH
options.add_argument('--start-maximized')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-cache')  # Ignoruje cache, jeśli problem z uprawnieniami
options.add_argument(f'--user-data-dir={OPERA_PROFILE_PATH}')  # Ładuje Twój profil
options.add_argument('--profile-directory=Default')  # Domyślny profil (zmień, jeśli używasz innego)
options.add_experimental_option('w3c', True)
options.add_argument('--disable-extensions')  # Wyłącz rozszerzenia, jeśli kolidują

service = Service(executable_path=OPERADRIVER_PATH)

with open('symbols_raw_list.txt', 'r') as file:
    symbols = [line.strip() for line in file if line.strip()]

try:
    # Uruchom przeglądarkę z selenium-wire
    driver = webdriver.Chrome(service=service, options=options)
    print("Przeglądarka Opera uruchomiona pomyślnie!")
except Exception as e:
    print(f"Błąd uruchamiania: {e}")
    exit(1)
with open('symbols_raw_list.txt', 'r') as file:
    symbols = [line.strip() for line in file if line.strip()]


# Monitorowanie WebSocket z filtrem na prodata.tradingview.com/socket.io
seen_messages = set()
iteration = 0
previous_request_count = 0
while True:
    # Get the current symbol (cycle through the list using modulo)
    current_symbol = symbols[iteration % len(symbols)]
    url = f'https://www.tradingview.com/chart/?symbol={current_symbol}'

    try:
        # Open the chart page with the current symbol
        driver.get(url)
        wait = WebDriverWait(driver, 20)  # Wait time
    except TimeoutException as e:
        print(f"Login error (Timeout) for {url}: {e}")
    except Exception as e:
        print(f"Unexpected error for {url}: {e}")

    iteration += 1
    print(f"\n--- Iteration {iteration} (Symbol: {current_symbol}) ---")
    total_requests = len(driver.requests)
    print(f"Total requests: {total_requests}")

    # Process only new requests since the previous iteration
    new_requests = driver.requests[previous_request_count:]
    print(f"Number of new requests: {len(new_requests)}")


    ws_requests = [r for r in new_requests if r.url.lower().startswith('wss://prodata.tradingview.com/socket.io')]
    print(f"Liczba WS requestów z prodata.tradingview.com/socket.io: {len(ws_requests)}")

    for request in ws_requests:
        if request.url.lower().startswith('wss://prodata.tradingview.com/socket.io'):
            #print(f"WS Request URL: {request.url}")
            if hasattr(request, 'ws_messages'):
                print(f"Liczba WS messages: {len(request.ws_messages)}")
                for msg in request.ws_messages:
                    payload = msg.data if hasattr(msg, 'data') else str(msg)  # Poprawiony dostęp
                    #print(f"Surowy payload WS: {payload[:500]}...")  # Zwiększ do 500 znaków
                    if payload and '"m":"du","p":["cs' in str(payload) and payload not in seen_messages:  # Szukaj w stringu
                        seen_messages.add(payload)
                        print(f"\nZnaleziono study_loading w payload!")
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
                                                    full_msg = json.dumps(data, indent=2)
                                                    print(f"Znaleziono study_loading z 'v' o długości 37 w st_data:\n{full_msg[:500]}{'...' if len(full_msg) > 500 else ''}")
                                                else:
                                                    print("Znaleziono study_loading, ale żaden 'v' nie ma dokładnie 37 wartości.")
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

    time.sleep(5)  # Sprawdź co 2s


    # Opcjonalna pauza, aby zobaczyć efekt
    time.sleep(2)  # Dodatkowa pauza na obserwację