import time
import json
import pyperclip
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys




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

try:
    # Uruchom przeglądarkę z selenium-wire
    driver = webdriver.Chrome(service=service, options=options)
    print("Przeglądarka Opera uruchomiona pomyślnie!")
except Exception as e:
    print(f"Błąd uruchamiania: {e}")
    exit(1)

try:
    # Otwórz stronę logowania
    driver.get('https://www.tradingview.com/chart')
    wait = WebDriverWait(driver, 20)  # Czas czekania

    # # Kliknij "Sign in with Email"
    # email_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Email']")))
    # email_button.click()
    # print("Kliknięto 'Sign in with Email'.")
    #
    # # Wpisz email w aktywnym polu (bez szukania po nazwie)
    # time.sleep(2)
    # active_element = driver.switch_to.active_element  # Tylko pobierz aktualny fokus
    # active_element.send_keys('karlossu@gmail.com')
    # print(f"Wpisano email: {active_element.get_attribute('value')}")
    #
    # # Przejdź do hasła i wpisz hasło
    # active_element.send_keys(Keys.TAB)  # Przejdź do pola hasła
    # time.sleep(1)  # Pauza na stabilizację
    # active_element = driver.switch_to.active_element  # Zaktualizuj na nowe aktywne pole
    # active_element.send_keys('PASSWORD')
    # print(f"Wpisano hasło: {active_element.get_attribute('value')}")
    #
    #
    # # Kliknij "Sign in"
    # signin_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[.//span[contains(., 'Sign in')]]")))
    # signin_button.click()
    # print("Kliknięto 'Sign in'.")

    # time.sleep(5)  # Czekaj na przekierowanie
    # print("Logowanie zakończone – sprawdź status.")

except TimeoutException as e:
    print(f"Błąd logowania (Timeout): {e}")
except Exception as e:
    print(f"Nieoczekiwany błąd: {e}")









# # Monitorowanie WebSocket z poprawionym warunkiem
# seen_messages = set()
# iteration = 0
# while True:
#     iteration += 1
#     print(f"\n--- Iteracja {iteration} ---")
#     total_requests = len(driver.requests)
#     print(f"Liczba wszystkich requestów: {total_requests}")
#
#     ws_requests = [r for r in driver.requests if
#                    'wss' in r.url.lower() or 'websocket' in r.url.lower() or 'socket.io' in r.url.lower()]
#     print(f"Liczba potencjalnych WS requestów (filtr po URL): {len(ws_requests)}")
#
#     for request in driver.requests:
#         print(f"Request URL: {request.url}")
#         if 'wss' in request.url.lower() or 'websocket' in request.url.lower() or 'socket.io' in request.url.lower():  # Poprawiono 'r' na 'request'
#             print(f"WS Request URL: {request.url}")
#             if hasattr(request, 'ws_messages'):
#                 print(f"Liczba WS messages: {len(request.ws_messages)}")
#                 for msg in request.ws_messages:
#                     payload = msg.data if hasattr(msg, 'data') else str(msg)  # Poprawiony dostęp
#                     print(f"Surowy payload WS: {payload[:500]}...")  # Zwiększ do 500 znaków
#                     if payload and '"m":"study_loading"' in str(payload):  # Szukaj w stringu
#                         seen_messages.add(payload)
#                         print(f"\nZnaleziono study_loading w payload!")
#                         parts = payload.split('~m~')
#                         i = 0
#                         while i < len(parts):
#                             if parts[i].isdigit():
#                                 msg_len = int(parts[i])
#                                 json_str = parts[i + 1]
#                                 if len(json_str) >= msg_len:
#                                     json_part = json_str[:msg_len]
#                                     try:
#                                         data = json.loads(json_part)
#                                         if data.get('m') == 'study_loading':
#                                             full_msg = json.dumps(data, indent=2)
#                                             print(f"Znaleziono study_loading:\n{full_msg}")
#                                             pyperclip.copy(full_msg)
#                                             print("Wiadomość skopiowana do schowka!")
#                                     except json.JSONDecodeError:
#                                         print(f"Błąd parsowania: {json_str}")
#                                 i += 2
#                             else:
#                                 i += 1
#                     else:
#                         print("Brak study_loading w tej wiadomości.")
#             else:
#                 print("Brak atrybutu ws_messages w tym request.")
#
#     if len(ws_requests) == 0:
#         print("Brak WS requestów – upewnij się, że chart/study jest załadowany.")
#
#     time.sleep(2)  # Sprawdź co 2s

# Monitorowanie WebSocket z filtrem na prodata.tradingview.com/socket.io
seen_messages = set()
iteration = 0
while True:
    iteration += 1
    print(f"\n--- Iteracja {iteration} ---")
    total_requests = len(driver.requests)
    print(f"Liczba wszystkich requestów: {total_requests}")

    ws_requests = [r for r in driver.requests if r.url.lower().startswith('wss://prodata.tradingview.com/socket.io')]
    print(f"Liczba WS requestów z prodata.tradingview.com/socket.io: {len(ws_requests)}")

    for request in driver.requests:
        if request.url.lower().startswith('wss://prodata.tradingview.com/socket.io'):
            #print(f"WS Request URL: {request.url}")
            if hasattr(request, 'ws_messages'):
                print(f"Liczba WS messages: {len(request.ws_messages)}")
                for msg in request.ws_messages:
                    payload = msg.data if hasattr(msg, 'data') else str(msg)  # Poprawiony dostęp
                    #print(f"Surowy payload WS: {payload[:500]}...")  # Zwiększ do 500 znaków
                    if payload and '"m":"du","p":["cs' in str(payload):  # Szukaj w stringu
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
                                            full_msg = json.dumps(data, indent=2)
                                            print(f"Znaleziono study_loading:\n{full_msg}")
                                    except json.JSONDecodeError:
                                        print(f"Błąd parsowania: {json_str}")
                                i += 2
                            else:
                                i += 1
                    else:
                        print("Brak study_loading w tej wiadomości.")
            else:
                print("Brak atrybutu ws_messages w tym request.")

    if len(ws_requests) == 0:
        print("Brak WS requestów z prodata.tradingview.com/socket.io – upewnij się, że chart/study jest załadowany.")

    time.sleep(5)  # Sprawdź co 2s

    # Wykonaj kombinację klawiszy: przytrzymaj Tab i wciśnij Arrow Down
    actions = ActionChains(driver)
    actions.key_down(Keys.TAB)  # Przytrzymaj Tab
    actions.send_keys(Keys.ARROW_DOWN)  # Wciśnij strzałkę w dół
    actions.key_up(Keys.TAB)  # Zwolnij Tab
    actions.perform()
    print("Wysłano kombinację klawiszy: Tab + Arrow Down.")

    # Poczekaj na załadowanie nowej strony po akcji
    wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))  # Czekaj na ponowne załadowanie
    print("Nowa strona załadowana po akcji klawiszowej.")

    # Opcjonalna pauza, aby zobaczyć efekt
    time.sleep(2)  # Dodatkowa pauza na obserwację