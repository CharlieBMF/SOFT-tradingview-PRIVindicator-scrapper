import json
import os
import sys
from seleniumwire import webdriver  # pip install selenium-wire
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
import requests
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Wyłącz logi selenium-wire na poziomie INFO
logging.getLogger("seleniumwire").setLevel(logging.WARNING)

# Dostosuj te ścieżki do swoich lokalizacji
OPERA_BINARY_PATH = r'/snap/opera/403/usr/lib/x86_64-linux-gnu/opera/opera'
OPERADRIVER_PATH = r'/home/czarli/Documents/operadriver_linux64/operadriver'
OPERA_PROFILE_PATH = r'/home/czarli/snap/opera/399/.config/opera/Default'

# Ustawienia dla Opera
options = Options()
options.binary_location = OPERA_BINARY_PATH
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-cache')
options.add_argument(f'--user-data-dir={OPERA_PROFILE_PATH}')
options.add_argument('--profile-directory=Default')
options.add_experimental_option('w3c', True)
options.add_argument('--disable-extensions')

service = Service(executable_path=OPERADRIVER_PATH)

# Założenie: API działa lokalnie na porcie 8000
API_URL = "http://localhost:8000/api/stock"

def fetch_enabled_symbols():
    """Fetch enabled symbols from API with enabled or open status."""
    try:
        response = requests.get(f"{API_URL}/symbols/with-state")
        response.raise_for_status()
        symbols = response.json()
        logger.info(f"Fetched {len(symbols)} symbols (enabled or open)")
        return [symbol["Symbol"] for symbol in symbols]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching symbols from API: {e}")
        return []

def insert_indicator_values(id_symbol, indicator_data):
    """Insert indicator values into tStock_IndicatorValues_Pifagor_Short via API."""
    try:
        response = requests.post(
            f"{API_URL}/indicators/short",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"values": indicator_data})
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Inserted {result['inserted']} indicator values for idSymbol {id_symbol}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error inserting indicator values for idSymbol {id_symbol}: {e}")

try:
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(100, 100)
    logger.info("Przeglądarka Opera uruchomiona")
except Exception as e:
    logger.error(f"Błąd uruchamiania: {e}")
    exit(1)

seen_messages = set()
iteration = 0
previous_request_count = 0
valid_indices = [5, 7, 22, 24]
restart_after_iterations = 50

symbols = fetch_enabled_symbols()
if not symbols:
    logger.error("No enabled symbols found or API error")
    driver.quit()
    exit(1)

while True:
    if iteration > 0 and iteration % restart_after_iterations == 0:
        logger.info(f"Restarting after {iteration} iterations")
        try:
            driver.close()
        except Exception as e:
            logger.error(f"Error closing browser: {e}")
        try:
            driver.quit()
            logger.info("WebDriver closed")
        except Exception as e:
            logger.error(f"Error closing WebDriver: {e}")
        os.execv(sys.executable, ['python3'] + sys.argv)

    found_study_loading = False
    current_symbol = symbols[iteration % len(symbols)]
    url = f'https://www.tradingview.com/chart/?symbol={current_symbol}'

    current_symbol_id = None
    try:
        response = requests.get(f"{API_URL}/symbols/with-state")
        response.raise_for_status()
        symbols_data = response.json()
        for symbol in symbols_data:
            if symbol["Symbol"] == current_symbol:
                current_symbol_id = symbol["id"]
                break
        if current_symbol_id is None:
            logger.error(f"Symbol {current_symbol} not found")
            iteration += 1
            continue
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching symbol ID for {current_symbol}: {e}")
        iteration += 1
        continue

    try:
        driver.get(url)
        WebDriverWait(driver, 20)
    except TimeoutException as e:
        logger.error(f"Timeout for {url}: {e}")
    except Exception as e:
        logger.error(f"Error for {url}: {e}")

    iteration += 1

    new_requests = driver.requests[previous_request_count:]
    ws_requests = [r for r in new_requests if r.url.lower().startswith('wss://prodata.tradingview.com/socket.io')]

    for request in ws_requests:
        if request.url.lower().startswith('wss://prodata.tradingview.com/socket.io'):
            if hasattr(request, 'ws_messages'):
                for msg in request.ws_messages:
                    if found_study_loading:
                        continue
                    payload = msg.data if hasattr(msg, 'data') else str(msg)
                    if payload and '"m":"du","p":["cs' in str(payload) and payload not in seen_messages:
                        seen_messages.add(payload)
                        parts = payload.split('~m~')
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
                                                    filtered_st_data = [
                                                        item for item in st_data
                                                        if len(item.get('v', [])) == 37
                                                           and isinstance(item.get('i'), (int, float))
                                                           and 0 <= item.get('i') <= 299
                                                    ]
                                                    indicator_data = []
                                                    for item in reversed(filtered_st_data):
                                                        v_list = item.get('v', [])
                                                        i_value = item.get('i')
                                                        for idx, value in enumerate(v_list):
                                                            if idx in valid_indices:
                                                                try:
                                                                    value_float = float(value)
                                                                    value_float = round(value_float, 2)
                                                                    if abs(value_float) > 1e10:
                                                                        value_float = 1234.5678
                                                                    indicator_data.append({
                                                                        "idSymbol": current_symbol_id,
                                                                        "TickerRelative": int(i_value - len(filtered_st_data) + 1),
                                                                        "IndicatorIndex": idx,
                                                                        "IndicatorValue": value_float
                                                                    })
                                                                except (ValueError, OverflowError):
                                                                    logger.error(f"Conversion error for value {value} at i={i_value}, idx={idx}")
                                                                    indicator_data.append({
                                                                        "idSymbol": current_symbol_id,
                                                                        "TickerRelative": int(i_value - len(filtered_st_data) + 1),
                                                                        "IndicatorIndex": idx,
                                                                        "IndicatorValue": None
                                                                    })

                                                    insert_indicator_values(current_symbol_id, indicator_data)
                                                    found_study_loading = True
                                                    break
                                            except (KeyError, IndexError):
                                                logger.error("Invalid data structure in 'p' or 'st'")
                                    except json.JSONDecodeError:
                                        logger.error(f"JSON parsing error: {json_str}")
                                i += 2
                            else:
                                i += 1

    if len(ws_requests) == 0:
        logger.warning("No WebSocket requests from prodata.tradingview.com")

    previous_request_count = len(driver.requests)