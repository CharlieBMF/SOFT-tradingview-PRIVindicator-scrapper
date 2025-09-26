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
OPERA_BINARY_PATH = r'C:\Users\kmarcinski\AppData\Local\Programs\Opera\opera.exe'  # Przykład ścieżki do Opera.exe
OPERADRIVER_PATH = r'C:\operadriver_win64\operadriver.exe'  # Pobierz z https://github.com/operasoftware/operachromiumdriver/releases i rozpakuj
OPERA_PROFILE_PATH = r'C:\Users\kmarcinski\AppData\Local\Opera Software\Opera Stable'

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
    driver = webdriver.Chrome(service=service, options=options)
    print("Przeglądarka Opera uruchomiona pomyślnie!")
except Exception as e:
    print(f"Błąd uruchamiania: {e}")
    exit(1)

try:
    # Otwórz stronę Stock Screener (zakładam, że to sekcja screenera)
    driver.get('https://www.tradingview.com/screener/')
    wait = WebDriverWait(driver, 20)

    # Poczekaj na załadowanie tabeli screenera (dostosuj selektor do tabeli)
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'tv-data-table')))  # Typowy klasa dla tabeli screenera
    print("Tabela screenera załadowana.")

    # Znajdź wszystkie wiersze tabeli z symbolami
    rows = driver.find_elements(By.XPATH, '//table[contains(@class, "tv-data-table")]//tr')
    print(f"Znaleziono {len(rows)} wierszy w tabeli.")

    # Lista do przechowywania unikalnych URL-i
    unique_urls = set()

    # Iteruj po każdym wierszu i najeżdżaj na symbol
    for index, row in enumerate(rows, 1):
        try:
            # Znajdź komórkę z symbolem (zwykle pierwsza kolumna)
            symbol_cell = row.find_element(By.XPATH, './/td[1]')  # Pierwsza kolumna, dostosuj indeks jeśli inaczej
            symbol = symbol_cell.text.strip()
            print(f"Przetwarzanie wiersza {index}: Symbol {symbol}")

            # Najeżdżaj na komórkę z symbolem
            actions = ActionChains(driver)
            actions.move_to_element(symbol_cell).perform()
            time.sleep(0.5)  # Daj czas na pojawienie się podpowiedzi

            # Spróbuj wyciągnąć URL z podpowiedzi lub atrybutów
            # 1. Sprawdź tooltip (title) lub data-href
            tooltip_url = symbol_cell.get_attribute('title') or symbol_cell.get_attribute('data-href')
            if tooltip_url and 'http' in tooltip_url:
                unique_urls.add(tooltip_url)
                print(f"Znaleziono URL z tooltip/data-href: {tooltip_url}")

            # 2. Sprawdź, czy podpowiedź generuje element z linkiem (np. div z klasą)
            try:
                hint_element = wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'hover-hint') or contains(@class, 'tooltip')]")))
                hint_url = hint_element.get_attribute('href') or hint_element.text
                if hint_url and 'http' in hint_url:
                    unique_urls.add(hint_url)
                    print(f"Znaleziono URL z podpowiedzi: {hint_url}")
            except TimeoutException:
                print(f"Brak podpowiedzi dla symbolu {symbol}")

        except NoSuchElementException:
            print(f"Nie znaleziono komórki symbolu w wierszu {index}")
        except Exception as e:
            print(f"Błąd przy przetwarzaniu wiersza {index}: {e}")

    # Wyświetl wszystkie unikalne URL-e
    print("\nZnalezione unikalne ścieżki URL:")
    for url in unique_urls:
        print(url)

except TimeoutException as e:
    print(f"Błąd timeout: {e}")
except Exception as e:
    print(f"Nieoczekiwany błąd: {e}")
finally:
    # Zamknij przeglądarkę po zakończeniu (opcjonalne, usuń jeśli chcesz zostawić otwartą)
    # driver.quit()
    print("Skrypt zakończony.")