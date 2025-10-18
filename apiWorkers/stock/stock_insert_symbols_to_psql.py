import requests
import json
from datetime import date

# Założenie: API działa lokalnie na porcie 8000 (dostosuj, jeśli inne)
API_URL = "http://localhost:8000/api/stock"

# Funkcja do konwersji daty na string ISO
def date_to_iso(date_obj):
    return date_obj.isoformat() if date_obj else None

# Ładuj symbole z pliku
with open('stock_symbols_raw_list.txt', 'r') as file:
    symbols = [line.strip() for line in file if line.strip()]

# Przygotuj dane do wysłania do API
symbols_data = [
    {
        "Symbol": symbol,
        "UpdatedShortTerm": date_to_iso(date(1990, 1, 1)),
        "UpdatedLongTerm": date_to_iso(date(1990, 1, 1)),
        "enabled": True,
        "requestStateCheck": False  # Domyślna wartość False dla nowych wpisów
    }
    for symbol in symbols
]

# Wyślij żądanie POST do API w celu wstawienia/aktualizacji symboli
try:
    response = requests.post(
        f"{API_URL}/symbols/batch",
        headers={"Content-Type": "application/json"},
        data=json.dumps({"symbols": symbols_data})
    )
    response.raise_for_status()  # Rzuć wyjątek, jeśli odpowiedź nie jest 200
    result = response.json()
    print("Połączenie z API nawiązane pomyślnie!")
    print(f"Status operacji: {result['status']}, Przetworzono: {result['processed']} symboli, Wyłączono: {result['disabled']}")
except requests.exceptions.RequestException as e:
    print(f"Błąd połączenia z API: {e}, Odpowiedź: {response.text if 'response' in locals() else 'Brak odpowiedzi'}")
    exit(1)

# Brak potrzeby osobnego sprawdzania symboli, ponieważ API obsługuje to w jednym endpointzie
print("Operacja zakończona. Połączenie z API zamknięte.")