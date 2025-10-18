import requests
import json
import logging
from tvDatafeed import TvDatafeed, Interval
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Założenie: API działa lokalnie na porcie 8000 (dostosuj, jeśli inne)
API_URL = "http://localhost:8000/api/stock"

# Initialize TvDatafeed
tv = TvDatafeed()

def fetch_enabled_symbols():
    """Fetch enabled symbols and their IDs from API."""
    try:
        response = requests.get(
            f"{API_URL}/symbols/with-state"
        )
        response.raise_for_status()
        symbols = response.json()
        logger.info(f"Fetched {len(symbols)} symbols")
        return [(symbol["id"], symbol["Symbol"]) for symbol in symbols]
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching symbols from API: {e}, Response: {response.text if 'response' in locals() else 'Brak odpowiedzi'}")
        return []

def fetch_data(exchange, symbol):
    """Fetch historical data for a given exchange and symbol."""
    try:
        data = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=Interval.in_1_minute,
            n_bars=1
        )
        logger.info(f"Fetched historical data for {exchange}:{symbol}")
        return data
    except Exception as error:
        logger.error(f"Error fetching historical data for {exchange}:{symbol}: {error}")
        return None

def main():
    # Fetch enabled symbols
    symbols = fetch_enabled_symbols()
    if not symbols:
        logger.error("No enabled symbols found or API error")
        return

    # Process each symbol
    for id_symbol, full_symbol in symbols:
        try:
            # Split symbol into exchange and symbol
            if ':' not in full_symbol:
                logger.error(f"Invalid symbol format: {full_symbol}")
                continue
            exchange, symbol = full_symbol.split(':')
            logger.info(f"Processing {exchange}:{symbol} (idSymbol: {id_symbol})")

            # Fetch historical data
            data = fetch_data(exchange, symbol)
            if data is not None and not data.empty:
                # Get the latest data point
                latest_data = data.iloc[-1]
                open_price = latest_data['open']
                high_price = latest_data['high']
                low_price = latest_data['low']
                close_price = latest_data['close']
                volume = latest_data['volume']
                timestamp = latest_data.name.to_pydatetime().replace(tzinfo=None)
                updated = datetime.now().replace(tzinfo=None)

                # Prepare data for API
                price_data = {
                    "idSymbol": id_symbol,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,
                    "timestamp": timestamp.isoformat(),
                    "updated": updated.isoformat()
                }

                # Send data to API
                response = requests.post(
                    f"{API_URL}/prices/real",
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(price_data)
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Updated/Inserted data for idSymbol: {id_symbol}, Status: {result['status']}")
            else:
                logger.warning(f"No data fetched for {exchange}:{symbol}")

        except ValueError as ve:
            logger.error(f"Error processing symbol {full_symbol}: {ve}")
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending data to API for idSymbol {id_symbol}: {e}, Response: {response.text if 'response' in locals() else 'Brak odpowiedzi'}")
            continue

    logger.info("Operation completed")

if __name__ == "__main__":
    main()