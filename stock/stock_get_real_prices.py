import psycopg2
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

# Database connection parameters
db_params = {
    'dbname': 'TradingView',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# Initialize TvDatafeed (no credentials for basic usage)
tv = TvDatafeed()

def fetch_enabled_symbols():
    """Fetch enabled symbols and their IDs from tStockSymbols."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        logger.info("Connected to the database")

        query = """
SELECT s.id, s."Symbol"
FROM public."tStockSymbols" s
LEFT JOIN public."tStockState" st ON s.id = st."idSymbol"
WHERE (s."enabled" = TRUE AND s."UpdatedShortTerm" = '2025-10-11') OR st.status = 'open'
        """
        cursor.execute(query)
        symbols = cursor.fetchall()
        print(symbols)
        logger.info(f"Fetched {len(symbols)} symbols")
        return [(symbol_id, symbol) for symbol_id, symbol in symbols]

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error fetching symbols: {error}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed")

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
        logger.error("No enabled symbols found or database error")
        return

    # Database connection
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    logger.info("Connected to the database for data insertion/update")

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
            print(data)
            if data is not None and not data.empty:
                # Get the latest data point (assuming 1 bar is fetched)
                latest_data = data.iloc[-1]
                open_price = latest_data['open']
                high_price = latest_data['high']
                low_price = latest_data['low']
                close_price = latest_data['close']
                volume = latest_data['volume']
                timestamp = latest_data.name.to_pydatetime().replace(
                    tzinfo=None)  # Convert to timestamp without timezone
                updated = datetime.now().replace(tzinfo=None)

                # Check if row exists for idSymbol
                cursor.execute("""
                    SELECT COUNT(*) FROM public."tStock_PricesReal" WHERE "idSymbol" = %s
                """, (id_symbol,))

                exists = cursor.fetchone()[0] > 0

                if exists:
                    # Update existing row
                    cursor.execute("""
                        UPDATE public."tStock_PricesReal"
                        SET "open" = %s, "high" = %s, "low" = %s, "close" = %s, 
                            "volume" = %s, "timestamp" = %s, "updated" = %s
                        WHERE "idSymbol" = %s
                    """, (open_price, high_price, low_price, close_price, volume, timestamp, updated, id_symbol))
                    logger.info(f"Updated data for idSymbol: {id_symbol}")
                else:
                    # Insert new row (assuming id is auto-incremented)
                    cursor.execute("""
                        INSERT INTO public."tStock_PricesReal" ("idSymbol", "open", "high", "low", "close", "volume", 
                            "timestamp", "updated")
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (id_symbol, open_price, high_price, low_price, close_price, volume, timestamp, updated))
                    logger.info(f"Inserted new data for idSymbol: {id_symbol}")

                # Commit the transaction
                conn.commit()
            else:
                logger.warning(f"No data fetched for {exchange}:{symbol}")

        except ValueError as ve:
            logger.error(f"Error processing symbol {full_symbol}: {ve}")
            continue
        except Exception as e:
            logger.error(f"Error processing data for idSymbol {id_symbol}: {e}")
            conn.rollback()

    # Close database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        logger.info("Database connection closed")



if __name__ == "__main__":
    main()