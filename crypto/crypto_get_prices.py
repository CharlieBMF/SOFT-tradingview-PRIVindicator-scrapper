import psycopg2
import logging
from tvDatafeed import TvDatafeed, Interval
import pandas as pd

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
    """Fetch enabled symbols and their IDs from tCryptoSymbols."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        logger.info("Connected to the database")

        query = """
        SELECT "id", "Symbol"
        FROM public."tCryptoSymbols"
        WHERE "enabled" = true
        """
        cursor.execute(query)
        symbols = cursor.fetchall()
        logger.info(f"Fetched {len(symbols)} enabled symbols")
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

def fetch_historical_data(exchange, symbol):
    """Fetch historical data for a given exchange and symbol."""
    try:
        data = tv.get_hist(
            symbol=symbol,
            exchange=exchange,
            interval=Interval.in_daily,
            n_bars=2500
        )
        logger.info(f"Fetched historical data for {exchange}:{symbol}")
        return data
    except Exception as error:
        logger.error(f"Error fetching historical data for {exchange}:{symbol}: {error}")
        return None

def insert_historical_data(id_symbol, data):
    """Insert historical data into tCrypto_Prices with TickerRelative after deleting existing records."""
    if data is None or data.empty:
        logger.warning(f"No data to insert for idSymbol {id_symbol}")
        return

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Check if records exist for idSymbol
        cursor.execute('SELECT COUNT(*) FROM public."tCrypto_Prices" WHERE "idSymbol" = %s', (id_symbol,))
        count = cursor.fetchone()[0]
        if count > 0:
            logger.info(f"Found {count} existing records for idSymbol {id_symbol}, deleting them")
            cursor.execute('DELETE FROM public."tCrypto_Prices" WHERE "idSymbol" = %s', (id_symbol,))
            conn.commit()
            logger.info(f"Deleted {count} records for idSymbol {id_symbol}")

        # Sort data by datetime descending (newest first)
        data = data.sort_index(ascending=False)
        # Calculate TickerRelative: 0 for newest, -1 for each day back
        data['TickerRelative'] = -data.index.to_series().diff(-1).dt.days.fillna(0).cumsum().astype(int)
        # Ensure newest date gets TickerRelative = 0
        data['TickerRelative'] = data['TickerRelative'] - data['TickerRelative'].iloc[0]

        query = """
        INSERT INTO public."tCrypto_Prices" ("idSymbol", "TickerRelative", "open", "high", "low", "close", "volume")
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for index, row in data.iterrows():
            cursor.execute(query, (
                id_symbol,
                row['TickerRelative'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ))
        conn.commit()
        logger.info(f"Inserted {len(data)} records for idSymbol {id_symbol}")

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error inserting data for idSymbol {id_symbol}: {error}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed")


def update_ticker_relative(id_symbol):
    """Update TickerRelative values for a given idSymbol based on id ascending order."""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        logger.info(f"Connected to the database for updating TickerRelative for idSymbol {id_symbol}")

        # Fetch records ordered by id ascending
        query = """
        SELECT "id"
        FROM public."tCrypto_Prices"
        WHERE "idSymbol" = %s
        ORDER BY "id" ASC
        """
        cursor.execute(query, (id_symbol,))
        records = cursor.fetchall()

        if not records:
            logger.warning(f"No records found for idSymbol {id_symbol}")
            return

        # Calculate TickerRelative: +1 for first, 0 for second, -1 for third, and so on
        for index, (record_id,) in enumerate(records):
            ticker_relative = -index  # +1 for first, 0 for second, -1 for third, etc.
            update_query = """
            UPDATE public."tCrypto_Prices"
            SET "TickerRelative" = %s
            WHERE "id" = %s
            """
            cursor.execute(update_query, (ticker_relative, record_id))
            #logger.info(f"Updated TickerRelative to {ticker_relative} for id {record_id}")

        conn.commit()
        logger.info(f"Successfully updated TickerRelative for {len(records)} records for idSymbol {id_symbol}")

    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error updating TickerRelative for idSymbol {id_symbol}: {error}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed")


def main():
    # Fetch enabled symbols
    symbols = fetch_enabled_symbols()
    if not symbols:
        logger.error("No enabled symbols found or database error")
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
            data = fetch_historical_data(exchange, symbol)
            if data is not None:
                print(f"\nHistorical data for {exchange}:{symbol}:")
                print(data)
                # Insert data into tCrypto_Prices
                insert_historical_data(id_symbol, data)
                # Update TickerRelative values
                update_ticker_relative(id_symbol)
            else:
                logger.warning(f"No data returned for {exchange}:{symbol}")


        except ValueError as ve:
            logger.error(f"Error processing symbol {full_symbol}: {ve}")
            continue



if __name__ == "__main__":
    main()