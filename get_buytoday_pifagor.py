import psycopg2
from datetime import date
import logging
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Parametry połączenia z bazą danych PostgreSQL
db_params = {
    'dbname': 'TradingView',
    'user': 'postgres',  # Replace with your PostgreSQL username
    'password': 'postgres',  # Replace with your PostgreSQL password
    'host': 'localhost',  # Adjust if your database is hosted elsewhere
    'port': '5432'  # Default PostgreSQL port
}

try:
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    logger.info("Connected to the database")

    # Get today's date
    today = date.today()
    logger.info(f"Querying for symbols with UpdatedShortTerm = {today}")

    # Fetch id and Symbol from tStockSymbols where UpdatedShortTerm is today
    query_hot_symbols = """
    SELECT id, "Symbol"
    FROM public."tStockSymbols"
    WHERE "UpdatedShortTerm" = %s
    """
    cursor.execute(query_hot_symbols, (today,))
    tstocksymbols_hot = cursor.fetchall()
    logger.info(f"Fetched {len(tstocksymbols_hot)} hot symbols: {tstocksymbols_hot}")

    # Filter tstocksymbols_hot based on conditions
    filtered_tstocksymbols_hot = []
    for symbol_id, symbol in tstocksymbols_hot:
        logger.info(f"Checking conditions for symbol: {symbol} (id: {symbol_id})")

        # Check if any row meets the conditions for any TickerRelative
        query_conditions = """
        WITH filtered AS (
            SELECT "TickerRelative"
            FROM public."tStock_IndicatorValues_Pifagor_Short"
            WHERE "idSymbol" = %s
            AND "TickerRelative" IN (0, -1)
            AND (
                ("IndicatorIndex" = 22 AND "IndicatorValue" > 3) OR
                ("IndicatorIndex" = 7 AND "IndicatorValue" > 0)
            )
            GROUP BY "TickerRelative"
            HAVING 
                COUNT(CASE WHEN "IndicatorIndex" = 22 AND "IndicatorValue" > 0 THEN 1 END) > 0
                AND COUNT(CASE WHEN "IndicatorIndex" IN (7, 8) AND "IndicatorValue" > 0 THEN 1 END) > 0
        )
        SELECT EXISTS (
            SELECT 1 FROM filtered
        )
        """
        try:
            cursor.execute(query_conditions, (symbol_id,))
            meets_conditions = cursor.fetchone()[0]  # True if any TickerRelative satisfies
            if meets_conditions:
                filtered_tstocksymbols_hot.append((symbol_id, symbol))
                logger.info(f"Symbol {symbol} meets conditions, keeping in tstocksymbols_hot")
            else:
                logger.info(f"Symbol {symbol} does not meet conditions, removing from tstocksymbols_hot")
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error checking conditions for symbol {symbol} (id: {symbol_id}): {error}")

    logger.info(f"Filtered tstocksymbols_hot: {len(filtered_tstocksymbols_hot)} symbols: {filtered_tstocksymbols_hot}")
    tstocksymbols_hot = filtered_tstocksymbols_hot  # Update tstocksymbols_hot with filtered list

    # Clear tStock_BuyToday_Pifagor
    try:
        cursor.execute('TRUNCATE TABLE public."tStock_BuyToday_Pifagor"')
        logger.info("Cleared table tStock_BuyToday_Pifagor")
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error clearing tStock_BuyToday_Pifagor: {error}")
        conn.rollback()
        raise

    # Insert filtered tstocksymbols_hot into tStock_BuyToday_Pifagor
    if tstocksymbols_hot:
        try:
            insert_query = """
            INSERT INTO public."tStock_BuyToday_Pifagor" ("idSymbol", "Symbol")
            VALUES %s
            """
            execute_values(cursor, insert_query, tstocksymbols_hot)
            logger.info(f"Inserted {len(tstocksymbols_hot)} rows into tStock_BuyToday_Pifagor")
            conn.commit()
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error inserting into tStock_BuyToday_Pifagor: {error}")
            conn.rollback()
            raise
    else:
        logger.info("No symbols to insert into tStock_BuyToday_Pifagor")

    # Dictionary to store rows for remaining symbols
    filtered_results = {}

    # Fetch rows for remaining symbols
    for symbol_id, symbol in tstocksymbols_hot:
        logger.info(f"Fetching rows for symbol: {symbol} (id: {symbol_id})")

        # Fetch rows where idSymbol = symbol_id, TickerRelative in (0, -5), and IndicatorIndex in (7, 8, 22, 24)
        query_indicators = """
        SELECT "idSymbol", "TickerRelative", "IndicatorIndex", "IndicatorValue"
        FROM public."tStock_IndicatorValues_Pifagor_Short"
        WHERE "idSymbol" = %s 
        AND "TickerRelative" IN (0, -4)
        AND "IndicatorIndex" IN (7, 8, 22, 24)
        """
        try:
            cursor.execute(query_indicators, (symbol_id,))
            indicator_rows = cursor.fetchall()
            if indicator_rows:
                filtered_results[symbol] = indicator_rows
                logger.info(f"Fetched {len(indicator_rows)} rows for symbol {symbol}")
            else:
                logger.info(f"No qualifying rows for symbol {symbol}")
        except (Exception, psycopg2.Error) as error:
            logger.error(f"Error fetching rows for symbol {symbol} (id: {symbol_id}): {error}")

    # Log and print final results
    logger.info(f"Final filtered results: {len(filtered_results)} symbols with qualifying rows")
    for symbol, rows in filtered_results.items():
        logger.info(f"Symbol: {symbol}")
        print(f"Symbol: {symbol}")
        print("Rows:")
        for row in rows:
            logger.info(
                f"  idSymbol: {row[0]}, TickerRelative: {row[1]}, IndicatorIndex: {row[2]}, IndicatorValue: {row[3]}")
            print(f"  idSymbol: {row[0]}, TickerRelative: {row[1]}, IndicatorIndex: {row[2]}, IndicatorValue: {row[3]}")
        print()  # Blank line for readability

except (Exception, psycopg2.Error) as error:
    logger.error(f"Error executing query: {error}")
finally:
    # Close database connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()
        logger.info("Database connection closed")

print(tstocksymbols_hot)