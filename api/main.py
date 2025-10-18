from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Date, DateTime, Float, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime
import logging
from enum import Enum
from psycopg2.extras import execute_values  # Dodane dla batch insertów

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Konfiguracja bazy danych (PostgreSQL)
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/TradingView"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency do sesji DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Enum dla typów aktywów
class AssetType(str, Enum):
    stock = "stock"
    crypto = "crypto"

# Mapper tabel
TABLE_MAPPER = {
    AssetType.stock: {
        "symbols": "1DtStockSymbols",
        "state": "1DtStockState",
        "prices_hist": "1DtStock_PricesHist",
        "prices_real": "1DtStock_PricesReal",
        "indicators_long": "1DtStock_IndicatorValues_Pifagor_Long",
        "indicators_short": "1DtStock_IndicatorValues_Pifagor_Short",
        "indicator_values_div_long": "1DtStock_IndicatorValues_div_Long",
        "indicator_values_div_short": "1DtStock_IndicatorValues_div_Short",
        "positions": "1DtStockPositions",
    },
    AssetType.crypto: {
        "symbols": "1DtCryptoSymbols",
        "state": "1DtCryptoState",
        "prices_hist": "1DtCrypto_PricesHist",
        "prices_real": "1DtCrypto_PricesReal",
        "indicators_long": "1DtCrypto_IndicatorValues_Pifagor_Long",
        "indicators_short": "1DtCrypto_IndicatorValues_Pifagor_Short",
        "indicator_values_div_long": "1DtCrypto_IndicatorValues_div_Long",
        "indicator_values_div_short": "1DtCrypto_IndicatorValues_div_Short",
        "positions": "1DtCryptoPositions",
    },
}

# Modele Pydantic
class SymbolBase(BaseModel):
    Symbol: str
    UpdatedShortTerm: Optional[date] = Field(default=date(1990, 1, 1))
    UpdatedLongTerm: Optional[date] = Field(default=date(1990, 1, 1))
    enabled: bool = True
    requestStateCheck: bool = False  # Domyślna wartość False

class SymbolResponse(SymbolBase):
    id: int

class StateBase(BaseModel):
    idSymbol: int
    status: str = "close"
    buy: bool = False
    shouldSell: bool = False
    sell: bool = False
    checked: Optional[datetime] = None
    lastAction: Optional[datetime] = Field(default=datetime(1990, 1, 1))
    invested: float = 0.0
    shares: float = 0.0
    maxValue: float = 0.0
    amountBuySell: float = 0.0

class PriceHistBase(BaseModel):
    idSymbol: int
    TickerRelative: int
    open: float
    high: float
    low: float
    close: float
    volume: float

class PriceRealBase(BaseModel):
    idSymbol: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: datetime
    updated: datetime

class IndicatorValueBase(BaseModel):
    idSymbol: int
    TickerRelative: int
    IndicatorIndex: int
    IndicatorValue: Optional[float] = None

# Modele dla batch operations
class BatchSymbols(BaseModel):
    symbols: List[SymbolBase]

class BatchPricesHist(BaseModel):
    prices: List[PriceHistBase]

class BatchIndicatorValues(BaseModel):
    values: List[IndicatorValueBase]

# Inicjalizacja FastAPI
app = FastAPI(
    title="TradingView API",
    description="Elastyczne API do zarządzania danymi TradingView dla różnych typów aktywów",
    version="1.0.0"
)

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Funkcje pomocnicze
def get_table_name(asset_type: AssetType, table_key: str) -> str:
    if asset_type not in TABLE_MAPPER or table_key not in TABLE_MAPPER[asset_type]:
        raise HTTPException(status_code=400, detail=f"Invalid asset_type or table_key: {asset_type}/{table_key}")
    return TABLE_MAPPER[asset_type][table_key]

def execute_query(db: Session, query: str, params: tuple = None, fetch: str = "none"):
    try:
        if params:
            result = db.execute(text(query), params)
        else:
            result = db.execute(text(query))
        if fetch == "all":
            return result.fetchall()
        elif fetch == "one":
            return result.fetchone()
        db.commit()
        return result
    except Exception as e:
        db.rollback()
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database operation failed")

# Endpointy

# 1. Fetch enabled symbols
@app.get("/api/{asset_type}/symbols/enabled", response_model=List[SymbolResponse])
def fetch_enabled_symbols(asset_type: AssetType, updated_short_term: Optional[date] = None, db: Session = Depends(get_db)):
    table = get_table_name(asset_type, "symbols")
    query = f"""
    SELECT id, "Symbol", "UpdatedShortTerm", "UpdatedLongTerm", enabled, "requestStateCheck"
    FROM public."{table}"
    WHERE enabled = TRUE
    """
    if updated_short_term:
        query += f" AND \"UpdatedShortTerm\" = :updated_short_term"
        params = {"updated_short_term": updated_short_term}
    else:
        params = None
    rows = execute_query(db, query, params, fetch="all")
    return [SymbolResponse(id=row[0], Symbol=row[1], UpdatedShortTerm=row[2], UpdatedLongTerm=row[3], enabled=row[4], requestStateCheck=row[5]) for row in rows]

# 2. Insert/Update symbols from list
@app.post("/api/{asset_type}/symbols/batch")
def insert_update_symbols(asset_type: AssetType, data: BatchSymbols, db: Session = Depends(get_db)):
    table = get_table_name(asset_type, "symbols")
    for symbol in data.symbols:
        check_query = f'SELECT COUNT(*) FROM public."{table}" WHERE "Symbol" = :symbol'
        count = execute_query(db, check_query, {"symbol": symbol.Symbol}, fetch="one")[0]
        if count == 0:
            insert_query = f"""
            INSERT INTO public."{table}" ("Symbol", "UpdatedShortTerm", "UpdatedLongTerm", enabled, "requestStateCheck")
            VALUES (:symbol, :updated_short, :updated_long, :enabled, :request_state_check)
            """
            execute_query(db, insert_query, {
                "symbol": symbol.Symbol,
                "updated_short": symbol.UpdatedShortTerm,
                "updated_long": symbol.UpdatedLongTerm,
                "enabled": symbol.enabled,
                "request_state_check": symbol.requestStateCheck
            })
        else:
            update_query = f"""
            UPDATE public."{table}"
            SET enabled = :enabled, "requestStateCheck" = :request_state_check
            WHERE "Symbol" = :symbol
            """
            execute_query(db, update_query, {
                "symbol": symbol.Symbol,
                "enabled": symbol.enabled,
                "request_state_check": symbol.requestStateCheck
            })
    all_db_symbols_query = f'SELECT "Symbol" FROM public."{table}"'
    db_symbols = [row[0] for row in execute_query(db, all_db_symbols_query, fetch="all")]
    input_symbols = {s.Symbol for s in data.symbols}
    to_disable = [s for s in db_symbols if s not in input_symbols]
    for s in to_disable:
        disable_query = f'UPDATE public."{table}" SET enabled = FALSE, "requestStateCheck" = FALSE WHERE "Symbol" = :symbol'
        execute_query(db, disable_query, {"symbol": s})
    return {"status": "success", "processed": len(data.symbols), "disabled": len(to_disable)}

# 3. Insert historical prices with delete existing
@app.post("/api/{asset_type}/prices/historical")
def insert_historical_prices(asset_type: AssetType, id_symbol: int, prices: List[Dict[str, Any]], db: Session = Depends(get_db)):
    table = get_table_name(asset_type, "prices_hist")
    delete_query = f'DELETE FROM public."{table}" WHERE "idSymbol" = :id_symbol'
    execute_query(db, delete_query, {"id_symbol": id_symbol})
    insert_data = [
        (id_symbol, p["TickerRelative"], p["open"], p["high"], p["low"], p["close"], p["volume"])
        for p in prices
    ]
    insert_query = f"""
    INSERT INTO public."{table}" ("idSymbol", "TickerRelative", "open", "high", "low", "close", "volume")
    VALUES %s
    """
    cur = db.connection().cursor()
    try:
        execute_values(cur, insert_query, insert_data)
        db.commit()
    finally:
        cur.close()
    update_relative_query = f"""
    UPDATE public."{table}"
    SET "TickerRelative" = subquery.new_relative
    FROM (
        SELECT "id", ROW_NUMBER() OVER (ORDER BY "id" ASC) * -1 + 1 AS new_relative
        FROM public."{table}"
        WHERE "idSymbol" = :id_symbol
    ) AS subquery
    WHERE public."{table}"."id" = subquery."id"
    """
    execute_query(db, update_relative_query, {"id_symbol": id_symbol})
    return {"status": "success", "inserted": len(prices)}

# 4. Insert/Update real prices
@app.post("/api/{asset_type}/prices/real")
def insert_update_real_prices(asset_type: AssetType, price: PriceRealBase, db: Session = Depends(get_db)):
    table = get_table_name(asset_type, "prices_real")
    check_query = f'SELECT COUNT(*) FROM public."{table}" WHERE "idSymbol" = :idSymbol'
    exists = execute_query(db, check_query, {"idSymbol": price.idSymbol}, fetch="one")[0] > 0
    if exists:
        update_query = """
        UPDATE public."{table}"
        SET "open" = :open, "high" = :high, "low" = :low, "close" = :close, 
            "volume" = :volume, "timestamp" = :timestamp, "updated" = :updated
        WHERE "idSymbol" = :idSymbol
        """.format(table=table)  # Dynamiczne wstawienie nazwy tabeli
    else:
        update_query = """
        INSERT INTO public."{table}" ("idSymbol", "open", "high", "low", "close", "volume", "timestamp", "updated")
        VALUES (:idSymbol, :open, :high, :low, :close, :volume, :timestamp, :updated)
        """.format(table=table)  # Dynamiczne wstawienie nazwy tabeli
    execute_query(db, update_query, price.dict())
    return {"status": "success"}

# 5. Insert indicator values
@app.post("/api/{asset_type}/indicators/{term}")
def insert_indicator_values(asset_type: AssetType, term: str, data: BatchIndicatorValues, db: Session = Depends(get_db)):
    if term not in ["long", "short"]:
        raise HTTPException(status_code=400, detail="Invalid term: must be 'long' or 'short'")
    table_key = f"indicators_{term}"
    table_name = get_table_name(asset_type, table_key)
    id_symbol = data.values[0].idSymbol if data.values else None

    if id_symbol:
        # Usuń istniejące rekordy dla danego idSymbol
        delete_query = text(f'DELETE FROM public."{table_name}" WHERE "idSymbol" = :id_symbol')
        db.execute(delete_query, {"id_symbol": id_symbol})
        db.commit()

    # Przygotuj dane do wstawienia
    insert_data = [
        {"idSymbol": v.idSymbol, "TickerRelative": v.TickerRelative, "IndicatorIndex": v.IndicatorIndex, "IndicatorValue": v.IndicatorValue}
        for v in data.values
    ]
    if insert_data:
        # Konstruuj zapytanie INSERT dla wielu rekordów
        values_clause = ", ".join(
            f"(:idSymbol{i}, :TickerRelative{i}, :IndicatorIndex{i}, :IndicatorValue{i})"
            for i in range(len(insert_data))
        )
        insert_query = text(f"""
            INSERT INTO public."{table_name}" ("idSymbol", "TickerRelative", "IndicatorIndex", "IndicatorValue")
            VALUES {values_clause}
        """)
        # Przygotuj parametry
        params = {}
        for i, row in enumerate(insert_data):
            params.update({
                f"idSymbol{i}": row["idSymbol"],
                f"TickerRelative{i}": row["TickerRelative"],
                f"IndicatorIndex{i}": row["IndicatorIndex"],
                f"IndicatorValue{i}": row["IndicatorValue"]
            })
        db.execute(insert_query, params)
        db.commit()

        # Aktualizacja UpdatedShortTerm/UpdatedLongTerm
        symbols_table = get_table_name(asset_type, "symbols")
        update_field = "UpdatedLongTerm" if term == "long" else "UpdatedShortTerm"
        update_query = text(f"""
            UPDATE public."{symbols_table}"
            SET "{update_field}" = CURRENT_DATE
        """)
        if id_symbol:
            update_query = text(f"""
                UPDATE public."{symbols_table}"
                SET "{update_field}" = CURRENT_DATE
                WHERE id = :id_symbol
            """)
            db.execute(update_query, {"id_symbol": id_symbol})
            db.commit()

            # Dodatkowa aktualizacja requestStateCheck na True, jeśli term to "short"
            if term == "short":
                request_state_update_query = text(f"""
                    UPDATE public."{symbols_table}"
                    SET "requestStateCheck" = TRUE
                    WHERE id = :id_symbol
                """)
                db.execute(request_state_update_query, {"id_symbol": id_symbol})
                db.commit()

        return {"status": "success", "inserted": len(data.values)}

# 6. Fetch indicators
@app.get("/api/{asset_type}/indicators/{term}/{symbol_id}", response_model=List[IndicatorValueBase])
def fetch_indicators(asset_type: AssetType, term: str, symbol_id: int, db: Session = Depends(get_db)):
    if term not in ["long", "short"]:
        raise HTTPException(status_code=400, detail="Invalid term: must be 'long' or 'short'")
    table_key = f"indicators_{term}"
    table_name = get_table_name(asset_type, table_key)
    query = """
    SELECT "idSymbol", "TickerRelative", "IndicatorIndex", "IndicatorValue"
    FROM public."{table_name}"
    WHERE "idSymbol" = :symbol_id
    AND "IndicatorIndex" IN (5, 7, 22, 24)
    AND "TickerRelative" > -20
    ORDER BY "TickerRelative" ASC, "IndicatorIndex" ASC
    """.format(table_name=table_name)
    params = {"symbol_id": symbol_id}
    rows = execute_query(db, query, params, fetch="all")
    if not rows:
        raise HTTPException(status_code=404, detail=f"No indicator data found for symbol_id {symbol_id}")
    return [IndicatorValueBase(idSymbol=row[0], TickerRelative=row[1], IndicatorIndex=row[2], IndicatorValue=row[3]) for row in rows]

# 7. Fetch/Update state
@app.get("/api/{asset_type}/state/{id_symbol}", response_model=StateBase)
def fetch_state(asset_type: AssetType, id_symbol: int, db: Session = Depends(get_db)):
    table = get_table_name(asset_type, "state")
    query = f"""
    SELECT "idSymbol", "status", "buy", "shouldSell", "sell", "checked", "lastAction", "invested", "shares", "maxValue", "amountBuySell"
    FROM public."{table}"
    WHERE "idSymbol" = :id_symbol
    """
    row = execute_query(db, query, {"id_symbol": id_symbol}, fetch="one")
    if not row:
        return StateBase(
            idSymbol=id_symbol,
            status="close",
            buy=False,
            shouldSell=False,
            sell=False,
            checked=datetime.now(),
            lastAction=datetime(1990, 1, 1),
            invested=0.0,
            shares=0.0,
            maxValue=0.0,
            amountBuySell=0.0
        )
    return StateBase(
        idSymbol=row[0], status=row[1], buy=bool(row[2]), shouldSell=bool(row[3]), sell=bool(row[4]),
        checked=row[5] if row[5] else datetime.now(), lastAction=row[6] if row[6] else datetime(1990, 1, 1),
        invested=float(row[7]) if row[7] is not None else 0.0,
        shares=float(row[8]) if row[8] is not None else 0.0,
        maxValue=float(row[9]) if row[9] is not None else 0.0,
        amountBuySell=float(row[10]) if row[10] is not None else 0.0
    )

@app.post("/api/{asset_type}/state/{id_symbol}")
def update_state(asset_type: AssetType, id_symbol: int, state: StateBase, db: Session = Depends(get_db)):
    table = get_table_name(asset_type, "state")
    check_query = f'SELECT COUNT(*) FROM public."{table}" WHERE "idSymbol" = :id_symbol'
    exists = execute_query(db, check_query, {"id_symbol": id_symbol}, fetch="one")[0] > 0
    if exists:
        update_query = f"""
        UPDATE public."{table}"
        SET "status" = :status, "buy" = :buy, "shouldSell" = :shouldSell, "sell" = :sell, 
            "checked" = :checked, "lastAction" = :lastAction, "invested" = :invested, 
            "shares" = :shares, "maxValue" = :maxValue, "amountBuySell" = :amountBuySell
        WHERE "idSymbol" = :id_symbol
        """
    else:
        update_query = f"""
        INSERT INTO public."{table}" ("idSymbol", "status", "buy", "shouldSell", "sell", "checked", 
            "lastAction", "invested", "shares", "maxValue", "amountBuySell")
        VALUES (:id_symbol, :status, :buy, :shouldSell, :sell, :checked, :lastAction, 
            :invested, :shares, :maxValue, :amountBuySell)
        """
    # Użyj state.dict(exclude_unset=True, exclude={"idSymbol"}) i dodaj id_symbol
    params = state.dict(exclude_unset=True, exclude={"idSymbol"})
    params["id_symbol"] = id_symbol
    execute_query(db, update_query, params)
    return {"status": "success"}

# 8. Fetch symbols with state
@app.get("/api/{asset_type}/symbols/with-state", response_model=List[SymbolResponse])
def fetch_symbols_with_state(asset_type: AssetType, db: Session = Depends(get_db)):
    symbols_table = get_table_name(asset_type, "symbols")
    state_table = get_table_name(asset_type, "state")
    query = f"""
    SELECT s.id, s."Symbol", s."UpdatedShortTerm", s.enabled, s."requestStateCheck"
    FROM public."{symbols_table}" s
    LEFT JOIN public."{state_table}" st ON s.id = st."idSymbol"
    WHERE s.enabled = TRUE OR st.status = 'open'
    """
    params = None
    rows = execute_query(db, query, params, fetch="all")
    return [SymbolResponse(id=row[0], Symbol=row[1], UpdatedShortTerm=row[2], UpdatedLongTerm=None, enabled=row[3], requestStateCheck=row[4]) for row in rows]

@app.get("/api/{asset_type}/symbols/with-short-state", response_model=List[SymbolResponse])
def fetch_symbols_with_short_state(asset_type: AssetType, db: Session = Depends(get_db)):
    symbols_table = get_table_name(asset_type, "symbols")
    state_table = get_table_name(asset_type, "state")
    current_date = datetime.now().date()  # Dzisiejsza data
    query = f"""
        SELECT s.id, s."Symbol", s."UpdatedShortTerm", s.enabled, s."requestStateCheck", st."lastAction"
        FROM public."{symbols_table}" s
        LEFT JOIN public."{state_table}" st ON s.id = st."idSymbol"
        WHERE (s.enabled = TRUE OR st.status = 'open') 
          AND s."requestStateCheck" = TRUE 
          AND s."UpdatedShortTerm" = :current_date
          AND (st."lastAction" IS NULL OR st."lastAction" < :current_date)
        ORDER BY s."UpdatedShortTerm" ASC
        """
    params = {"current_date": current_date}
    rows = execute_query(db, query, params, fetch="all")
    return [SymbolResponse(id=row[0], Symbol=row[1], UpdatedShortTerm=row[2], UpdatedLongTerm=None, enabled=row[3], requestStateCheck=row[4]) for row in rows]

# Uruchomienie aplikacji
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)