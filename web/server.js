const express = require('express');
const path = require('path');
const { spawn } = require('child_process');
const { Pool } = require('pg');
const app = express();
const port = 3000;

let pythonProcess = null;
let logs = [];

// Konfiguracja połączenia z PostgreSQL
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'TradingView',
    password: 'postgres',
    port: 5432,
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/scripts', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'scripts.html'));
});

app.get('/stock-1d', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'stock-1d.html'));
});

app.get('/stock-detail/:symbol', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'stock-detail.html'));
});

app.get('/open-blocks', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'open-blocks.html'));
});

app.get('/run-stock-1d-scrap', (req, res) => {
    if (pythonProcess) {
        return res.status(400).send('Skrypt już działa.');
    }
    const scriptPath = path.join(__dirname, '..', 'stock', 'stock_scrap_by_symbol_list_short.py');
    pythonProcess = spawn('python3', [scriptPath], { stdio: ['pipe', 'pipe', 'pipe'] });

    logs = [];
    pythonProcess.stdout.on('data', (data) => {
        const logMessage = data.toString();
        logs.push(logMessage);
        console.log('Log dodany:', logMessage);
        if (logs.length > 100) logs.shift();
    });

    pythonProcess.stderr.on('data', (data) => {
        const errorMessage = data.toString();
        logs.push(`Błąd: ${errorMessage}`);
        console.log('Błąd logu:', errorMessage);
        if (logs.length > 100) logs.shift();
    });

    pythonProcess.on('close', (code) => {
        logs.push(`Skrypt zakończony z kodem ${code}`);
        console.log(`Skrypt zakończony z kodem ${code}`);
        pythonProcess = null;
    });

    res.send('Skrypt dla Stock 1D uruchomiony.');
});

app.get('/stop-stock-1d-scrap', (req, res) => {
    if (!pythonProcess) {
        return res.status(400).send('Żaden skrypt nie działa.');
    }
    pythonProcess.kill('SIGINT');
    pythonProcess = null;
    res.send('Skrypt zatrzymany.');
});

app.get('/get-logs', (req, res) => {
    res.json(logs);
});

app.get('/get-stock-data', (req, res) => {
    pool.query(
        'SELECT s."Symbol", t."buy", t."shouldSell", t."sell", t."invested", t."shares", t."maxValue" ' +
        'FROM public."tStockState" t ' +
        'JOIN public."tStockSymbols" s ON t."idSymbol" = s.id',
        (err, result) => {
            if (err) {
                console.error('Błąd zapytania do bazy danych:', err);
                return res.status(500).json({ error: 'Błąd pobierania danych' });
            }
            console.log('Pobrane dane:', result.rows);
            if (result.rows.length === 0) {
                console.log('Brak danych w tabeli.');
                return res.json([]);
            }
            res.json(result.rows);
        }
    );
});

app.get('/get-open-blocks', (req, res) => {
    pool.query(
        'SELECT s."Symbol", s.id AS symbol_id, t."invested", t."shares", t."lastAction", t."buy", t."sell", t."shouldSell" ' +
        'FROM public."tStockState" t ' +
        'JOIN public."tStockSymbols" s ON t."idSymbol" = s.id ' +
        'WHERE t."status" = $1',
        ['open'],
        (err, result) => {
            if (err) {
                console.error('Błąd pobierania otwartych bloków:', err);
                return res.status(500).json({ error: 'Błąd pobierania danych' });
            }
            console.log('Pobrane otwarte bloki:', result.rows);

            const processBlocks = result.rows.map(async (row) => {
                // Pobierz aktualną cenę z tStock_PricesReal
                const priceResult = await pool.query(
                    'SELECT "close" ' +
                    'FROM public."tStock_PricesReal" ' +
                    'WHERE "idSymbol" = (SELECT "id" FROM public."tStockSymbols" WHERE "Symbol" = $1) ' +
                    'ORDER BY "updated" DESC ' +
                    'LIMIT 1',
                    [row.Symbol]
                );

                let currentValue = 0;
                let profitLoss = 'N/A';
                if (priceResult.rows.length > 0 && row.invested !== undefined && row.shares !== undefined) {
                    const currentPrice = parseFloat(priceResult.rows[0].close) || 0;
                    currentValue = row.shares * currentPrice;
                    profitLoss = ((currentValue - row.invested) / row.invested * 100).toFixed(2) || 'N/A';
                }

                // Oblicz czas od otwarcia na podstawie lastAction
                let timeOpened = 'N/A';
                if (row.lastAction) {
                    const openDate = new Date(row.lastAction);
                    const now = new Date();
                    if (!isNaN(openDate.getTime())) {
                        const diffTime = Math.abs(now - openDate);
                        const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
                        timeOpened = diffDays === 0 ? 'today' : diffDays;
                    }
                }

                return {
                    Symbol: row.Symbol || 'N/A',
                    Invested: row.invested !== undefined ? row.invested.toFixed(2) : 'N/A',
                    Shares: row.shares !== undefined ? row.shares : 'N/A',
                    'Profit/Loss (%)': profitLoss,
                    'Time Opened (days)': timeOpened,
                    symbol_id: row.symbol_id,
                    buy: row.buy || false,
                    sell: row.sell || false,
                    shouldSell: row.shouldSell || false
                };
            });

            Promise.all(processBlocks)
                .then(processedBlocks => {
                    console.log('Przetworzone bloki:', processedBlocks);
                    res.json(processedBlocks);
                })
                .catch(error => {
                    console.error('Błąd przetwarzania bloków:', error);
                    res.status(500).json({ error: 'Błąd przetwarzania danych' });
                });
        }
    );
});

app.get('/get-symbol-data/:symbol', (req, res) => {
    const symbol = req.params.symbol;
    pool.query(
        'SELECT "id", "idSymbol", "type", "amount", "price", "shares", "timestamp" ' +
        'FROM public."tStockPositions" ' +
        'WHERE "idSymbol" = (SELECT "id" FROM public."tStockSymbols" WHERE "Symbol" = $1) ' +
        'ORDER BY "timestamp" ASC',
        [symbol],
        (err, result) => {
            if (err) {
                console.error('Błąd zapytania do bazy danych dla symbolu:', err);
                return res.status(500).json({ error: 'Błąd pobierania danych symbolu' });
            }
            console.log(`Dane dla symbolu ${symbol}:`, result.rows);
            res.json(result.rows);
        }
    );
});

app.get('/get-price-data/:symbol', (req, res) => {
    const symbol = req.params.symbol;
    pool.query(
        'SELECT "close", "updated" ' +
        'FROM public."tStock_PricesReal" ' +
        'WHERE "idSymbol" = (SELECT "id" FROM public."tStockSymbols" WHERE "Symbol" = $1) ' +
        'ORDER BY "updated" DESC ' +
        'LIMIT 1',
        [symbol],
        (err, result) => {
            if (err) {
                console.error('Błąd zapytania do bazy danych dla ceny symbolu:', err);
                return res.status(500).json({ error: 'Błąd pobierania ceny' });
            }
            console.log(`Cena dla symbolu ${symbol}:`, result.rows);
            res.json(result.rows);
        }
    );
});

app.post('/add-transaction', (req, res) => {
    const { idSymbol, type, amount, price, shares, timestamp } = req.body;
    pool.query(
        'INSERT INTO public."tStockPositions" ("idSymbol", "type", "amount", "price", "shares", "timestamp") VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
        [idSymbol, type, amount, price, shares, timestamp],
        (err, result) => {
            if (err) {
                console.error('Błąd dodawania transakcji:', err);
                return res.status(500).json({ success: false, error: err.message });
            }
            console.log('Transakcja dodana:', result.rows[0]);
            res.json({ success: true, transaction: result.rows[0] });
        }
    );
});

app.post('/update-stock-state', (req, res) => {
    const { idSymbol, status, buy, shouldSell, sell, lastAction, invested, shares, maxValue, amountBuySell } = req.body;

    pool.query(
        'SELECT * FROM public."tStockState" WHERE "idSymbol" = $1',
        [idSymbol],
        (err, result) => {
            if (err) {
                console.error('Błąd pobierania rekordu tStockState:', err);
                return res.status(500).json({ success: false, error: err.message });
            }

            const existing = result.rows[0];
            if (!existing) {
                console.error('Brak rekordu dla idSymbol:', idSymbol);
                return res.status(404).json({ success: false, error: 'Rekord nie istnieje' });
            }

            let newInvested = invested !== undefined ? invested : existing.invested;
            let newShares = shares !== undefined ? shares : existing.shares;
            let newMaxValue = maxValue !== undefined ? maxValue : existing.maxValue;

            // Dla akcji "buy" dodaj do istniejących wartości
            if (status === 'open') {
                newInvested = (existing.invested || 0) + (amountBuySell || 10); // Użyj amountBuySell jeśli podane
                newShares = (existing.shares || 0) + (shares || 0); // Dodaj zakupione akcje
            }

            // Dla akcji "sell" zeruj odpowiednie pola
            if (status === 'close') {
                newInvested = 0;
                newShares = 0;
                newMaxValue = 0;
            }

            // Aktualizacja istniejącego rekordu
            const query = `
                UPDATE public."tStockState"
                SET "status" = $2,
                    "buy" = $3,
                    "shouldSell" = $4,
                    "sell" = $5,
                    "lastAction" = $6,
                    "invested" = $7,
                    "shares" = $8,
                    "maxValue" = $9,
                    "amountBuySell" = $10
                WHERE "idSymbol" = $1
            `;
            pool.query(
                query,
                [idSymbol, status, buy, shouldSell, sell, lastAction, newInvested, newShares, newMaxValue, amountBuySell],
                (err) => {
                    if (err) {
                        console.error('Błąd aktualizacji tStockState:', err);
                        return res.status(500).json({ success: false, error: err.message });
                    }
                    res.json({ success: true });
                }
            );
        }
    );
});

app.listen(port, () => {
    console.log(`Serwer działa na http://localhost:${port}`);
});