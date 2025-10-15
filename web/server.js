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

app.listen(port, () => {
    console.log(`Serwer działa na http://localhost:${port}`);
});