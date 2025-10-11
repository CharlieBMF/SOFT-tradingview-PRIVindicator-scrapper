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
    const scriptPath = path.join(__dirname, '..', 'stock', 'stock_scrap_by_symbollist_short.py');
    pythonProcess = spawn('python3', [scriptPath], { stdio: ['pipe', 'pipe', 'pipe'] });

    logs = []; // Wyczyść logi przed nowym uruchomieniem
    pythonProcess.stdout.on('data', (data) => {
        const logMessage = data.toString();
        logs.push(logMessage);
        console.log('Log dodany:', logMessage); // Debug
        if (logs.length > 100) logs.shift(); // Ogranicz długość logów
    });

    pythonProcess.stderr.on('data', (data) => {
        const errorMessage = data.toString();
        logs.push(`Błąd: ${errorMessage}`);
        console.log('Błąd logu:', errorMessage); // Debug
        if (logs.length > 100) logs.shift();
    });

    pythonProcess.on('close', (code) => {
        logs.push(`Skrypt zakończony z kodem ${code}`);
        console.log(`Skrypt zakończony z kodem ${code}`); // Debug
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

// Endpoint do pobierania danych z tStockState z joinem do tStockSymbols
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
            console.log('Struktura tabeli tStockState:', result.fields.map(f => f.name)); // Debug: pokaż nazwy kolumn
            console.log('Pobrane dane:', result.rows); // Debug: wyświetl dane
            if (result.rows.length === 0) {
                console.log('Brak danych w tabeli.');
                return res.json([]);
            }
            res.json(result.rows);
        }
    );
});

app.listen(port, () => {
    console.log(`Serwer działa na http://localhost:${port}`);
});