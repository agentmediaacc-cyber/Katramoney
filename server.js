const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const app = express();

app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));
app.set('view engine', 'ejs');

// Database Setup
const db = new sqlite3.Database('./data/katra.db');
db.run("CREATE TABLE IF NOT EXISTS loans (id INTEGER PRIMARY KEY, name TEXT, amount REAL, months INTEGER, status TEXT DEFAULT 'Pending')");

// Routes
app.get('/', (req, res) => res.render('index'));

app.post('/apply', (req, res) => {
    const { name, amount, months } = req.body;
    db.run("INSERT INTO loans (name, amount, months) VALUES (?, ?, ?)", [name, amount, months], () => {
        res.send("<h1>Application Submitted! Katramoney will review soon.</h1><a href='/'>Back</a>");
    });
});

app.get('/admin', (req, res) => {
    db.all("SELECT * FROM loans", [], (err, rows) => {
        res.render('admin', { loans: rows });
    });
});

app.listen(3000, () => console.log('Katramoney running at http://localhost:3000'));
