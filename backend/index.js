const express = require('express');
const { Pool } = require('pg');

const app = express();
const PORT = 3000;

app.use(express.json());

// Dane polaczenia z PostgreSQL
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'booking_tool',
  password: 'Mariposa1',
  port: 5432,
});

app.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW()');
    res.send(`Polaczenie dziala! Serwer: ${result.rows[0].now}`);
  } catch (err) {
    console.error(err);
    res.status(500).send('Blad polaczenia z baza');
  }
});

app.get('/reservations', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM reservations ORDER BY id');
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json('Blad pobierania rezerwacji');
  }
});

// POST: dodaj nowa rezerwacje
app.post('/reservations', async (req, res) => {
  const { name, email, date } = req.body;

  // WALIDACJA
  if (!name || !email || !date) {
    return res.status(400).json({ error: 'Pola name, email i date są wymagane.' });
  }

  try {
    const result = await pool.query(
      'INSERT INTO reservations (name, email, date) VALUES ($1, $2, $3) RETURNING *',
      [name, email, date]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).send('Błąd dodawania rezerwacji');
  }
});

// DELETE: usun rezerwacje po ID
app.delete('/reservations/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(
      'DELETE FROM reservations WHERE id = $1 RETURNING *',
      [id]
    );

    if (result.rowCount === 0) {
      return res.status(404).send('Nie znaleziono rezerwacji o podanym ID.');
    }

    res.send(`Rezerwacja ID ${id} usunieta.`);
  } catch (err) {
    console.error(err);
    res.status(500).send('Blad usuwania rezerwacji.');
  }
});

// PUT: aktualizuj rezerwacje po ID
app.put('/reservations/:id', async (req, res) => {
  const { id } = req.params;
  const { name, email, date } = req.body;

  try {
    const result = await pool.query(
      'UPDATE reservations SET name = $1, email = $2, date = $3 WHERE id = $4 RETURNING *',
      [name, email, date, id]
    );

    if (result.rowCount === 0) {
      return res.status(404).send('Nie znaleziono rezerwacji o podanym ID.');
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).send('Blad aktualizacji rezerwacji.');
  }
});

app.listen(PORT, () => {
  console.log(`Serwer działa na http://localhost:${PORT}`);
});

