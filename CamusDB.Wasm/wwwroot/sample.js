// The data set the playground loads on start, and the example queries it offers. The smoke test
// runs all of them, so an example that stops working fails the build instead of a visitor.

export const sampleScript = `
CREATE TABLE authors (
    id OID PRIMARY KEY NOT NULL,
    name STRING NOT NULL,
    country STRING NOT NULL,
    born INT64 NOT NULL
);

CREATE TABLE books (
    id OID PRIMARY KEY NOT NULL,
    title STRING NOT NULL,
    author STRING NOT NULL,
    year INT64 NOT NULL,
    pages INT64 NOT NULL,
    price FLOAT64 NOT NULL
);

CREATE INDEX books_year ON books (year);

INSERT INTO authors (id, name, country, born) VALUES
    (GEN_ID(), 'Ursula K. Le Guin', 'US', 1929),
    (GEN_ID(), 'Isaac Asimov', 'US', 1920),
    (GEN_ID(), 'Stanislaw Lem', 'PL', 1921),
    (GEN_ID(), 'Arthur C. Clarke', 'UK', 1917),
    (GEN_ID(), 'Octavia E. Butler', 'US', 1947),
    (GEN_ID(), 'Iain M. Banks', 'UK', 1954),
    (GEN_ID(), 'Liu Cixin', 'CN', 1963);

INSERT INTO books (id, title, author, year, pages, price) VALUES
    (GEN_ID(), 'A Wizard of Earthsea', 'Ursula K. Le Guin', 1968, 183, 9.99),
    (GEN_ID(), 'The Left Hand of Darkness', 'Ursula K. Le Guin', 1969, 304, 12.50),
    (GEN_ID(), 'The Dispossessed', 'Ursula K. Le Guin', 1974, 387, 13.25),
    (GEN_ID(), 'Foundation', 'Isaac Asimov', 1951, 255, 8.75),
    (GEN_ID(), 'I, Robot', 'Isaac Asimov', 1950, 253, 7.99),
    (GEN_ID(), 'The Gods Themselves', 'Isaac Asimov', 1972, 288, 10.40),
    (GEN_ID(), 'Solaris', 'Stanislaw Lem', 1961, 204, 11.00),
    (GEN_ID(), 'The Cyberiad', 'Stanislaw Lem', 1965, 295, 12.00),
    (GEN_ID(), 'Childhood''s End', 'Arthur C. Clarke', 1953, 214, 9.25),
    (GEN_ID(), 'Rendezvous with Rama', 'Arthur C. Clarke', 1973, 256, 10.99),
    (GEN_ID(), 'Kindred', 'Octavia E. Butler', 1979, 264, 14.00),
    (GEN_ID(), 'Parable of the Sower', 'Octavia E. Butler', 1993, 345, 15.50),
    (GEN_ID(), 'Consider Phlebas', 'Iain M. Banks', 1987, 471, 16.20),
    (GEN_ID(), 'The Player of Games', 'Iain M. Banks', 1988, 309, 13.75),
    (GEN_ID(), 'The Three-Body Problem', 'Liu Cixin', 2008, 400, 17.99);
`;

export const examples = [
    {
        label: 'Filter and sort',
        sql: 'SELECT title, author, year\nFROM books\nWHERE year < 1970\nORDER BY year;',
    },
    {
        label: 'Join and group',
        sql: 'SELECT a.country, COUNT(*) AS books, AVG(b.price) AS avg_price\nFROM books b\nJOIN authors a ON b.author = a.name\nGROUP BY a.country\nORDER BY books DESC;',
    },
    {
        label: 'Subquery',
        sql: "SELECT title, year\nFROM books\nWHERE author IN (SELECT name FROM authors WHERE country = 'UK')\nORDER BY year;",
    },
    {
        label: 'Use the index',
        sql: 'EXPLAIN SELECT title FROM books WHERE year = 1969;',
    },
    {
        label: 'Update, then check',
        sql: "UPDATE books SET price = price * 0.9 WHERE year < 1960;\n\nSELECT title, price FROM books WHERE year < 1960 ORDER BY title;",
    },
    {
        label: 'Your own table',
        sql: "CREATE TABLE notes (id OID PRIMARY KEY NOT NULL, body STRING NOT NULL, stars INT64);\n\nINSERT INTO notes (id, body, stars) VALUES (GEN_ID(), 'Loved Solaris', 5), (GEN_ID(), 'Read Kindred next', NULL);\n\nSELECT body, stars FROM notes;",
    },
    {
        label: 'Schema',
        sql: 'SHOW TABLES;\n\nSHOW COLUMNS FROM books;',
    },
    {
        label: 'Functions',
        sql: "SELECT upper(title) AS shout, length(title) AS chars, md5(title) AS digest\nFROM books\nORDER BY chars DESC\nLIMIT 3;",
    },
];
