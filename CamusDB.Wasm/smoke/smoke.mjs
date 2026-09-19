// End-to-end check of the browser playground host under Node, which runs the same single-threaded
// browser-wasm runtime a tab does. It fails on any statement error, a wrong result, or a hang.
//
// Usage: node CamusDB.Wasm/smoke/smoke.mjs <published wwwroot directory>
//   dotnet publish CamusDB.Wasm/CamusDB.Wasm.csproj -c Release -o artifacts/wasm
//   node CamusDB.Wasm/smoke/smoke.mjs artifacts/wasm/wwwroot

import path from 'node:path';
import { pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';

const root = process.argv[2];
if (!root) {
    console.error('usage: node smoke.mjs <published wwwroot directory>');
    process.exit(2);
}

// A deadlock on the single-threaded runtime leaves the event loop idle and the process would just
// exit with pending promises, or spin forever; either way this turns it into a failure.
const deadline = setTimeout(() => {
    console.error('FAIL: timed out after 120 s');
    process.exit(1);
}, 120_000);

const { dotnet } = await import(pathToFileURL(path.resolve(root, '_framework/dotnet.js')).href);
const runtime = await dotnet.create();
const exports = await runtime.getAssemblyExports(runtime.getConfig().mainAssemblyName);
const engine = exports.CamusDB.Wasm.PlaygroundEngine;

const started = performance.now();
await engine.InitAsync();
console.log(`engine up in ${Math.round(performance.now() - started)} ms`);

async function run(script) {
    const results = JSON.parse(await engine.ExecuteAsync(script));
    for (const r of results) {
        if (!r.ok)
            throw new Error(`statement failed: ${r.sql}\n  ${r.code}: ${r.message}`);
    }
    return results;
}

const inserts = Array.from({ length: 50 }, (_, i) => `(GEN_ID(), 'bot${i}', ${1950 + i})`).join(', ');

await run(`
    CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL);
    CREATE INDEX robots_year ON robots (year);
    CREATE TABLE makers (id OID PRIMARY KEY NOT NULL, robot STRING NOT NULL, maker STRING NOT NULL);
    INSERT INTO robots (id, name, year) VALUES ${inserts};
    INSERT INTO makers (id, robot, maker) VALUES (GEN_ID(), 'bot1', 'Acme'), (GEN_ID(), 'bot2', 'Initech');
`);

const [ordered] = await run('SELECT name, year FROM robots WHERE year >= 1997 ORDER BY year');
assert.deepEqual(ordered.columns.map(c => c.name), ['name', 'year']);
assert.deepEqual(ordered.rows, [['bot47', 1997], ['bot48', 1998], ['bot49', 1999]]);

const [joined] = await run('SELECT r.name, m.maker FROM robots r JOIN makers m ON r.name = m.robot ORDER BY r.name');
assert.deepEqual(joined.rows, [['bot1', 'Acme'], ['bot2', 'Initech']]);

const [grouped] = await run('SELECT COUNT(*) AS n, MIN(year) AS lo, MAX(year) AS hi FROM robots');
assert.deepEqual(grouped.rows, [[50, 1950, 1999]]);

const [plan] = await run('EXPLAIN SELECT name FROM robots WHERE year = 1977');
assert.ok(JSON.stringify(plan.rows).includes('robots_year'), 'the plan should use the year index');

const [updated, deleted] = await run("UPDATE robots SET year = 2000 WHERE name = 'bot0'; DELETE FROM robots WHERE year < 1960");
assert.equal(updated.affected, 1);
assert.equal(deleted.affected, 9);

const [hashed] = await run("SELECT md5('abc') AS h");
assert.deepEqual(hashed.rows, [['900150983cd24fb0d6963f7d28e17f72']]);

// A failing statement stops the script and reports the engine's error code.
const failed = JSON.parse(await engine.ExecuteAsync('SELECT 1; SELECT * FROM missing; SELECT 2'));
assert.equal(failed.length, 2);
assert.equal(failed[1].ok, false);
assert.match(failed[1].code, /^CA/);

// The page's sample data set and every example it offers must run cleanly on a fresh engine.
await engine.ResetAsync();
const { sampleScript, examples } = await import(pathToFileURL(path.resolve(root, 'sample.js')).href);
await run(sampleScript);
for (const example of examples)
    await run(example.sql);

await engine.ResetAsync();
const [tables] = await run('SHOW TABLES');
assert.deepEqual(tables.rows, []);

clearTimeout(deadline);
console.log(`PASS in ${Math.round(performance.now() - started)} ms`);
process.exit(0);
