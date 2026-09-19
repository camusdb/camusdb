// Browser side of the playground: loads the .NET runtime, starts the engine through the exports on
// CamusDB.Wasm.PlaygroundEngine, and renders each statement's outcome.

import { dotnet } from './_framework/dotnet.js';
import { sampleScript, examples } from './sample.js';

const $ = id => document.getElementById(id);
const editor = $('sql');
const runButton = $('run');
const resetButton = $('reset');
const results = $('results');

const numericTypes = new Set(['Integer64', 'Float64', 'Float32']);

let engine = null;
let busy = false;

function setStatus(text, state) {
    $('status-text').textContent = text;
    $('status').dataset.state = state;
}

function setBusy(value) {
    busy = value;
    runButton.disabled = value || !engine;
    resetButton.disabled = value || !engine;
}

if (/Mac|iPhone|iPad/.test(navigator.platform))
    $('run-key').textContent = '⌘ ↵';

for (const example of examples) {
    const chip = document.createElement('button');
    chip.className = 'chip';
    chip.textContent = example.label;
    chip.addEventListener('click', () => {
        editor.value = example.sql;
        editor.focus();
        run();
    });
    $('examples').append(chip);
}

editor.value = examples[0].sql;

editor.addEventListener('keydown', event => {
    if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
        event.preventDefault();
        run();
    }
});

runButton.addEventListener('click', run);

resetButton.addEventListener('click', async () => {
    if (busy)
        return;
    setBusy(true);
    setStatus('Resetting…', 'loading');
    try {
        await engine.ResetAsync();
        await engine.ExecuteAsync(sampleScript);
        results.replaceChildren(note('The data was reset to the sample data set.'));
        setStatus('Ready', 'ready');
    } catch (error) {
        setStatus('Reset failed: ' + error.message, 'error');
    } finally {
        setBusy(false);
    }
});

async function run() {
    const sql = editor.value.trim();
    if (!engine || busy || !sql)
        return;

    setBusy(true);
    setStatus('Running…', 'loading');
    try {
        const outcomes = JSON.parse(await engine.ExecuteAsync(sql));
        results.replaceChildren(...(outcomes.length ? outcomes.map(render) : [note('Nothing to run.')]));
        const failed = outcomes.some(o => !o.ok);
        setStatus(failed ? 'A statement failed' : 'Ready', failed ? 'error' : 'ready');
    } catch (error) {
        results.replaceChildren(note('The engine stopped: ' + error.message, 'error'));
        setStatus('Engine error', 'error');
    } finally {
        setBusy(false);
    }
}

function note(text, kind = '') {
    const p = document.createElement('p');
    p.className = 'empty ' + kind;
    p.textContent = text;
    return p;
}

function render(outcome) {
    const card = document.createElement('article');
    card.className = 'card' + (outcome.ok ? '' : ' failed');

    const head = document.createElement('header');
    const sql = document.createElement('code');
    sql.className = 'stmt';
    sql.textContent = outcome.sql;
    const meta = document.createElement('span');
    meta.className = 'meta';
    meta.textContent = describe(outcome);
    head.append(sql, meta);
    card.append(head);

    if (!outcome.ok) {
        const error = document.createElement('p');
        error.className = 'error-text';
        const code = document.createElement('strong');
        code.textContent = outcome.code;
        error.append(code, ' ', outcome.message);
        card.append(error);
    } else if (outcome.columns) {
        card.append(outcome.rows.length ? table(outcome) : note('No rows.'));
    }

    if (outcome.warning) {
        const warning = document.createElement('p');
        warning.className = 'warning-text';
        warning.textContent = outcome.warning;
        card.append(warning);
    }

    return card;
}

function describe(outcome) {
    const ms = outcome.ms < 10 ? outcome.ms.toFixed(1) : Math.round(outcome.ms);
    if (!outcome.ok)
        return `failed · ${ms} ms`;
    if (outcome.columns)
        return `${outcome.rows.length} row${outcome.rows.length === 1 ? '' : 's'} · ${ms} ms`;
    return `${outcome.affected} affected · ${ms} ms`;
}

function table(outcome) {
    const wrap = document.createElement('div');
    wrap.className = 'table-wrap';
    const t = document.createElement('table');

    const headRow = t.createTHead().insertRow();
    for (const column of outcome.columns) {
        const th = document.createElement('th');
        th.textContent = column.name;
        th.title = column.type;
        if (numericTypes.has(column.type))
            th.className = 'num';
        headRow.append(th);
    }

    const body = t.createTBody();
    for (const row of outcome.rows) {
        const tr = body.insertRow();
        for (const value of row) {
            const td = tr.insertCell();
            if (value === null) {
                td.textContent = 'NULL';
                td.className = 'null';
            } else if (typeof value === 'object') {
                td.textContent = JSON.stringify(value);
            } else {
                td.textContent = String(value);
                if (typeof value === 'number')
                    td.className = 'num';
            }
        }
    }

    wrap.append(t);
    return wrap;
}

async function start() {
    try {
        const runtime = await dotnet
            .withModuleConfig({
                onDownloadResourceProgress: (loaded, total) =>
                    setStatus(`Downloading the engine… ${loaded}/${total}`, 'loading'),
            })
            .create();

        setStatus('Starting the engine…', 'loading');
        const exports = await runtime.getAssemblyExports(runtime.getConfig().mainAssemblyName);
        const playground = exports.CamusDB.Wasm.PlaygroundEngine;

        await playground.InitAsync();
        const loaded = JSON.parse(await playground.ExecuteAsync(sampleScript));
        const failure = loaded.find(o => !o.ok);
        if (failure)
            throw new Error(`the sample data did not load (${failure.code}: ${failure.message})`);

        engine = playground;
        setStatus('Ready', 'ready');
        setBusy(false);
        editor.focus();
    } catch (error) {
        setStatus('The engine could not start: ' + error.message, 'error');
    }
}

start();
