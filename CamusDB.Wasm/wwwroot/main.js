// Browser side of the playground: loads the .NET runtime, starts the engine through the exports on
// CamusDB.Wasm.PlaygroundEngine, and renders each statement's outcome.
//
// Two modes. Single node is the default and is what the page starts with. Cluster mode starts three
// CamusDB nodes in this one tab, wired over an in-memory transport, and adds the node strip: which
// node leads which partition, which nodes are stopped or cut off, and the buttons that do it.

import { dotnet } from './_framework/dotnet.js';
import { sampleScript, examples } from './sample.js';

const $ = id => document.getElementById(id);
const editor = $('sql');
const runButton = $('run');
const resetButton = $('reset');
const results = $('results');
const modeSingle = $('mode-single');
const modeCluster = $('mode-cluster');
const clusterPane = $('cluster');
const nodeSelect = $('node-select');
const restoreLinks = $('restore-links');

const numericTypes = new Set(['Integer64', 'Float64', 'Float32']);

// How many nodes the cluster button starts. Three is the smallest count that keeps a majority
// after one node stops, which is what makes a failover worth watching.
const clusterSize = 3;

let engine = null;
let busy = false;
let mode = 'single';
let status = null;
let selectedNode = 0;
let poller = null;

function setStatus(text, state) {
    $('status-text').textContent = text;
    $('status').dataset.state = state;
}

function setBusy(value) {
    busy = value;
    const idle = !value && engine;
    runButton.disabled = !idle;
    resetButton.disabled = !idle;
    modeSingle.disabled = !idle;
    modeCluster.disabled = !idle;
    for (const button of clusterPane.querySelectorAll('button, select'))
        button.disabled = !idle;
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
        if (mode === 'cluster') {
            // A cluster keeps nothing on a disk, so the whole cluster restarts.
            status = JSON.parse(await engine.StartClusterAsync(clusterSize));
            renderCluster();
            await engine.ExecuteOnNodeAsync(0, sampleScript, null);
        } else {
            await engine.ResetAsync();
            await engine.ExecuteAsync(sampleScript);
        }
        results.replaceChildren(note('The data was reset to the sample data set.'));
        setStatus('Ready', 'ready');
    } catch (error) {
        setStatus('Reset failed: ' + error.message, 'error');
    } finally {
        setBusy(false);
    }
});

modeSingle.addEventListener('click', () => switchMode('single'));
modeCluster.addEventListener('click', () => switchMode('cluster'));

restoreLinks.addEventListener('click', () => clusterAction('Restoring the links…', () => engine.RestoreLinksAsync()));

async function switchMode(next) {
    if (busy || mode === next)
        return;

    setBusy(true);
    try {
        if (next === 'cluster') {
            setStatus(`Starting ${clusterSize} nodes… this takes a few seconds`, 'loading');
            status = JSON.parse(await engine.StartClusterAsync(clusterSize));
            selectedNode = 0;
            mode = 'cluster';
            renderCluster();
            clusterPane.hidden = false;

            setStatus('Loading the sample data…', 'loading');
            await engine.ExecuteOnNodeAsync(0, sampleScript, null);

            results.replaceChildren(note(
                `${clusterSize} nodes are running. Statements go to the node you pick; stop the leader and watch another node take over.`));
            startPolling();
        } else {
            setStatus('Stopping the cluster…', 'loading');
            stopPolling();
            await engine.StopClusterAsync();
            mode = 'single';
            status = null;
            clusterPane.hidden = true;
            results.replaceChildren(note('Back to the single-node engine, with the data it already held.'));
        }

        modeSingle.classList.toggle('is-on', mode === 'single');
        modeCluster.classList.toggle('is-on', mode === 'cluster');
        setStatus('Ready', 'ready');
    } catch (error) {
        setStatus('The cluster could not start: ' + error.message, 'error');
    } finally {
        setBusy(false);
    }
}

async function run() {
    const sql = editor.value.trim();
    if (!engine || busy || !sql)
        return;

    setBusy(true);
    setStatus('Running…', 'loading');
    try {
        const json = mode === 'cluster'
            ? await engine.ExecuteOnNodeAsync(selectedNode, sql, null)
            : await engine.ExecuteAsync(sql);

        const outcomes = JSON.parse(json);
        results.replaceChildren(...(outcomes.length ? outcomes.map(render) : [note('Nothing to run.')]));
        const failed = outcomes.some(o => !o.ok);
        setStatus(failed ? 'A statement failed' : 'Ready', failed ? 'error' : 'ready');
    } catch (error) {
        results.replaceChildren(note('The engine stopped: ' + error.message, 'error'));
        setStatus('Engine error', 'error');
    } finally {
        setBusy(false);
        if (mode === 'cluster')
            refreshStatus();
    }
}

// ── Cluster controls ─────────────────────────────────────────────────────────

// Re-reads the cluster state every couple of seconds, so a failover that nothing on the page
// triggered still shows up. It skips a tick while a statement runs: the call would only queue
// behind it on the engine's gate.
function startPolling() {
    stopPolling();
    poller = setInterval(() => {
        if (mode === 'cluster' && !busy)
            refreshStatus();
    }, 2000);
}

function stopPolling() {
    if (poller !== null)
        clearInterval(poller);
    poller = null;
}

async function refreshStatus() {
    try {
        status = JSON.parse(await engine.ClusterStatusAsync());
        renderCluster();
    } catch {
        // The cluster stopped between the tick and the call. The next state change redraws.
    }
}

async function clusterAction(message, action) {
    if (busy)
        return;

    setBusy(true);
    setStatus(message, 'loading');
    try {
        status = JSON.parse(await action());
        renderCluster();
        setStatus('Ready', 'ready');
    } catch (error) {
        setStatus(error.message, 'error');
    } finally {
        setBusy(false);
    }
}

// Cuts every link of one node, in both directions. The node keeps running and keeps campaigning,
// which is what makes a network partition different from a crash.
async function isolate(index) {
    await clusterAction(`Cutting the links of node ${index + 1}…`, async () => {
        let last;
        for (const other of status.nodes) {
            if (other.index === index)
                continue;
            await engine.BlockLinkAsync(index, other.index);
            last = await engine.BlockLinkAsync(other.index, index);
        }
        return last;
    });
}

function renderCluster() {
    if (!status)
        return;

    const running = status.nodes.filter(n => n.running);
    if (!running.some(n => n.index === selectedNode))
        selectedNode = running.length ? running[0].index : 0;

    nodeSelect.replaceChildren(...status.nodes.map(node => {
        const option = document.createElement('option');
        option.value = String(node.index);
        option.textContent = `node ${node.index + 1}` + (node.running ? '' : ' (stopped)');
        option.disabled = !node.running;
        option.selected = node.index === selectedNode;
        return option;
    }));

    restoreLinks.hidden = !status.nodes.some(n => n.blockedTo.length);

    $('nodes').replaceChildren(...status.nodes.map(renderNode));
}

nodeSelect.addEventListener('change', () => { selectedNode = Number(nodeSelect.value); });

function renderNode(node) {
    const card = document.createElement('div');
    card.className = 'node'
        + (node.running ? '' : ' is-stopped')
        + (node.leads.length ? ' is-leader' : '')
        + (node.blockedTo.length ? ' is-cut' : '');

    const head = document.createElement('div');
    head.className = 'node-head';
    const dot = document.createElement('span');
    dot.className = 'node-dot';
    const name = document.createElement('span');
    name.className = 'node-name';
    name.textContent = `node ${node.index + 1}`;
    const state = document.createElement('span');
    state.className = 'badge';
    state.textContent = node.running ? 'running' : 'stopped';
    head.append(dot, name, state);

    const badges = document.createElement('div');
    badges.className = 'node-badges';
    for (const partition of node.leads) {
        const badge = document.createElement('span');
        badge.className = 'badge lead';
        badge.textContent = `leads p${partition}`;
        badges.append(badge);
    }
    if (node.blockedTo.length) {
        const badge = document.createElement('span');
        badge.className = 'badge cut';
        badge.textContent = 'links cut';
        badges.append(badge);
    }

    const actions = document.createElement('div');
    actions.className = 'node-actions';

    if (node.running) {
        const stop = document.createElement('button');
        stop.textContent = 'Stop';
        stop.title = 'Stop this node as if its host had crashed';
        stop.addEventListener('click', () =>
            clusterAction(`Stopping node ${node.index + 1}…`, () => engine.StopNodeAsync(node.index)));

        const cut = document.createElement('button');
        cut.textContent = 'Isolate';
        cut.title = 'Keep the node running but cut every link to it';
        cut.addEventListener('click', () => isolate(node.index));

        actions.append(stop, cut);
    } else {
        const start = document.createElement('button');
        start.textContent = 'Start';
        start.title = 'Start it again: it comes back empty and catches up from the others';
        start.addEventListener('click', () =>
            clusterAction(`Starting node ${node.index + 1}…`, () => engine.StartNodeAsync(node.index)));

        actions.append(start);
    }

    card.append(head, badges, actions);
    return card;
}

// ── Results ──────────────────────────────────────────────────────────────────

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
    const where = mode === 'cluster' ? `node ${selectedNode + 1} · ` : '';
    if (!outcome.ok)
        return `${where}failed · ${ms} ms`;
    if (outcome.columns)
        return `${where}${outcome.rows.length} row${outcome.rows.length === 1 ? '' : 's'} · ${ms} ms`;
    return `${where}${outcome.affected} affected · ${ms} ms`;
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
