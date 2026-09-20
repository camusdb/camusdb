// End-to-end check of the playground's cluster mode under Node, which runs the same
// single-threaded browser-wasm runtime a tab does. Three CamusDB nodes share one event loop and
// one pair of in-memory transports, so this exercises DDL replication, a read on a follower, a
// failover and a rejoin.
//
// Usage: node CamusDB.Wasm/smoke/smoke-cluster.mjs <published wwwroot directory>
//   dotnet publish CamusDB.Wasm/CamusDB.Wasm.csproj -c Release -o artifacts/wasm
//   node CamusDB.Wasm/smoke/smoke-cluster.mjs artifacts/wasm/wwwroot

import path from 'node:path';
import { pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';

const root = process.argv[2];
if (!root) {
    console.error('usage: node smoke-cluster.mjs <published wwwroot directory>');
    process.exit(2);
}

// A deadlock on the single-threaded runtime leaves the event loop idle and the process would just
// exit with pending promises, or spin forever; either way this turns it into a failure.
const deadline = setTimeout(() => {
    console.error('FAIL: timed out after 300 s');
    process.exit(1);
}, 300_000);

const { dotnet } = await import(pathToFileURL(path.resolve(root, '_framework/dotnet.js')).href);
const runtime = await dotnet.create();
const exports = await runtime.getAssemblyExports(runtime.getConfig().mainAssemblyName);
const engine = exports.CamusDB.Wasm.PlaygroundEngine;

const started = performance.now();

function leadersOf(status) {
    // partition id -> node index, for every partition some running node claims.
    const leaders = {};
    for (const node of status.nodes) {
        for (const partition of node.leads)
            leaders[partition] = node.index;
    }
    return leaders;
}

async function runOn(nodeIndex, script) {
    const results = JSON.parse(await engine.ExecuteOnNodeAsync(nodeIndex, script, null));
    for (const r of results) {
        if (!r.ok)
            throw new Error(`statement failed on node ${nodeIndex + 1}: ${r.sql}\n  ${r.code}: ${r.message}`);
    }
    return results;
}

// A follower applies a committed schema change a moment after the leader does, and a node that
// just rejoined catches up over a few heartbeats, so a read right after a write can legitimately
// be early. Retry the assertion instead of sleeping for a fixed time.
async function eventually(what, attempt, timeoutMs = 60_000) {
    const until = performance.now() + timeoutMs;
    let last;
    for (;;) {
        try {
            return await attempt();
        } catch (error) {
            last = error;
            if (performance.now() > until)
                throw new Error(`${what} did not hold within ${timeoutMs} ms: ${last.message}`);
            await new Promise(resolve => setTimeout(resolve, 200));
        }
    }
}

// 1. Three nodes in one tab.
const startedCluster = performance.now();
let status = JSON.parse(await engine.StartClusterAsync(3));
console.log(`cluster up in ${Math.round(performance.now() - startedCluster)} ms`);

assert.equal(status.nodes.length, 3);
assert.ok(status.nodes.every(n => n.running), 'every node should run');
assert.ok(Object.keys(leadersOf(status)).length > 0, 'some partition should have a leader');

// 2. DDL on node 1 reaches node 2 and node 3. Node 1 need not be the schema leader: a follower
//    forwards the ticket to whoever leads, and every node applies the committed change.
await runOn(0, `
    CREATE TABLE robots (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, year INT64 NOT NULL);
`);

for (const node of [1, 2]) {
    await eventually(`node ${node + 1} sees the table`, async () => {
        const [shown] = await runOn(node, 'SHOW TABLES');
        assert.ok(shown.rows.flat().includes('robots'), `node ${node + 1} does not have the table yet`);
    });
}

// 3. Rows written on one node are read back on the others.
await runOn(0, "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'atlas', 1997), (GEN_ID(), 'nomad', 2001)");

for (const node of [1, 2]) {
    await eventually(`node ${node + 1} reads both rows`, async () => {
        const [rows] = await runOn(node, 'SELECT name FROM robots ORDER BY name');
        assert.deepEqual(rows.rows, [['atlas'], ['nomad']]);
    });
}

// 4. Stop the leader of partition 0. The others elect a new one and keep accepting writes.
const stopped = await engine.LeaderOfPartitionAsync(0);
console.log(`stopping node ${stopped + 1}, the leader of partition 0`);

status = JSON.parse(await engine.StopNodeAsync(stopped));
assert.equal(status.nodes[stopped].running, false);

const newLeader = await engine.LeaderOfPartitionAsync(0);
assert.notEqual(newLeader, stopped, 'a different node should lead partition 0 now');
console.log(`node ${newLeader + 1} leads partition 0 now`);

const survivor = [0, 1, 2].find(i => i !== stopped);
await eventually('the surviving nodes accept a write', () =>
    runOn(survivor, "INSERT INTO robots (id, name, year) VALUES (GEN_ID(), 'lodestar', 2013)"));

// 5. The stopped node comes back empty and catches up from the others.
status = JSON.parse(await engine.StartNodeAsync(stopped));
assert.equal(status.nodes[stopped].running, true);

await eventually(`node ${stopped + 1} caught up`, async () => {
    const [rows] = await runOn(stopped, 'SELECT name FROM robots ORDER BY name');
    assert.deepEqual(rows.rows, [['atlas'], ['lodestar'], ['nomad']]);
});

// 6. No leader moves while nothing fails. Kommander's IRaft does not expose the Raft term, so the
//    observable stand-in is the leader of each partition: a needless election shows up as a
//    partition whose leader changed with no fault to explain it.
const before = leadersOf(JSON.parse(await engine.ClusterStatusAsync()));
console.log('watching for needless elections for 10 s:', JSON.stringify(before));

for (let i = 0; i < 10; i++) {
    await new Promise(resolve => setTimeout(resolve, 1_000));

    const now = leadersOf(JSON.parse(await engine.ClusterStatusAsync()));
    for (const [partition, leader] of Object.entries(before)) {
        assert.equal(now[partition], leader,
            `partition ${partition} changed leader with no fault: node ${leader + 1} to node ${now[partition] + 1}`);
    }
}

await engine.StopClusterAsync();

clearTimeout(deadline);
console.log(`PASS in ${Math.round(performance.now() - started)} ms`);
process.exit(0);
