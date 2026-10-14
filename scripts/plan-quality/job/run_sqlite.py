#!/usr/bin/env python3
"""SQLite oracle for the Join Order Benchmark tier of the plan-quality harness.

Runs every JOB query on the SQLite twin that gen_job.py wrote, and stores the one result row of
each query as JSON. The harness compares CamusDB's row with it.

usage: run_sqlite.py <sqlite.db> <queries-dir> <out.json>
"""
import json, os, sqlite3, sys, time

db, qdir, out = sys.argv[1:4]
con = sqlite3.connect(db)

# SQLite LIKE ignores ASCII case by default; PostgreSQL and CamusDB do not. Without this, query 17f
# gives a false mismatch ('Abravo' against 'Alpha Blue').
con.execute("PRAGMA case_sensitive_like = ON")

res = {}
for fn in sorted(os.listdir(qdir), key=lambda f: (int(''.join(c for c in f if c.isdigit()) or 0), f)):
    if not fn.endswith('.sql'):
        continue
    sql = open(os.path.join(qdir, fn)).read()
    deadline = time.time() + 120
    con.set_progress_handler(lambda: 1 if time.time() > deadline else 0, 10000)
    t = time.time()
    try:
        row = con.execute(sql).fetchone()
        res[fn[:-4]] = {"ms": round((time.time() - t) * 1000, 1), "row": list(row)}
    except Exception as e:
        res[fn[:-4]] = {"ms": round((time.time() - t) * 1000, 1), "error": str(e)}

json.dump(res, open(out, 'w'), indent=1, sort_keys=True)
ok = [k for k, v in res.items() if 'row' in v]
print(len(res), 'queries;', len(ok), 'ok; errors:', [k for k in res if k not in ok])
