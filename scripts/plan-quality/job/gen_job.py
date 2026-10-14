#!/usr/bin/env python3
"""Synthetic IMDB data for the Join Order Benchmark.

Generates a scaled-down data set with the JOB schema. String columns that the
queries filter on are seeded with values built from the query literals, so the
filters select rows. Writes one TSV per table (\\N = NULL), a CamusDB schema,
and a SQLite database with the same rows plus the FK indexes.

usage: gen_job.py <job-bench-dir> <out-dir> <scale>
"""
import os, re, random, sqlite3, sys

job_dir, out_dir, scale = sys.argv[1], sys.argv[2], float(sys.argv[3])
rnd = random.Random(20261012)

REAL = dict(aka_name=901343, aka_title=361472, cast_info=36244344, char_name=3140339,
            comp_cast_type=4, company_name=234997, company_type=4, complete_cast=135086,
            info_type=113, keyword=134170, kind_type=7, link_type=18, movie_companies=2609129,
            movie_info=14835720, movie_info_idx=1380035, movie_keyword=4523930, movie_link=29997,
            name=4167491, person_info=2963664, role_type=12, title=2528312)

# ---------- schema ----------
schema_sql = open(os.path.join(job_dir, "data", "schema.sql")).read()
tables = {}  # name -> [(col, type, notnull)]
for m in re.finditer(r"CREATE TABLE (\w+) \((.*?)\);", schema_sql, re.S):
    cols = []
    for line in m.group(2).strip().split(",\n"):
        line = line.strip()
        name, rest = line.split(None, 1)
        typ = "int" if rest.startswith("integer") else "str"
        cols.append((name, typ, "NOT NULL" in rest))
    tables[m.group(1)] = cols

# ---------- seeds from query literals ----------
qdir = os.path.join(job_dir, "queries")
seeds = {}  # (table, col) -> set of literal strings / LIKE patterns


def add_seed(table, col, value):
    seeds.setdefault((table, col), set()).add(value)


for fn in sorted(os.listdir(qdir)):
    if not fn.endswith(".sql"):
        continue
    sql = open(os.path.join(qdir, fn)).read()
    aliases = dict((a, t) for t, a in re.findall(r"(\w+) AS (\w+)", sql))
    for a, c, lit in re.findall(r"(\w+)\.(\w+)\s*=\s*'([^']*)'", sql):
        if a in aliases: add_seed(aliases[a], c, lit)
    for a, c, neg, pat in re.findall(r"(\w+)\.(\w+)\s+(NOT\s+)?LIKE\s+'([^']*)'", sql):
        if a in aliases and not neg: add_seed(aliases[a], c, "LIKE:" + pat)
    for a, c, neg, body in re.findall(r"(\w+)\.(\w+)\s+(NOT\s+)?IN\s*\(([^)]*)\)", sql, re.S):
        if a in aliases and not neg:
            for lit in re.findall(r"'([^']*)'", body):
                add_seed(aliases[a], c, lit)

WORDS = ("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike "
         "november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu "
         "stone river night city blue red green dark light love war story man woman").split()


def filler(n=None):
    n = n if n is not None else rnd.randint(0, 2)
    return " ".join(rnd.choice(WORDS) for _ in range(n))


def expand(seed):
    if not seed.startswith("LIKE:"):
        return seed
    out = []
    for ch in seed[5:]:
        if ch == "%":
            out.append(filler(rnd.randint(0, 1)))
        elif ch == "_":
            out.append("x")
        else:
            out.append(ch)
    return "".join(out)


def seeded_str(table, col, p_seed, maxlen=None):
    s = seeds.get((table, col))
    if s and rnd.random() < p_seed:
        v = expand(rnd.choice(sorted(s)))
    else:
        v = filler(rnd.randint(1, 3)).title()
    if maxlen:
        v = v[:maxlen]
    return v


def n_rows(t, minimum=200):
    return max(minimum, int(REAL[t] * scale))


# ---------- dimension tables (real values) ----------
rows = {}
rows["kind_type"] = [(i + 1, k) for i, k in enumerate(
    ["movie", "tv series", "tv movie", "video movie", "tv mini series", "video game", "episode"])]
rows["company_type"] = [(i + 1, k) for i, k in enumerate(
    ["distributors", "production companies", "special effects companies", "miscellaneous companies"])]
rows["comp_cast_type"] = [(i + 1, k) for i, k in enumerate(["cast", "crew", "complete", "complete+verified"])]
rows["role_type"] = [(i + 1, k) for i, k in enumerate(
    ["actor", "actress", "producer", "writer", "cinematographer", "composer", "costume designer",
     "director", "editor", "miscellaneous crew", "production designer", "guest"])]
rows["link_type"] = [(i + 1, k) for i, k in enumerate(
    ["follows", "followed by", "remake of", "remade as", "references", "referenced in", "spoofs",
     "spoofed in", "features", "featured in", "spin off from", "spin off", "version of", "similar to",
     "edited into", "edited from", "alternate language version of", "unknown link"])]
info_vals = sorted(seeds[("info_type", "info")])
info_vals += ["info %d" % i for i in range(113 - len(info_vals))]
rows["info_type"] = [(i + 1, v) for i, v in enumerate(info_vals)]
seeded_info_ids = list(range(1, len(seeds[("info_type", "info")]) + 1))

kw_seeds = sorted(seeds[("keyword", "keyword")])
nk = n_rows("keyword")
rows["keyword"] = [(i + 1, kw_seeds[i] if i < len(kw_seeds) else "kw-" + filler(2).replace(" ", "-") + str(i), None)
                   for i in range(nk)]
seeded_kw_ids = list(range(1, len(kw_seeds) + 1))

# ---------- entity tables ----------
NT = n_rows("title")
title_seed_ids = set()
rows["title"] = []
for i in range(1, NT + 1):
    kind = rnd.choice([1, 1, 1, 2, 3, 4, 7, 7])
    year = None if rnd.random() < 0.05 else rnd.randint(1900, 2015) if rnd.random() < 0.4 else rnd.randint(1990, 2015)
    ep = rnd.randint(1, 200) if kind == 7 and rnd.random() < 0.7 else None
    rows["title"].append((i, seeded_str("title", "title", 0.15), None, kind, year, None, None, None,
                          rnd.randint(1, 10) if ep else None, ep, None, None))

NN = n_rows("name")
rows["name"] = [(i, seeded_str("name", "name", 0.3), None, None,
                 None if rnd.random() < 0.3 else rnd.choice("mf"),
                 rnd.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + str(rnd.randint(100, 999)),
                 None, None, None) for i in range(1, NN + 1)]
NC = n_rows("char_name")
rows["char_name"] = [(i, seeded_str("char_name", "name", 0.3), None, None, None, None, None) for i in range(1, NC + 1)]
NCN = n_rows("company_name")
cc_seeds = sorted(seeds[("company_name", "country_code")])
rows["company_name"] = [(i, seeded_str("company_name", "name", 0.2),
                         None if rnd.random() < 0.1 else (rnd.choice(cc_seeds) if rnd.random() < 0.7 else "[%s]" % rnd.choice(["fr", "it", "es", "jp", "ca"])),
                         None, None, None, None) for i in range(1, NCN + 1)]


def movie():  # skewed toward low title ids, like popular movies
    return int(NT * rnd.random() ** 2) + 1


def person():
    return int(NN * rnd.random() ** 1.5) + 1


def gen(table, maker):
    rows[table] = [maker(i) for i in range(1, n_rows(table) + 1)]


gen("aka_name", lambda i: (i, person(), seeded_str("aka_name", "name", 0.3), None, None, None, None, None))
gen("aka_title", lambda i: (i, movie(), seeded_str("aka_title", "title", 0.2), None, rnd.randint(1, 7),
                            rnd.randint(1950, 2015), None, None, None, None, None, None))
gen("cast_info", lambda i: (i, person(), movie(), (int(NC * rnd.random()) + 1) if rnd.random() < 0.5 else None,
                            None if rnd.random() < 0.5 else seeded_str("cast_info", "note", 0.6),
                            rnd.randint(1, 30) if rnd.random() < 0.6 else None,
                            rnd.choice([1, 1, 2, 2, 3, 4, 8] + list(range(1, 13)))))
gen("complete_cast", lambda i: (i, movie(), rnd.randint(1, 2), rnd.randint(3, 4)))
gen("movie_companies", lambda i: (i, movie(), int(NCN * rnd.random() ** 2) + 1, rnd.randint(1, 2) if rnd.random() < 0.9 else rnd.randint(3, 4),
                                  None if rnd.random() < 0.4 else seeded_str("movie_companies", "note", 0.6)))


def info_type_id():
    return rnd.choice(seeded_info_ids) if rnd.random() < 0.8 else rnd.randint(1, 113)


gen("movie_info", lambda i: (i, movie(), info_type_id(), seeded_str("movie_info", "info", 0.6),
                             None if rnd.random() < 0.6 else seeded_str("movie_info", "note", 0.5)))


def rating():
    r = rnd.random()
    if r < 0.4: return "%.1f" % rnd.uniform(1.0, 9.9)
    if r < 0.6: return str(rnd.randint(5, 50000))
    return seeded_str("movie_info_idx", "info", 0.5)


gen("movie_info_idx", lambda i: (i, movie(), info_type_id(), rating(), None))
gen("movie_keyword", lambda i: (i, movie(), rnd.choice(seeded_kw_ids) if rnd.random() < 0.5 else rnd.randint(1, nk)))
gen("movie_link", lambda i: (i, movie(), movie(), rnd.randint(1, 18)))
gen("person_info", lambda i: (i, person(), info_type_id(), seeded_str("person_info", "info", 0.4),
                              None if rnd.random() < 0.5 else seeded_str("person_info", "note", 0.5)))

for t, cols in tables.items():
    assert all(len(r) == len(cols) for r in rows[t]), t

# ---------- write TSV + schema ----------
os.makedirs(os.path.join(out_dir, "data"), exist_ok=True)
for t, cols in tables.items():
    with open(os.path.join(out_dir, "data", t + ".tsv"), "w") as f:
        f.write("\t".join(c for c, _, _ in cols) + "\n")
        for r in rows[t]:
            f.write("\t".join("\\N" if v is None else str(v) for v in r) + "\n")

with open(os.path.join(out_dir, "types.tsv"), "w") as f:
    for t, cols in tables.items():
        for c, typ, _ in cols:
            f.write("%s\t%s\t%s\n" % (t, c, typ))

# ---------- SQLite twin ----------
dbp = os.path.join(out_dir, "sqlite.db")
if os.path.exists(dbp):
    os.remove(dbp)
con = sqlite3.connect(dbp)
con.executescript(schema_sql)
con.executescript(open(os.path.join(job_dir, "data", "fkindexes.sql")).read())
for t, cols in tables.items():
    con.executemany("INSERT INTO %s VALUES (%s)" % (t, ",".join("?" * len(cols))), rows[t])
con.commit()
con.execute("ANALYZE")
con.commit()
print({t: len(r) for t, r in rows.items()}, "total", sum(len(r) for r in rows.values()))
