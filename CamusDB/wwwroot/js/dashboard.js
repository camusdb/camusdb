/*
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * The dashboard's poll loop. Plain JavaScript, no framework, no bundler.
 *
 * Five rules shape everything below:
 *
 *   1. Each panel owns its own timer and its own failure. One panel that cannot
 *      load must never blank the page or stop its neighbours.
 *   2. A hidden tab polls nothing. A dashboard left open on a second monitor
 *      overnight must not keep asking a production node for numbers nobody is
 *      reading.
 *   3. Rates are computed here, not on the server. The engine's counters
 *      accumulate from process start and never reset, and the server holds no
 *      previous sample, so only the browser can subtract two readings.
 *   4. A 403 stops that panel's timer. A privilege does not change between
 *      polls, so retrying is pure waste.
 *   5. Nothing is stored but the theme. The session lives in an HttpOnly cookie
 *      that script cannot read, which is the point of putting it there.
 * ─────────────────────────────────────────────────────────────────────────────
 */

(function () {
  'use strict';

  var MAX_BACKOFF_MS = 30000;

  // ── Theme ────────────────────────────────────────────────────────────────
  // Stamps data-theme on the root element, the same mechanism the documentation
  // site uses. The initial stamp happens in an inline script in the layout so
  // the page never paints the wrong theme first; this only wires the toggle.

  function storedTheme() {
    try { return localStorage.getItem('camus-theme'); } catch (e) { return null; }
  }

  function saveTheme(value) {
    // A private window can throw on write. A theme that does not persist is a
    // far smaller problem than a dashboard that fails to load.
    try { localStorage.setItem('camus-theme', value); } catch (e) { /* ignore */ }
  }

  function currentTheme() {
    var stamped = document.documentElement.getAttribute('data-theme');
    if (stamped) return stamped;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  function wireTheme() {
    var button = document.getElementById('theme-toggle');
    if (!button) return;

    function label() {
      button.textContent = currentTheme() === 'dark' ? 'Light theme' : 'Dark theme';
    }

    label();
    button.addEventListener('click', function () {
      var next = currentTheme() === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      saveTheme(next);
      label();
    });
  }

  // ── Fetch ────────────────────────────────────────────────────────────────

  function getJson(url) {
    return fetch(url, {
      credentials: 'same-origin',
      headers: { 'Accept': 'application/json' },
    }).then(function (response) {
      if (response.status === 401) {
        // The session expired or was revoked. Nothing on the page will work
        // again until a new one exists.
        window.location.href = '/SignIn';
        return Promise.reject({ handled: true });
      }
      return response.json()
        .catch(function () { return {}; })
        .then(function (body) {
          if (!response.ok) {
            return Promise.reject({
              status: response.status,
              code: body.code,
              message: body.message || ('Request failed with status ' + response.status),
            });
          }
          return body;
        });
    });
  }

  // ── Panels ───────────────────────────────────────────────────────────────

  /**
   * One self-refreshing panel.
   *
   * `render` receives the payload. If it throws, the panel degrades exactly as
   * a network failure does, so a malformed payload cannot take the page down.
   */
  function Panel(options) {
    this.url = options.url;
    this.intervalMs = options.intervalMs;
    this.render = options.render;
    this.element = options.element;
    this.timer = null;
    this.backoffMs = 0;
    this.stopped = false;
    this.lastGoodAt = null;
  }

  Panel.prototype.degrade = function (title, detail) {
    if (!this.element) return;
    var age = '';
    if (this.lastGoodAt !== null) {
      var seconds = Math.round((Date.now() - this.lastGoodAt) / 1000);
      age = ' Last reading ' + seconds + 's ago.';
    }
    this.element.innerHTML = '';
    this.element.appendChild(box(title, detail + age));
  };

  Panel.prototype.poll = function () {
    var panel = this;
    if (panel.stopped) return;

    getJson(panel.url).then(function (body) {
      panel.backoffMs = 0;
      panel.lastGoodAt = Date.now();
      try {
        panel.render(body);
      } catch (e) {
        panel.degrade('This panel could not be drawn', 'The node answered, but the answer did not fit the page.');
      }
      panel.schedule(panel.intervalMs);
    }).catch(function (failure) {
      if (failure && failure.handled) return;

      if (failure && failure.status === 403) {
        // A privilege does not change between polls. Stop asking.
        panel.stopped = true;
        panel.degrade('You do not have access to this', failure.message || 'This panel needs a superuser.');
        return;
      }

      panel.degrade('This panel is not refreshing', (failure && failure.message) || 'The node did not answer.');
      panel.backoffMs = panel.backoffMs ? Math.min(panel.backoffMs * 2, MAX_BACKOFF_MS) : panel.intervalMs;
      panel.schedule(panel.backoffMs);
    });
  };

  Panel.prototype.schedule = function (delayMs) {
    if (this.stopped) return;
    clearTimeout(this.timer);
    if (document.hidden) return;      // resumed by the visibility handler
    this.timer = setTimeout(this.poll.bind(this), delayMs);
  };

  Panel.prototype.start = function () { this.poll(); };

  Panel.prototype.pause = function () { clearTimeout(this.timer); this.timer = null; };

  // ── Rate tracking ────────────────────────────────────────────────────────

  /**
   * Turns cumulative counters into rates.
   *
   * Keyed by metric plus tag set, because one instrument splits into many
   * series. A counter can only grow, so a value that went down means the
   * process restarted: the stored sample is dropped rather than producing a
   * large negative rate from an unrelated baseline.
   */
  function RateTracker() { this.previous = {}; }

  RateTracker.prototype.rate = function (key, value, monotonicMs) {
    var last = this.previous[key];
    this.previous[key] = { value: value, at: monotonicMs };

    if (!last) return null;                       // first sample: no rate exists yet
    if (value < last.value) return null;          // counter went backwards: restarted
    var elapsed = (monotonicMs - last.at) / 1000;
    if (elapsed <= 0) return null;
    return (value - last.value) / elapsed;
  };

  // ── Rendering helpers ────────────────────────────────────────────────────

  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  }

  function box(title, detail) {
    var wrap = element('div', 'notice-box');
    wrap.appendChild(element('span', 'dot dot-warn'));
    var body = element('span');
    body.appendChild(element('strong', null, title));
    body.appendChild(document.createTextNode(detail));
    wrap.appendChild(body);
    return wrap;
  }

  function stateSpan(kind, word) {
    var wrap = element('span', 'state');
    wrap.appendChild(element('span', 'dot dot-' + kind));
    // The word is not decoration. It is what makes the state readable to
    // someone who cannot distinguish the dot colors.
    wrap.appendChild(document.createTextNode(word));
    return wrap;
  }

  function bytes(value) {
    if (value === null || value === undefined) return '—';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var index = 0;
    var size = value;
    while (size >= 1024 && index < units.length - 1) { size /= 1024; index++; }
    return (index === 0 ? size : size.toFixed(1)) + ' ' + units[index];
  }

  function duration(totalSeconds) {
    var days = Math.floor(totalSeconds / 86400);
    var hours = Math.floor((totalSeconds % 86400) / 3600);
    var minutes = Math.floor((totalSeconds % 3600) / 60);
    var clock = String(hours).padStart(2, '0') + ':' + String(minutes).padStart(2, '0');
    return days > 0 ? days + 'd ' + clock : clock;
  }

  function number(value, digits) {
    if (value === null || value === undefined) return '—';
    return value.toLocaleString(undefined, {
      minimumFractionDigits: digits || 0,
      maximumFractionDigits: digits || 0,
    });
  }

  function fillTable(container, headers, rows, buildRow) {
    container.innerHTML = '';

    if (rows.length === 0) {
      container.appendChild(box('Nothing to show', 'The node reported no rows.'));
      return;
    }

    var table = element('table');
    var thead = element('thead');
    var headRow = element('tr');
    headers.forEach(function (header) {
      var cell = element('th', header.align === 'right' ? 'r' : null, header.label);
      headRow.appendChild(cell);
    });
    thead.appendChild(headRow);
    table.appendChild(thead);

    var tbody = element('tbody');
    rows.forEach(function (row, index) { tbody.appendChild(buildRow(row, index)); });
    table.appendChild(tbody);
    container.appendChild(table);
  }

  // ── Wiring ───────────────────────────────────────────────────────────────

  document.addEventListener('DOMContentLoaded', function () {
    wireTheme();

    var root = document.getElementById('dashboard');
    if (!root) return;                                   // the sign-in page

    var panels = [];
    var rates = new RateTracker();

    // Summary: identity and load. Its refresh interval comes from the node, so
    // an operator can slow a busy one down without shipping a new page.
    var summaryPanel = new Panel({
      url: '/v1/dashboard/summary',
      intervalMs: 2000,
      element: document.getElementById('band-body'),
      render: function (body) {
        summaryPanel.intervalMs = (body.refreshSeconds || 2) * 1000;
        renderSummary(body);
      },
    });
    panels.push(summaryPanel);

    var metricsPanel = new Panel({
      url: '/v1/dashboard/metrics',
      intervalMs: 5000,
      element: document.getElementById('metrics-body'),
      render: function (body) { renderMetrics(body, rates); },
    });
    panels.push(metricsPanel);

    var clusterPanel = new Panel({
      url: '/v1/cluster/membership',
      intervalMs: 10000,
      element: document.getElementById('cluster-body'),
      render: renderCluster,
    });
    panels.push(clusterPanel);

    var databasesPanel = new Panel({
      url: '/v1/dashboard/databases',
      intervalMs: 30000,
      element: document.getElementById('databases-body'),
      render: renderDatabases,
    });
    panels.push(databasesPanel);

    var backupsPanel = new Panel({
      url: '/v1/backups',
      intervalMs: 60000,
      element: document.getElementById('backups-body'),
      render: renderBackups,
    });
    panels.push(backupsPanel);

    var slowQueriesPanel = new Panel({
      url: '/v1/dashboard/slow-queries',
      intervalMs: 15000,
      element: document.getElementById('slow-queries-body'),
      render: renderSlowQueries,
    });
    panels.push(slowQueriesPanel);

    // Configuration does not change under a running node, so it loads once.
    var configPanel = new Panel({
      url: '/v1/dashboard/config',
      intervalMs: 0,
      element: document.getElementById('config-body'),
      render: function (body) { renderConfig(body); configPanel.stopped = true; },
    });
    panels.push(configPanel);

    panels.forEach(function (panel) { panel.start(); });

    // A background tab polls nothing at all.
    document.addEventListener('visibilitychange', function () {
      panels.forEach(function (panel) {
        if (document.hidden) panel.pause();
        else if (!panel.stopped) panel.poll();
      });
    });

    wireSignOut();
  });

  // ── Panel renderers ──────────────────────────────────────────────────────

  function renderSummary(body) {
    var head = document.getElementById('band-head');
    head.innerHTML = '';

    var title = element('h1', null, body.localEndpoint || 'this node');
    head.appendChild(title);

    if (body.localRole) head.appendChild(element('span', 'pill', body.localRole));

    if (body.commitStalled) head.appendChild(stateSpan('bad', 'Replication stalled'));
    else if (body.ready) head.appendChild(stateSpan('ok', 'Ready'));
    else if (body.initialized) head.appendChild(stateSpan('warn', 'Not serving'));
    else head.appendChild(stateSpan('warn', 'Starting'));

    var meta = document.getElementById('band-meta');
    meta.innerHTML = '';
    [
      ['Cluster mode', body.clusterMode ? 'on' : 'off'],
      ['Authentication', body.authenticationEnabled ? 'on' : 'off'],
      ['Version', body.version || 'unknown'],
      ['Uptime', duration(body.uptimeSeconds || 0)],
      ['Data directory', body.dataDirectory || 'unset'],
    ].forEach(function (pair) {
      var span = element('span');
      span.appendChild(document.createTextNode(pair[0] + ' '));
      span.appendChild(element('b', null, pair[1]));
      meta.appendChild(span);
    });

    var strip = document.getElementById('band-strip');
    strip.innerHTML = '';
    [
      ['In-flight requests', number(body.inFlightRequests), 'Foreground data requests now executing'],
      ['Open transactions', number(body.activeTransactions), 'Explicit sessions on this node'],
      ['Prepared statements', number(body.preparedStatements), bytes(body.preparedStatementBytes) + ' retained'],
      ['Hosted partitions', number(body.hostedPartitions), body.clusterMode ? 'Served by this node' : 'Single node'],
    ].forEach(function (stat) {
      var cell = element('div');
      cell.appendChild(element('dt', null, stat[0]));
      cell.appendChild(element('dd', null, stat[1]));
      cell.appendChild(element('small', null, stat[2]));
      strip.appendChild(cell);
    });
  }

  function renderMetrics(body, rates) {
    var container = document.getElementById('metrics-body');
    container.innerHTML = '';

    document.getElementById('metrics-node').textContent =
      body.node ? 'Node ' + body.node : 'This node';

    if (!body.metricsEnabled) {
      container.appendChild(box(
        'Engine metrics are switched off',
        ' Set engine_metrics_enabled to true and restart the node to collect them.'));
      return;
    }

    if (!body.rows || body.rows.length === 0) {
      container.appendChild(box(
        'No instrument has recorded yet',
        ' Numbers appear once the node handles its first work.'));
      return;
    }

    body.rows.forEach(function (row) {
      var key = row.metric + '|' + row.tags;
      var line = element('div', 'metric');

      var name = element('span', 'name');
      name.appendChild(document.createTextNode(prettyMetric(row.metric)));
      name.appendChild(element('small', null, row.metric + (row.tags ? ' · ' + row.tags : '')));
      line.appendChild(name);

      var value = element('span', 'val');
      if (row.kind === 'counter') {
        var perSecond = rates.rate(key, row.total !== null ? row.total : row.count, body.monotonicMs);
        if (perSecond === null) {
          value.appendChild(document.createTextNode(number(row.total)));
          value.appendChild(element('u', null, ' total'));
        } else {
          value.appendChild(document.createTextNode(number(perSecond, perSecond < 10 ? 2 : 0)));
          value.appendChild(element('u', null, ' /s'));
        }
      } else if (row.kind === 'histogram') {
        var mean = row.count > 0 && row.total !== null ? row.total / row.count : null;
        value.appendChild(document.createTextNode(number(mean, 2)));
        value.appendChild(element('u', null, ' avg'));
      } else {
        value.appendChild(document.createTextNode(number(row.last)));
      }
      line.appendChild(value);

      container.appendChild(line);
    });

    if (body.omitted > 0) {
      // A truncated panel must never read as a complete one.
      container.appendChild(box(
        'Some instruments are not shown',
        ' ' + body.omitted + ' more matched but exceeded the row cap. Run SHOW ENGINE STATS for the rest.'));
    }
  }

  function prettyMetric(name) {
    var last = name.split('.').slice(1).join(' ').replace(/[._]/g, ' ');
    return last.charAt(0).toUpperCase() + last.slice(1);
  }

  function renderCluster(body) {
    var container = document.getElementById('cluster-body');
    var version = document.getElementById('cluster-version');
    if (version) version.textContent = 'Membership version ' + (body.membershipVersion || 0);

    fillTable(
      container,
      [{ label: 'Endpoint' }, { label: 'Node', align: 'right' }, { label: 'Role' }],
      body.members || [],
      function (member) {
        var row = element('tr');
        row.appendChild(element('td', 'key', member.endpoint));
        row.appendChild(element('td', 'r num', member.nodeId));
        var roleCell = element('td');
        roleCell.appendChild(stateSpan(member.role === 'Leaving' || member.role === 'NotMember' ? 'warn' : 'ok', member.role));
        row.appendChild(roleCell);
        return row;
      });
  }

  function renderDatabases(body) {
    var container = document.getElementById('databases-body');

    fillTable(
      container,
      [{ label: 'Name' }, { label: 'Id' }, { label: 'Branched from' }, { label: 'Memory' }],
      body.databases || [],
      function (database) {
        var row = element('tr', 'selectable');
        row.appendChild(element('td', 'key', database.name));
        row.appendChild(element('td', 'key dim', database.id));
        row.appendChild(element('td', database.branchedFrom ? 'key dim' : 'dim', database.branchedFrom || '—'));
        // "Not loaded", never "Evicted": a database that has not been opened since the
        // node started was never in memory, so nothing evicted it. And neither state is
        // a warning — leaving an idle database on disk is what idle eviction is for.
        var memory = element('td');
        memory.appendChild(database.resident
          ? stateSpan('info', 'Loaded')
          : stateSpan('idle', 'Not loaded'));
        row.appendChild(memory);

        // On click only. This opens a descriptor, and doing that on a timer
        // would keep every listed database resident for as long as the tab is
        // open, defeating idle eviction.
        row.addEventListener('click', function () { selectDatabase(database.name, row); });
        return row;
      });
  }

  function selectDatabase(name, row) {
    var previous = document.querySelector('#databases-body tr.selected');
    if (previous) previous.classList.remove('selected');
    row.classList.add('selected');

    var container = document.getElementById('tables-body');
    var heading = document.getElementById('tables-heading');
    heading.textContent = 'Relations in ' + name;
    container.innerHTML = '';
    container.appendChild(box('Loading', ' Reading the relations of ' + name + '.'));

    getJson('/v1/dashboard/databases/' + encodeURIComponent(name) + '/tables')
      .then(function (body) {
        fillTable(
          container,
          [{ label: 'Relation' }, { label: 'Kind' }],
          body.tables || [],
          function (table) {
            var line = element('tr');
            line.appendChild(element('td', 'key', table.name));
            var kind = element('td');
            kind.appendChild(element('span', 'pill', table.kind));
            line.appendChild(kind);
            return line;
          });
      })
      .catch(function (failure) {
        if (failure && failure.handled) return;
        container.innerHTML = '';
        container.appendChild(box('These relations could not be read',
          ' ' + ((failure && failure.message) || 'The node did not answer.')));
      });
  }

  function renderBackups(body) {
    var container = document.getElementById('backups-body');

    fillTable(
      container,
      [{ label: 'Taken' }, { label: 'Kind' }, { label: 'Size', align: 'right' }],
      body.backups || [],
      function (backup) {
        var row = element('tr');
        row.appendChild(element('td', 'num', (backup.createdAt || '').replace('T', ' ').slice(0, 16)));
        var kind = element('td');
        kind.appendChild(element('span', 'pill', backup.kind || backup.type || 'backup'));
        row.appendChild(kind);
        row.appendChild(element('td', 'r num', bytes(backup.sizeBytes)));
        return row;
      });
  }

  /**
   * The newest slow statements on this node.
   *
   * Three things this panel must say out loud, because a reader who assumes
   * otherwise draws the opposite conclusion from an empty table:
   *
   *  - the log can be switched off, and off looks exactly like "nothing slow";
   *  - the entries are one node's, not the cluster's;
   *  - the ring wraps, so an empty-looking history may simply have scrolled
   *    past. `seq` above the capacity is what reveals that.
   */
  function renderSlowQueries(body) {
    var container = document.getElementById('slow-queries-body');
    var label = document.getElementById('slow-queries-node');

    label.textContent = body.node ? 'Node ' + body.node : 'This node';

    if (!body.logEnabled) {
      container.innerHTML = '';
      container.appendChild(box(
        'The slow query log is switched off',
        ' Set slow_query_log_enabled to true and restart the node to collect statements.'));
      return;
    }

    fillTable(
      container,
      [
        { label: 'When' },
        { label: 'Took', align: 'right' },
        { label: 'Kind' },
        { label: 'Database' },
        { label: 'User' },
        { label: 'Rows out', align: 'right' },
        { label: 'Rows read', align: 'right' },
        { label: 'Why' },
        { label: 'Outcome' },
        { label: 'Statement' },
      ],
      body.rows || [],
      function (entry) {
        var row = element('tr');

        row.appendChild(element('td', 'dim', clockOf(entry.startedAt)));
        row.appendChild(element('td', 'r num', millis(entry.durationMs)));
        row.appendChild(element('td', 'dim', entry.kind));
        row.appendChild(element('td', 'key', entry.database || '—'));
        row.appendChild(element('td', 'dim', entry.user || '—'));
        row.appendChild(element('td', 'r num', number(entry.rowsReturned)));
        row.appendChild(element('td', 'r num', number(entry.rowsRead)));

        // The two flags are the panel's whole reason for existing: they turn
        // "this was slow" into "this was slow because".
        var why = element('td');
        if (entry.fullScan) why.appendChild(element('span', 'pill', 'Full scan'));
        if (entry.spilled) why.appendChild(element('span', 'pill', 'Spilled'));
        if (!entry.fullScan && !entry.spilled) why.appendChild(document.createTextNode('—'));
        row.appendChild(why);

        var outcome = element('td');
        if (entry.outcome === 'failed') {
          outcome.appendChild(stateSpan('bad', entry.errorCode || 'failed'));
        } else if (entry.outcome === 'abandoned') {
          outcome.appendChild(stateSpan('warn', 'abandoned'));
        } else {
          outcome.appendChild(stateSpan('ok', 'completed'));
        }
        row.appendChild(outcome);

        // The text is set through textContent by `element`, never innerHTML: it
        // is whatever a client sent, and this page renders it back to an
        // operator.
        var statement = element('td', 'key sql', entry.sql + (entry.truncated ? ' …' : ''));
        statement.title = entry.sql;
        row.appendChild(statement);

        return row;
      });

    if (body.newestSequence > body.capacity) {
      container.appendChild(box(
        'Older entries have been overwritten',
        ' ' + number(body.newestSequence) + ' statements have been recorded and the log holds ' +
        number(body.capacity) + '. Raise slow_query_log_max_entries or the threshold to keep more.'));
    }

    if (body.omitted > 0) {
      container.appendChild(box(
        'Some entries are not shown',
        ' ' + number(body.omitted) + ' more are held. Run SHOW SLOW QUERIES for the rest.'));
    }
  }

  /** Wall-clock time of day from the entry's ISO-8601 UTC stamp, in the reader's zone. */
  function clockOf(isoUtc) {
    var when = new Date(isoUtc);
    return isNaN(when.getTime()) ? isoUtc : when.toLocaleTimeString();
  }

  /** A duration a reader can compare at a glance: sub-second stays in ms, above that becomes seconds. */
  function millis(value) {
    if (value === null || value === undefined) return '—';
    return value >= 1000 ? number(value / 1000, 2) + ' s' : number(value, 0) + ' ms';
  }

  function renderConfig(body) {
    var container = document.getElementById('config-body');

    fillTable(
      container,
      [{ label: 'Setting' }, { label: 'Value' }, { label: 'Changes' }, { label: 'Agreement' }],
      body.variables || [],
      function (variable) {
        var row = element('tr');
        row.appendChild(element('td', 'key', variable.name));
        row.appendChild(element('td', 'key', variable.value === null ? '—' : variable.value));
        var mutability = element('td');
        mutability.appendChild(element('span', 'pill', variable.mutability === 'runtime' ? 'Live' : 'Restart'));
        row.appendChild(mutability);
        row.appendChild(element('td', 'dim', variable.scope));
        return row;
      });

    var overlay = document.getElementById('overlay-body');
    if (!body.overlayAvailable) {
      overlay.innerHTML = '';
      overlay.appendChild(box('This node carries no cluster-settings overlay',
        ' The engine was composed without the service, so there is nothing to report.'));
      return;
    }

    fillTable(
      overlay,
      [{ label: 'Setting' }, { label: 'Value' }],
      body.clusterSettings || [],
      function (setting) {
        var row = element('tr');
        row.appendChild(element('td', 'key', setting.name));
        row.appendChild(element('td', 'key', setting.value === null ? '—' : setting.value));
        return row;
      });
  }

  // ── Sign out ─────────────────────────────────────────────────────────────

  function wireSignOut() {
    var button = document.getElementById('sign-out');
    if (!button) return;

    button.addEventListener('click', function () {
      fetch('/v1/dashboard/logout', { method: 'POST', credentials: 'same-origin' })
        .then(function () { window.location.href = '/SignIn'; })
        // Even a failed revocation sends the operator to the sign-in page: the
        // cookie is cleared regardless, so staying here would only show panels
        // that can no longer load.
        .catch(function () { window.location.href = '/SignIn'; });
    });
  }
}());
