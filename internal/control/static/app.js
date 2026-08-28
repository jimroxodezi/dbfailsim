// dbfailsim dashboard. Plain JS, no build step; talks to the control API
// on the same origin. Everything it knows about fault kinds comes from
// GET /kinds, so adding a kind to the engine's catalogue adds it here.

const nodesEl = document.getElementById('nodes');
const scenariosEl = document.getElementById('scenarios');
const logEl = document.getElementById('log');
const connDot = document.getElementById('conn-dot');
const summaryEl = document.getElementById('summary');
const checkResultsEl = document.getElementById('check-results');
const catalogEl = document.getElementById('catalog');
const injectNode = document.getElementById('inject-node');
const injectKind = document.getElementById('inject-kind');
const injectFor = document.getElementById('inject-for');
const injectDesc = document.getElementById('inject-desc');
const injectParams = document.getElementById('inject-params');
const injectResult = document.getElementById('inject-result');

let kinds = [];          // from /kinds
let kindByName = {};
let lastNodes = [];      // from /status

function esc(s) {
  return String(s ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function log(msg, cls) {
  const line = document.createElement('div');
  if (cls) line.className = cls;
  const t = new Date().toLocaleTimeString();
  line.textContent = `[${t}] ${msg}`;
  logEl.prepend(line);
  while (logEl.children.length > 300) logEl.removeChild(logEl.lastChild);
}

// ---- API client with bearer-token prompt -------------------------------

let apiToken = localStorage.getItem('dbfailsim-token') || '';
let tokenPromptDeclined = false;

function buildOpts(method, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  if (apiToken) opts.headers['Authorization'] = `Bearer ${apiToken}`;
  return opts;
}

async function api(method, path, body) {
  let res = await fetch(path, buildOpts(method, body));
  if (res.status === 401 && !tokenPromptDeclined) {
    const t = window.prompt('This control API requires a token (control_token in the server config):');
    if (t === null || !t.trim()) {
      tokenPromptDeclined = true;
    } else {
      apiToken = t.trim();
      localStorage.setItem('dbfailsim-token', apiToken);
      res = await fetch(path, buildOpts(method, body));
      if (res.status === 401) tokenPromptDeclined = true;
    }
  }
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  if (!res.ok) throw new Error(typeof data === 'string' ? data.trim() : JSON.stringify(data));
  return data;
}

// ---- Nodes ---------------------------------------------------------------

function badge(kind, text, title) {
  return `<span class="badge badge-${kind}" ${title ? `title="${esc(title)}"` : ''}>${esc(text)}</span>`;
}

function faultChip(node, name, removable) {
  const k = kindByName[name] || {};
  const cls = `chip chip-${k.class || 'conn'}`;
  const x = removable ? `<button class="chip-x" data-remove="${esc(name)}" data-node="${esc(node)}" title="remove ${esc(name)}">&times;</button>` : '';
  return `<span class="${cls}" title="${esc(k.description || name)}">${esc(name)}${x}</span>`;
}

function renderNode(n) {
  const down = n.partitioned || n.crashed;
  const wire = (n.active_faults || []).length > 0;
  const nodeFaults = n.node_faults || [];
  const cls = down ? 'node-card down' : (wire || nodeFaults.length) ? 'node-card faulted' : 'node-card';

  let badges = down ? badge('bad', n.crashed ? 'CRASHED' : 'PARTITIONED') : badge('ok', 'reachable');
  if (n.role) badges += badge('muted', n.role);
  badges += badge(n.stream === 'replication' ? 'info' : 'muted', n.stream === 'replication' ? 'replication stream' : 'query stream');
  if (n.latency_ms > 0) badges += badge('warn', `+${n.latency_ms}ms`);
  if (n.drop_percent > 0) badges += badge('warn', `${n.drop_percent}% drop`);

  let chips = '';
  if (n.partitioned) chips += faultChip(n.name, 'partition', true);
  if (n.crashed) chips += faultChip(n.name, 'crash', true);
  chips += (n.active_faults || []).map(f => faultChip(n.name, f, true)).join('');
  chips += nodeFaults.map(f => faultChip(n.name, f, true)).join('');
  if (!chips) chips = '<span class="hint">no faults</span>';

  const target = n.target
    ? `<span class="target" title="node-level faults act through this target">${esc(n.target)}</span>`
    : `<span class="target none" title="no target configured: only proxy-level faults apply">proxy-only</span>`;

  return `
  <div class="${cls}" data-node="${esc(n.name)}">
    <div class="node-head">
      <div class="node-name">${esc(n.name)}</div>
      ${target}
    </div>
    <div class="node-addrs">${esc(n.listen_addr)} &rarr; ${esc(n.upstream_addr)}</div>
    <div class="node-status-line">${badges}</div>
    <div class="node-faults">${chips}</div>
    <div class="node-status-line muted">active clients: ${n.active_clients}</div>
    <div class="node-actions">
      <button class="btn" data-action="latency" data-node="${esc(n.name)}" title="latency 500ms">+500ms</button>
      <button class="btn" data-action="drop" data-node="${esc(n.name)}" title="drop 20%">20% drop</button>
      <button class="btn" data-action="tcp_rst" data-node="${esc(n.name)}" title="reset connections">RST</button>
      <button class="btn btn-danger" data-action="partition" data-node="${esc(n.name)}">Partition</button>
      <button class="btn btn-danger" data-action="crash" data-node="${esc(n.name)}" title="proxy-level crash">Crash</button>
      ${n.target ? `<button class="btn btn-danger" data-action="node_crash" data-node="${esc(n.name)}" title="SIGKILL the process via ${esc(n.target)}">Kill</button>
      <button class="btn btn-danger" data-action="zombie" data-node="${esc(n.name)}" title="freeze the process">Zombie</button>` : ''}
      <button class="btn btn-heal" data-action="heal" data-node="${esc(n.name)}">Heal</button>
      <button class="btn" data-inject="${esc(n.name)}" title="open the inject form for this node">Inject&hellip;</button>
    </div>
  </div>`;
}

function renderSummary(nodes) {
  const total = nodes.length;
  const down = nodes.filter(n => n.partitioned || n.crashed).length;
  const faulted = nodes.filter(n => (n.active_faults || []).length || (n.node_faults || []).length).length;
  const clients = nodes.reduce((a, n) => a + (n.active_clients || 0), 0);
  summaryEl.textContent = `${total} nodes · ${down} down · ${faulted} faulted · ${clients} clients`;
}

async function refreshStatus() {
  try {
    const nodes = await api('GET', '/status');
    connDot.className = 'dot live';
    lastNodes = nodes;
    nodesEl.innerHTML = nodes.map(renderNode).join('');
    renderSummary(nodes);
    syncNodeSelect(nodes);
  } catch (e) {
    connDot.className = 'dot down';
  }
}

nodesEl.addEventListener('click', async (e) => {
  const rm = e.target.closest('button[data-remove]');
  if (rm) {
    const node = rm.dataset.node, name = rm.dataset.remove;
    try {
      await api('DELETE', `/nodes/${encodeURIComponent(node)}/faults/${encodeURIComponent(name)}`);
      log(`${node}: removed ${name}`);
    } catch (err) {
      log(`${node}: remove ${name} failed - ${err.message}`, 'err');
    }
    refreshStatus();
    return;
  }
  const open = e.target.closest('button[data-inject]');
  if (open) {
    injectNode.value = open.dataset.inject;
    document.getElementById('inject-panel').scrollIntoView({ behavior: 'smooth', block: 'start' });
    injectKind.focus();
    return;
  }
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  const node = btn.dataset.node;
  const action = btn.dataset.action;
  const payload = { kind: action };
  if (action === 'latency') payload.latency_ms = 500;
  if (action === 'drop') payload.drop_percent = 20;
  try {
    await api('POST', `/nodes/${encodeURIComponent(node)}/fault`, payload);
    log(`${node}: applied ${action}${action === 'latency' ? ' (+500ms)' : action === 'drop' ? ' (20%)' : ''}`);
  } catch (err) {
    log(`${node}: ${action} failed - ${err.message}`, 'err');
  }
  refreshStatus();
});

// ---- Inject form ---------------------------------------------------------

function syncNodeSelect(nodes) {
  const current = injectNode.value;
  const names = nodes.map(n => n.name);
  const want = ['*', ...names];
  const have = Array.from(injectNode.options).map(o => o.value);
  if (JSON.stringify(want) !== JSON.stringify(have)) {
    injectNode.innerHTML = want.map(v => `<option value="${esc(v)}">${v === '*' ? '* (every node)' : esc(v)}</option>`).join('');
    if (want.includes(current)) injectNode.value = current;
  }
}

function classLabel(c) {
  return { state: 'reachability', conn: 'connection', dial: 'dial', packet: 'packet', read_hook: 'read hook', node: 'node' }[c] || c;
}

function renderKindOptions() {
  const groups = {};
  for (const k of kinds) (groups[k.class] ||= []).push(k);
  injectKind.innerHTML = Object.entries(groups).map(([cls, ks]) =>
    `<optgroup label="${esc(classLabel(cls))}">${ks.map(k => `<option value="${esc(k.kind)}">${esc(k.kind)}</option>`).join('')}</optgroup>`
  ).join('');
  injectKind.value = 'latency';
  renderParamFields();
}

function renderParamFields() {
  const k = kindByName[injectKind.value];
  if (!k) { injectParams.innerHTML = ''; injectDesc.textContent = ''; return; }
  let desc = k.description;
  if (k.stream) desc += ` Acts on the ${k.stream} stream only.`;
  if (k.needs_target) desc += ' Needs a node target.';
  injectDesc.textContent = desc;
  injectParams.innerHTML = (k.params || []).map(p => {
    const id = `param-${p.name}`;
    let input;
    if (p.type === 'enum') {
      input = `<select id="${id}" data-param="${esc(p.name)}" data-type="${p.type}">${(p.enum || []).map(v => `<option ${v === p.default ? 'selected' : ''}>${esc(v)}</option>`).join('')}</select>`;
    } else {
      const step = p.type === 'float' ? 'step="any"' : p.type === 'int' ? 'step="1"' : '';
      const type = (p.type === 'int' || p.type === 'float') ? 'number' : 'text';
      input = `<input id="${id}" type="${type}" ${step} data-param="${esc(p.name)}" data-type="${p.type}" value="${esc(p.default ?? '')}" placeholder="${esc(p.default ?? '')}" />`;
    }
    return `<label class="param"><span class="param-name">${esc(p.name)}</span>${input}<span class="param-help">${esc(p.help || '')}</span></label>`;
  }).join('') || '<div class="hint">no parameters</div>';
}

injectKind.addEventListener('change', renderParamFields);

document.getElementById('inject-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const node = injectNode.value;
  const kind = injectKind.value;
  const params = {};
  for (const el of injectParams.querySelectorAll('[data-param]')) {
    const v = el.value.trim();
    if (v === '') continue;
    const t = el.dataset.type;
    params[el.dataset.param] = (t === 'int' || t === 'float') ? Number(v) : v;
  }
  const payload = { kind };
  if (Object.keys(params).length) payload.params = params;
  const forText = injectFor.value.trim();
  if (forText) {
    const ms = parseDurationMs(forText);
    if (ms === null) { injectResult.textContent = 'for: use e.g. 500ms, 10s, 2m'; return; }
    payload.for_ms = ms;
  }
  injectResult.textContent = 'injecting…';
  try {
    await api('POST', `/nodes/${encodeURIComponent(node)}/fault`, payload);
    const summary = `${kind}${Object.keys(params).length ? ' ' + JSON.stringify(params) : ''}${payload.for_ms ? ` for ${forText}` : ''}`;
    injectResult.textContent = `ok: ${summary}`;
    log(`${node}: injected ${summary}`);
  } catch (err) {
    injectResult.textContent = `failed: ${err.message}`;
    log(`${node}: inject ${kind} failed - ${err.message}`, 'err');
  }
  refreshStatus();
});

function parseDurationMs(s) {
  const m = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h)?$/.exec(s);
  if (!m) return null;
  const n = parseFloat(m[1]);
  const unit = m[2] || 'ms';
  return Math.round(n * { ms: 1, s: 1000, m: 60000, h: 3600000 }[unit]);
}

// ---- Catalogue -----------------------------------------------------------

async function loadKinds() {
  try {
    kinds = await api('GET', '/kinds');
    kindByName = Object.fromEntries(kinds.map(k => [k.kind, k]));
    renderKindOptions();
    catalogEl.innerHTML = kinds.map(k => `
      <div class="catalog-row">
        <div class="catalog-head">
          <span class="chip chip-${esc(k.class)}">${esc(k.kind)}</span>
          <span class="catalog-class">${esc(classLabel(k.class))}${k.stream ? ` · ${esc(k.stream)} stream` : ''}${k.needs_target ? ' · needs target' : ''}</span>
        </div>
        <div class="catalog-desc">${esc(k.description)}</div>
        ${(k.params || []).length ? `<div class="catalog-params">${k.params.map(p =>
          `<span><code>${esc(p.name)}</code> <em>${esc(p.type)}</em>${p.default !== undefined && p.default !== null ? ` = ${esc(p.default)}` : ''}${p.help ? ` — ${esc(p.help)}` : ''}</span>`).join('')}</div>` : ''}
      </div>`).join('');
  } catch (e) {
    catalogEl.innerHTML = '<div class="hint">Failed to load the fault catalogue.</div>';
  }
}

// ---- Scenarios -----------------------------------------------------------

function describeStep(st) {
  let s = `${st.node} · ${st.kind}`;
  if (st.latency_ms) s += ` ${st.latency_ms}ms`;
  if (st.drop_percent) s += ` ${st.drop_percent}%`;
  if (st.params && Object.keys(st.params).length) s += ' ' + JSON.stringify(st.params);
  s += ` @${st.after_ms || 0}ms`;
  if (st.for_ms) s += ` for ${st.for_ms}ms`;
  return s;
}

async function loadScenarios() {
  try {
    const scenarios = await api('GET', '/scenarios');
    if (!scenarios || scenarios.length === 0) {
      scenariosEl.innerHTML = '<div class="hint">No scenarios defined in config.</div>';
      return;
    }
    scenariosEl.innerHTML = scenarios.map(s => `
      <div class="scenario-row">
        <div class="scenario-main">
          <div class="scenario-name">${esc(s.name)}</div>
          <div class="scenario-desc">${esc(s.description || '')}</div>
          <div class="scenario-steps">${(s.steps || []).map(st => `<span class="step">${esc(describeStep(st))}</span>`).join('')}</div>
        </div>
        <button class="btn btn-primary" data-scenario="${esc(s.name)}">Run</button>
      </div>
    `).join('');
  } catch (e) {
    scenariosEl.innerHTML = '<div class="hint">Failed to load scenarios.</div>';
  }
}

scenariosEl.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-scenario]');
  if (!btn) return;
  const name = btn.dataset.scenario;
  try {
    await api('POST', `/scenarios/${encodeURIComponent(name)}/run`);
    log(`scenario started: ${name}`);
  } catch (err) {
    log(`scenario ${name} failed - ${err.message}`, 'err');
  }
  setTimeout(refreshStatus, 300);
});

// ---- Heal all, check --------------------------------------------------------

document.getElementById('heal-all').addEventListener('click', async () => {
  try {
    await api('POST', '/heal');
    log('healed all nodes');
  } catch (err) {
    log(`heal failed - ${err.message}`, 'err');
  }
  refreshStatus();
});

document.getElementById('check-run').addEventListener('click', async () => {
  const query = document.getElementById('check-query').value.trim();
  if (!query) return;
  checkResultsEl.innerHTML = '<div class="hint">Running...</div>';
  try {
    const data = await api('GET', `/check?query=${encodeURIComponent(query)}`);
    renderCheck(data);
    log(`consistency check: "${query}" → ${data.agree ? 'agree' : 'DISAGREE'}`);
  } catch (err) {
    checkResultsEl.innerHTML = `<div class="hint">Failed: ${esc(err.message)}</div>`;
  }
});

function renderCheck(data) {
  const results = data.results || [];
  if (results.length === 0) {
    checkResultsEl.innerHTML = '<div class="hint">No nodes have a check_command configured.</div>';
    return;
  }
  let html = results.map(r => `
    <div class="check-result-row ${r.diverges ? 'diverges' : ''}">
      <div class="rname">${esc(r.node)}${r.error ? ' (error)' : r.diverges ? ' — diverges' : ''}</div>
      <div class="rout">${esc(r.output || r.error || '')}</div>
    </div>
  `).join('');
  html += data.agree
    ? `<div class="check-verdict agree">All nodes agree — data looks consistent from any client's perspective right now.</div>`
    : `<div class="check-verdict diverge">Nodes disagree — a client connected to a different node right now would see different data.</div>`;
  checkResultsEl.innerHTML = html;
}

// ---- boot -----------------------------------------------------------------

loadKinds().then(refreshStatus);
loadScenarios();
setInterval(refreshStatus, 1500);
