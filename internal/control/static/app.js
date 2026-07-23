const nodesEl = document.getElementById('nodes');
const scenariosEl = document.getElementById('scenarios');
const logEl = document.getElementById('log');
const connDot = document.getElementById('conn-dot');
const checkResultsEl = document.getElementById('check-results');

function log(msg) {
  const line = document.createElement('div');
  const t = new Date().toLocaleTimeString();
  line.textContent = `[${t}] ${msg}`;
  logEl.prepend(line);
  while (logEl.children.length > 200) logEl.removeChild(logEl.lastChild);
}

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
      tokenPromptDeclined = true; // stop re-prompting on every status poll
    } else {
      apiToken = t.trim();
      localStorage.setItem('dbfailsim-token', apiToken);
      res = await fetch(path, buildOpts(method, body));
      if (res.status === 401) tokenPromptDeclined = true; // wrong token; don't loop
    }
  }
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = text; }
  if (!res.ok) throw new Error(typeof data === 'string' ? data : JSON.stringify(data));
  return data;
}

function badge(kind, text) {
  return `<span class="badge badge-${kind}">${text}</span>`;
}

function renderNode(n) {
  const faulted = n.latency_ms > 0 || n.drop_percent > 0 || n.partitioned || n.crashed;
  const down = n.partitioned || n.crashed;
  const cls = down ? 'node-card down' : faulted ? 'node-card faulted' : 'node-card';

  let badges = '';
  if (down) {
    badges += badge('bad', n.crashed ? 'CRASHED' : 'PARTITIONED');
  } else {
    badges += badge('ok', 'reachable');
  }
  if (n.latency_ms > 0) badges += badge('warn', `+${n.latency_ms}ms`);
  if (n.drop_percent > 0) badges += badge('warn', `${n.drop_percent}% drop`);

  return `
  <div class="${cls}" data-node="${n.name}">
    <div class="node-name">${n.name}</div>
    <div class="node-addrs">${n.listen_addr} &rarr; ${n.upstream_addr}</div>
    <div class="node-status-line">${badges}</div>
    <div class="node-status-line" style="color:var(--muted)">active clients: ${n.active_clients}</div>
    <div class="node-actions">
      <button class="btn" data-action="latency" data-node="${n.name}">+500ms</button>
      <button class="btn" data-action="drop" data-node="${n.name}">20% drop</button>
      <button class="btn btn-danger" data-action="partition" data-node="${n.name}">Partition</button>
      <button class="btn btn-danger" data-action="crash" data-node="${n.name}">Crash</button>
      <button class="btn btn-heal" data-action="heal" data-node="${n.name}">Heal</button>
    </div>
  </div>`;
}

async function refreshStatus() {
  try {
    const nodes = await api('GET', '/status');
    connDot.className = 'dot live';
    nodesEl.innerHTML = nodes.map(renderNode).join('');
  } catch (e) {
    connDot.className = 'dot down';
  }
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
        <div>
          <div class="scenario-name">${s.name}</div>
          <div class="scenario-desc">${s.description || ''}</div>
        </div>
        <button class="btn btn-primary" data-scenario="${s.name}">Run</button>
      </div>
    `).join('');
  } catch (e) {
    scenariosEl.innerHTML = '<div class="hint">Failed to load scenarios.</div>';
  }
}

nodesEl.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  const node = btn.dataset.node;
  const action = btn.dataset.action;
  let payload = { kind: action };
  if (action === 'latency') payload.latency_ms = 500;
  if (action === 'drop') payload.drop_percent = 20;
  try {
    await api('POST', `/nodes/${encodeURIComponent(node)}/fault`, payload);
    log(`${node}: applied ${action}${action==='latency' ? ' (+500ms)' : action==='drop' ? ' (20%)' : ''}`);
  } catch (err) {
    log(`${node}: ${action} failed - ${err.message}`);
  }
  refreshStatus();
});

scenariosEl.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-scenario]');
  if (!btn) return;
  const name = btn.dataset.scenario;
  try {
    await api('POST', `/scenarios/${encodeURIComponent(name)}/run`);
    log(`scenario started: ${name}`);
  } catch (err) {
    log(`scenario ${name} failed - ${err.message}`);
  }
  setTimeout(refreshStatus, 300);
});

document.getElementById('heal-all').addEventListener('click', async () => {
  await api('POST', '/heal');
  log('healed all nodes');
  refreshStatus();
});

document.getElementById('check-run').addEventListener('click', async () => {
  const query = document.getElementById('check-query').value.trim();
  if (!query) return;
  checkResultsEl.innerHTML = '<div class="hint">Running...</div>';
  try {
    const data = await api('GET', `/check?query=${encodeURIComponent(query)}`);
    renderCheck(data);
    log(`consistency check: "${query}"`);
  } catch (err) {
    checkResultsEl.innerHTML = `<div class="hint">Failed: ${err.message}</div>`;
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
      <div class="rname">${r.node}${r.error ? ' (error)' : r.diverges ? ' — diverges' : ''}</div>
      <div class="rout">${(r.output || r.error || '').replace(/</g, '&lt;')}</div>
    </div>
  `).join('');
  if (data.agree) {
    html += `<div class="check-verdict agree">All nodes agree — data looks consistent from any client's perspective right now.</div>`;
  } else {
    html += `<div class="check-verdict diverge">Nodes disagree — a client connected to a different node right now would see different data.</div>`;
  }
  checkResultsEl.innerHTML = html;
}

refreshStatus();
loadScenarios();
setInterval(refreshStatus, 1500);
