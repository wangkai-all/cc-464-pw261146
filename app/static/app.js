'use strict';

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const fmt = (n, digits = 2) =>
  n === null || n === undefined ? '—'
    : Number(n).toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits });
const int = (n) => (n === null || n === undefined ? '—' : Number(n).toLocaleString('zh-CN'));
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const timeOnly = (iso) => (iso ? iso.slice(11, 19) : '—');

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok && !(res.status === 202)) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

const state = {
  auto: true,
  selectedJob: null,
  pinnedVersion: null,
  lastVersion: null,
};

function note(target, payload) {
  const el = $(target);
  el.classList.remove('muted');
  el.textContent = typeof payload === 'string' ? payload : JSON.stringify(payload, null, 2);
}

function fail(target, err) {
  const el = $(target);
  el.classList.remove('muted');
  el.textContent = '✗ ' + (err.message || err);
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

function renderMetrics(s) {
  const v = s.view;
  $('#m-version').textContent = 'v' + v.version;
  $('#m-updated').textContent = v.updated_at ? '更新于 ' + timeOnly(v.updated_at) : '尚未物化';
  $('#m-watermark').textContent = int(v.watermark);
  $('#m-factwater').textContent = `事实流 ${int(s.fact_watermark)} · ${int(s.fact_events)} 事件 / ${int(s.fact_keys)} 键`;
  const pending = v.pending_partitions || [];
  $('#m-pending').textContent = int(pending.length);
  $('#m-range').textContent = pending.length
    ? `${pending[0].day} → ${pending[pending.length - 1].day}`
    : '全部分区已同步';
  if (s.lock) {
    const expired = s.lock.expired;
    $('#m-lock').textContent = expired ? '已过期' : '占用中';
    $('#m-lockdetail').textContent = `${s.lock.owner} · 至 ${timeOnly(s.lock.expires_at)}`;
    $('#m-lock').className = expired ? 'warn' : '';
  } else {
    $('#m-lock').textContent = '空闲';
    $('#m-lockdetail').textContent = '无并发任务';
  }

  renderPending(v.pending_partitions || []);
  renderGenerations(v.partition_generations || []);
  renderJobs(s.jobs || []);
}

function renderPending(rows) {
  const body = $('#pending-table tbody');
  body.innerHTML = rows.length
    ? rows.map((r) => `<tr>
        <td>${esc(r.day)}</td>
        <td class="num">${int(r.fact_floor)}</td>
        <td class="num">${int(r.fact_ceil)}</td>
        <td class="num">${esc(r.marked_at || '')}</td>
      </tr>`).join('')
    : '<tr><td colspan="4" class="hint">没有待回补分区</td></tr>';
}

function renderGenerations(rows) {
  const body = $('#gen-table tbody');
  body.innerHTML = rows.length
    ? rows.map((r) => `<tr>
        <td>${esc(r.day)}</td>
        <td>v${r.version}</td>
        <td class="num">${int(r.watermark)}</td>
        <td class="num">${int(r.row_count)}</td>
        <td class="num" title="${esc(r.checksum)}">${esc(r.checksum)}</td>
      </tr>`).join('')
    : '<tr><td colspan="5" class="hint">尚无已提交分区</td></tr>';
}

const STATUS_LABEL = {
  success: '成功', failed: '失败', interrupted: '中断', retried: '已重试',
  queued: '排队', running: '执行中', noop: '无变更', aborted: '终止',
  superseded: '被取代',
};

function renderJobs(jobs) {
  const body = $('#jobs-table tbody');
  if (!jobs.length) {
    body.innerHTML = '<tr><td colspan="9" class="hint">还没有维护任务</td></tr>';
    return;
  }
  body.innerHTML = jobs.map((j) => {
    const swapped = (j.swapped || []).length;
    const reused = (j.reused || []).length;
    const swapText = j.status === 'success'
      ? `切换 ${swapped} 分区${reused ? ` · 复用 ${reused}` : ''} → v${j.version_to}`
      : '—';
    const canRetry = (j.status === 'failed' || j.status === 'interrupted') && j.attempt < j.max_attempts;
    const canResume = j.status === 'interrupted';
    const canRun = j.status === 'queued';
    return `<tr data-job="${esc(j.id)}" class="${j.id === state.selectedJob ? 'flash' : ''}">
      <td><span class="link" data-detail="${esc(j.id)}">${esc(j.id)}</span>${j.retry_of ? `<br><small class="hint">重试自 ${esc(j.retry_of)}</small>` : ''}${j.resumed_from ? `<br><small class="hint">恢复自 ${esc(j.resumed_from)}</small>` : ''}</td>
      <td><span class="status ${esc(j.status)}">${STATUS_LABEL[j.status] || esc(j.status)}</span></td>
      <td>v${j.version_from}→v${j.version_to}</td>
      <td class="num">${int(j.watermark)}</td>
      <td class="num">${j.attempt}/${j.max_attempts}</td>
      <td class="num">${(j.partitions || []).length}</td>
      <td class="num">${j.duration_ms == null ? '—' : int(j.duration_ms) + ' ms'}</td>
      <td>${esc(swapText)}</td>
      <td>
        ${canRun ? `<button class="ghost" data-run="${esc(j.id)}">执行</button>` : ''}
        ${canResume ? `<button class="ghost" data-resume="${esc(j.id)}">恢复</button>` : ''}
        ${canRetry && !canResume ? `<button class="ghost" data-retry="${esc(j.id)}">重试</button>` : ''}
      </td>
    </tr>`;
  }).join('');

  $$('[data-detail]', body).forEach((el) => el.addEventListener('click', () => selectJob(el.dataset.detail)));
  $$('[data-retry]', body).forEach((el) => el.addEventListener('click', (e) => {
    e.stopPropagation(); actRetry(el.dataset.retry);
  }));
  $$('[data-resume]', body).forEach((el) => el.addEventListener('click', (e) => {
    e.stopPropagation(); actResumeJob(el.dataset.resume);
  }));
  $$('[data-run]', body).forEach((el) => el.addEventListener('click', (e) => {
    e.stopPropagation(); actRunJob(el.dataset.run);
  }));
}

function renderView(q) {
  state.lastVersion = q.committed_version;
  const tbody = $('#view-table tbody');
  if (!q.days || !q.days.length) {
    tbody.innerHTML = '<tr><td colspan="5" class="hint">视图为空，先生成事实并触发回补</td></tr>';
  } else {
    tbody.innerHTML = q.days.map((d) => `<tr>
      <td>${esc(d.day)}</td>
      <td>v${d.version}</td>
      <td class="num">${int(d.cnt)}</td>
      <td class="num">${fmt(d.total)}</td>
      <td><div class="cat-chips">${d.categories.map((c) =>
        `<span class="chip">${esc(c.category)} · ${int(c.cnt)} · ${fmt(c.total)}</span>`).join('')}</div></td>
    </tr>`).join('');
  }
  $('#v-total-cnt').textContent = int(q.total_cnt);
  $('#v-total-amt').textContent = fmt(q.total_amount);

  const badge = $('#q-badge');
  if (q.stale) {
    badge.className = 'pill warn';
    badge.textContent = `固定旧版本 v${q.version}（当前 v${q.committed_version}）`;
  } else {
    badge.className = 'pill ok';
    badge.textContent = `版本 v${q.version} · 水位 ${q.watermark}`;
  }
}

function renderEvents(job, events) {
  $('#event-job').textContent = job
    ? `${job.id} · ${STATUS_LABEL[job.status] || job.status} · 尝试 ${job.attempt}/${job.max_attempts}${job.error ? ' · ' + job.error : ''}`
    : '—';
  const list = $('#events');
  if (!events || !events.length) {
    list.innerHTML = '<li class="hint" style="grid-template-columns:1fr">选择任务后显示事件</li>';
    return;
  }
  list.innerHTML = events.map((e) => `<li class="k-${esc(e.kind)}">
    <span class="t">${timeOnly(e.created_at)}</span>
    <span>${esc(e.kind)}</span>
    <span class="d">${esc(e.day || '')}</span>
    <span>${esc(e.detail || '')}</span>
  </li>`).join('');
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

let busy = false;
async function refresh() {
  if (busy) return;
  busy = true;
  try {
    const s = await api('/api/state');
    renderMetrics(s);
    $('#conn').className = 'pill ok';
    $('#conn').textContent = '在线';
    await refreshQuery();
    if (state.selectedJob) await selectJob(state.selectedJob, true);
  } catch (err) {
    $('#conn').className = 'pill bad';
    $('#conn').textContent = '离线';
  } finally {
    busy = false;
  }
}

async function refreshQuery() {
  const params = new URLSearchParams();
  const v = $('#q-version').value.trim();
  const d = $('#q-day').value.trim();
  if (v) params.set('version', v);
  if (d) params.set('day', d);
  try {
    renderView(await api('/api/view?' + params.toString()));
  } catch (err) {
    $('#q-badge').className = 'pill bad';
    $('#q-badge').textContent = err.message;
  }
}

async function selectJob(id, silent = false) {
  state.selectedJob = id;
  try {
    const data = await api('/api/job?id=' + encodeURIComponent(id));
    renderEvents(data.job, data.events);
  } catch (err) {
    if (!silent) note('#maint-out', '✗ ' + err.message);
  }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

async function withBusy(btn, fn) {
  if (btn) btn.disabled = true;
  try { await fn(); } finally { if (btn) btn.disabled = false; await refresh(); }
}

async function actSeed(btn) {
  await withBusy(btn, async () => {
    const res = await api('/api/seed', { method: 'POST', body: { day_count: 5, per_day: 4 } });
    note('#fact-out', `生成 ${res.seeded} 条事实\n脏分区: ${res.dirty_days.join(', ')}\n事实水位: ${res.watermark}`);
  });
}

async function actChaos(btn, scenario) {
  await withBusy(btn, async () => {
    const res = await api('/api/chaos', { method: 'POST', body: { scenario } });
    note('#fact-out', `${scenario}\n${JSON.stringify(res.inserted)}\n脏分区: ${res.dirty_days.join(', ')}`);
  });
}

function jobOptions() {
  const failDay = $('#opt-fail').value.trim();
  const failN = Number($('#opt-failn').value || 0);
  const fail_plan = failDay && failN > 0 ? { [failDay]: failN } : {};
  return {
    max_attempts: Number($('#opt-attempts').value || 3),
    slow_ms: Number($('#opt-slow').value || 0),
    fail_plan,
  };
}

async function actTrigger(btn, extra = {}) {
  await withBusy(btn, async () => {
    const res = await api('/api/trigger', { method: 'POST', body: { ...jobOptions(), ...extra } });
    const j = res.job;
    note('#maint-out', res.duplicate
      ? `已存在进行中的任务 ${j.id}（去重，未重复触发）`
      : `任务 ${j.id} ${j.status}\n分区: ${j.partitions.join(', ') || '无'}\n水位: ${j.watermark} · 版本 v${j.version_from}→v${j.version_to}`);
    state.selectedJob = j.id;
    await selectJob(j.id);
  });
}

async function actRun(btn) {
  await withBusy(btn, async () => {
    const res = await api('/api/run', { method: 'POST', body: {} });
    if (!res.ran) return note('#maint-out', '队列为空');
    const j = res.job;
    note('#maint-out', `任务 ${j.id} ${j.status}\n切换: ${res.swapped.join(', ') || '无'}\n复用: ${res.reused.join(', ') || '无'}\n耗时: ${j.duration_ms} ms`);
    state.selectedJob = j.id;
    await selectJob(j.id);
  });
}

async function actCrashThenRun(btn) {
  await withBusy(btn, async () => {
    const crashAt = Number($('#opt-crash').value || 2);
    const t = await api('/api/trigger', {
      method: 'POST',
      body: { ...jobOptions(), crash_at: crashAt, slow_ms: Math.max(Number($('#opt-slow').value || 0), 40) },
    });
    state.selectedJob = t.job.id;
    await api('/api/run-job', { method: 'POST', body: { id: t.job.id } });
    const job = (await api('/api/job?id=' + t.job.id)).job;
    if (job.status === 'interrupted') {
      const kept = job.partitions.slice(0, crashAt).length;
      note('#maint-out', `任务 ${t.job.id} 在第 ${crashAt} 个分区后中断\n状态: ${job.status}\n已提交版本未变，暂存保留 ${kept} 个分区\n可点击“恢复中断任务”`);
    } else {
      note('#maint-out', `任务 ${t.job.id} 状态 ${job.status}（未触发中断，检查分区数是否少于崩溃点）`);
    }
    await selectJob(t.job.id);
  });
}

async function actResume(btn) {
  await withBusy(btn, async () => {
    const interrupted = (await api('/api/jobs')).jobs.find((j) => j.status === 'interrupted');
    if (!interrupted) return note('#maint-out', '没有可恢复的中断任务');
    const res = await api('/api/resume', { method: 'POST', body: { id: interrupted.id } });
    note('#maint-out', `已创建恢复任务 ${res.job.id}（复用 ${interrupted.id} 的暂存）`);
    state.selectedJob = res.job.id;
    const run = await api('/api/run-job', { method: 'POST', body: { id: res.job.id, lock_wait: 3 } });
    note('#maint-out', `恢复任务 ${res.job.id} ${run.job.status}\n复用: ${run.reused.join(', ') || '无'}\n切换: ${run.swapped.join(', ')}`);
    await selectJob(res.job.id);
  });
}

async function actRetry(id) {
  try {
    const res = await api('/api/retry', { method: 'POST', body: { id } });
    note('#maint-out', `创建重试任务 ${res.job.id}（尝试 ${res.job.attempt}/${res.job.max_attempts}）`);
    state.selectedJob = res.job.id;
    await refresh();
  } catch (err) { fail('#maint-out', err); }
}

async function actResumeJob(id) {
  try {
    const res = await api('/api/resume', { method: 'POST', body: { id } });
    note('#maint-out', `创建恢复任务 ${res.job.id}（复用 ${id} 的暂存）`);
    state.selectedJob = res.job.id;
    await refresh();
  } catch (err) { fail('#maint-out', err); }
}

async function actRunJob(id) {
  try {
    const run = await api('/api/run-job', { method: 'POST', body: { id, lock_wait: 3 } });
    note('#maint-out', `任务 ${id} ${run.job.status}\n切换: ${run.swapped.join(', ') || '无'}\n复用: ${run.reused.join(', ') || '无'}`);
    state.selectedJob = id;
    await refresh();
  } catch (err) { fail('#maint-out', err); }
}

async function actRelease(btn) {
  await withBusy(btn, async () => {
    await api('/api/release-lock', { method: 'POST', body: {} });
    note('#maint-out', '维护锁已释放');
  });
}

async function actVerify(btn) {
  if (btn) btn.disabled = true;
  try {
    const res = await api('/api/verify');
    const out = $('#verify-out');
    out.classList.remove('hidden', 'muted');
    out.textContent = res.ok
      ? `✓ 全部 ${res.partitions.length} 个分区与底座事实一致\n` +
        res.partitions.map((p) => `${p.day} v${p.version} @${p.watermark} ${p.checksum}`).join('\n')
      : `✗ 漂移分区: ${res.drift.join(', ')}`;
  } catch (err) {
    fail('#verify-out', err);
  } finally {
    if (btn) btn.disabled = false;
  }
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

const ACTION_HANDLERS = {
  seed: actSeed,
  late_update: (b) => actChaos(b, 'late_update'),
  late_delete: (b) => actChaos(b, 'late_delete'),
  move_day: (b) => actChaos(b, 'move_day'),
  new_partition: (b) => actChaos(b, 'new_partition'),
  trigger: actTrigger,
  run: actRun,
  crash: actCrashThenRun,
  resume: actResume,
  release: actRelease,
};

document.addEventListener('click', (e) => {
  const btn = e.target.closest('[data-action]');
  if (!btn) return;
  const fn = ACTION_HANDLERS[btn.dataset.action];
  if (fn) fn(btn).catch((err) => fail('#maint-out', err));
});

$('#fact-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const fact = { key: fd.get('key'), category: fd.get('category') || undefined };
  const occurred = fd.get('occurred_at');
  if (occurred) fact.occurred_at = occurred;
  if (fd.getAll('op').includes('delete')) fact.op = 'delete';
  else {
    const value = fd.get('value');
    if (value === '') return fail('#fact-out', new Error('upsert 需要金额'));
    fact.value = Number(value);
  }
  try {
    const res = await api('/api/facts', { method: 'POST', body: { facts: [fact] } });
    note('#fact-out', `写入 seq=${res.watermark}\n脏分区: ${res.dirty_days.join(', ')}`);
    e.target.reset();
    await refresh();
  } catch (err) { fail('#fact-out', err); }
});

$('#q-run').addEventListener('click', () => refreshQuery());
$('#q-live').addEventListener('click', () => { $('#q-version').value = ''; refreshQuery(); });
$('#q-verify').addEventListener('click', (e) => actVerify(e.currentTarget));

$('#reset').addEventListener('click', async () => {
  if (!confirm('清空所有事实、视图与任务记录？')) return;
  await api('/api/reset', { method: 'POST', body: {} });
  state.selectedJob = null;
  renderEvents(null, []);
  note('#fact-out', '已重置');
  note('#maint-out', '已重置');
  $('#verify-out').classList.add('hidden');
  await refresh();
});

$('#toggle-auto').addEventListener('click', (e) => {
  state.auto = !state.auto;
  e.currentTarget.textContent = `自动刷新：${state.auto ? '开' : '关'}`;
});

setInterval(() => { if (state.auto) refresh(); }, 1500);
setInterval(() => { $('#clock').textContent = new Date().toLocaleTimeString('zh-CN'); }, 1000);
refresh();
