const $ = selector => document.querySelector(selector);
let timer;

const templates = {
  pqv1: `source:\n  pqv1:\n    discovery_endpoint: grpc://localhost:2135\n    topic_path: /demo/events\n    consumer_name: transferia-demo\n    partition_group_ids: [0]\n    auth: { type: access_token, token: demo }\n    parser:\n      common:\n        table_naming: { type: from_config, name: events }\n      json_parser:\n        conversion_error: dlq\n        unknown_fields: { action: fail }\n        columns:\n          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }\n`,
  postgres: `source:\n  postgres:\n    connection: host=localhost port=5432 user=postgres password=postgres dbname=postgres\n    trusted_plaintext: true\n    tables:\n      - { schema: public, name: events }\n`,
  clickhouse: `source:\n  clickhouse:\n    endpoint: localhost:9000\n    trusted_plaintext: true\n    tables:\n      - { database: default, name: events, output_name: events, order_by: [id] }\n`,
  s3: `source:\n  s3:\n    bucket: demo\n    prefix: input\n    region: us-east-1\n    allow_http: true\n    endpoint: http://localhost:4566\n    credentials: { access_key: test, secret_key: test }\n    parser:\n      common:\n        table_naming: { type: from_config, name: events }\n      json_parser:\n        conversion_error: dlq\n        unknown_fields: { action: fail }\n        columns:\n          - { jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }\n`
  ,
  ytsaurus: `source:\n  ytsaurus:\n    endpoint: http://localhost:8000\n    trusted_plaintext: true\n    tables:\n      - { path: //home/demo/events, output_name: events }\n`
};
const sinks = {
  clickhouse: `sink:\n  clickhouse:\n    endpoint: localhost:9000\n    trusted_plaintext: true\n`,
  postgres: `sink:\n  postgres:\n    connection: host=localhost port=5432 user=postgres password=postgres dbname=postgres\n    trusted_plaintext: true\n    create_tables: true\n`,
  pqv1: `sink:\n  pqv1:\n    endpoint: grpc://localhost:2135\n    topic_path: /demo/output\n    message_group_id: transferia-demo\n    partition_group_id: 0\n    auth: { type: access_token, token: demo }\n    trusted_plaintext: true\n`,
  s3: `sink:\n  s3:\n    bucket: demo\n    object_layout_version: 5\n    region: us-east-1\n    allow_http: true\n    endpoint: http://localhost:4566\n    credentials: { access_key: test, secret_key: test }\n    rotation: { max_rows: 10000, max_bytes: 32MiB }\n    buffering: { max_epoch_buffers: 32, max_pending_upload_objects: 64, max_buffered_bytes: 128MiB, max_epoch_bytes: 64MiB }\n    upload: { multipart_threshold: 25MiB, part_size: 5MiB, parallel_parts: 2, max_in_flight_objects: 2 }\n    retry: { initial_backoff: 100ms, max_backoff: 5s, max_attempts: 10 }\n`
  ,
  ytsaurus: `sink:\n  ytsaurus:\n    endpoint: http://localhost:8000\n    trusted_plaintext: true\n    replace_tables: true\n    format: arrow\n    tables:\n      - { dataset: events, path: //home/demo/events_out }\n`,
  discard: `sink:\n  discard: {}\n`
};

async function api(path, options = {}) {
  const response = await fetch(path, {headers: {'content-type': 'application/json'}, ...options});
  const text = await response.text();
  if (!response.ok) throw new Error(text);
  return text ? JSON.parse(text) : null;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, character => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[character]));
}

function rebuildConfig() {
  const source = templates[$('#source').value];
  const sink = sinks[$('#sink').value];
  if (!source || !sink) {
    $('#config').value = '';
    $('#schema').innerHTML = '';
    $('#error').textContent = 'This provider has no explicit demo template.';
    return;
  }
  $('#config').value = `${source}${sink}middlewares: []\npipeline_memory_limit_bytes: 268435456\nkeep_system_columns_in_sink: false\n`;
  scheduleDiscovery();
}

function renderSchema(discovery) {
  $('#schema').innerHTML = discovery.datasets.map(dataset => `<div class="dataset"><h3>${escapeHtml(dataset.role)}: ${escapeHtml(dataset.name)}</h3><table><tr><th>Column</th><th>Arrow</th><th>Properties</th></tr>${dataset.columns.map(column => `<tr><td>${escapeHtml(column.name)}</td><td>${escapeHtml(column.arrow_type)}</td><td>${escapeHtml([column.nullable && 'nullable', column.primary_key && 'PK', column.low_cardinality && 'low-cardinality', column.max_length && `max ${column.max_length}`].filter(Boolean).join(', ') || '—')}</td></tr>`).join('')}</table></div>`).join('');
}

function scheduleDiscovery() {
  clearTimeout(timer);
  timer = setTimeout(async () => {
    try {
      const result = await api('/api/discover', {method: 'POST', body: JSON.stringify({config_yaml: $('#config').value})});
      $('#error').textContent = '';
      renderSchema(result);
    } catch (error) { $('#schema').innerHTML = ''; $('#error').textContent = error.message; }
  }, 350);
}

async function loadDeliveries() {
  const deliveries = await api('/api/deliveries');
  $('#deliveries').innerHTML = deliveries.length ? deliveries.map(delivery => `<div class="delivery"><div><strong>${escapeHtml(delivery.name)}</strong><div class="hint">${escapeHtml(delivery.id)}</div></div><span class="status">${typeof delivery.status === 'string' ? escapeHtml(delivery.status) : `active · pid ${Number(delivery.status.active.pid)}`}</span>${delivery.status === 'created' ? `<button data-activate="${escapeHtml(delivery.id)}">Activate</button>` : ''}</div>`).join('') : '<p class="hint">No deliveries yet.</p>';
  document.querySelectorAll('[data-activate]').forEach(button => button.onclick = () => activate(button.dataset.activate));
}

window.activate = async id => { await api(`/api/deliveries/${id}/activate`, {method: 'POST'}); await loadDeliveries(); };
$('#create').onclick = () => { $('#editor').classList.remove('hidden'); rebuildConfig(); };
$('#cancel').onclick = () => $('#editor').classList.add('hidden');
$('#source').onchange = rebuildConfig; $('#sink').onchange = rebuildConfig; $('#config').oninput = scheduleDiscovery;
$('#save').onclick = async () => { await api('/api/deliveries', {method: 'POST', body: JSON.stringify({name: $('#name').value, config_yaml: $('#config').value})}); $('#editor').classList.add('hidden'); await loadDeliveries(); };

api('/api/providers').then(providers => {
  $('#source').innerHTML = providers.sources.map(name => `<option>${name}</option>`).join('');
  $('#sink').innerHTML = providers.sinks.map(name => `<option>${name}</option>`).join('');
  loadDeliveries();
});
