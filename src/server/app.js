const $ = selector => document.querySelector(selector);

let definition;
let formData;
let timer;
let lastDiscoveryValid = false;

const containers = {
  identity: $('#identity-form'),
  source: $('#source-form'),
  sink: $('#sink-form'),
  pipeline: $('#pipeline-form')
};

async function api(path, options = {}) {
  const response = await fetch(path, {headers: {'content-type': 'application/json'}, ...options});
  const text = await response.text();
  if (!response.ok) throw new Error(text);
  return text ? JSON.parse(text) : null;
}

function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function humanize(value) {
  return String(value)
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, character => character.toUpperCase())
    .replace(/Pqv1/gi, 'PQv1')
    .replace(/S3/gi, 'S3');
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, character => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[character]));
}

function resolveSchema(schema) {
  let resolved = schema || {};
  const seen = new Set();
  while (resolved.$ref && !seen.has(resolved.$ref)) {
    seen.add(resolved.$ref);
    const path = resolved.$ref.replace(/^#\//, '').split('/');
    let target = definition.schema;
    for (const segment of path) target = target?.[segment.replace(/~1/g, '/').replace(/~0/g, '~')];
    resolved = {...target, ...resolved};
    delete resolved.$ref;
  }
  return resolved;
}

function nullableSchema(schema) {
  const resolved = resolveSchema(schema);
  const choices = resolved.anyOf || resolved.oneOf;
  if (!choices) return null;
  const nonNull = choices.filter(choice => {
    const candidate = resolveSchema(choice);
    return candidate.type !== 'null' && candidate.const !== null;
  });
  return nonNull.length === 1 && nonNull.length !== choices.length ? nonNull[0] : null;
}

function atPath(path) {
  return path.reduce((value, key) => value?.[key], formData);
}

function setPath(path, value) {
  if (!path.length) {
    formData = value;
    return;
  }
  let target = formData;
  for (const key of path.slice(0, -1)) target = target[key];
  target[path.at(-1)] = value;
}

function deletePath(path) {
  const target = path.slice(0, -1).reduce((value, key) => value[key], formData);
  delete target[path.at(-1)];
}

function constValue(schema) {
  const resolved = resolveSchema(schema);
  if (Object.hasOwn(resolved, 'const')) return resolved.const;
  if (resolved.enum?.length === 1) return resolved.enum[0];
  return undefined;
}

function createValue(schema) {
  const resolved = resolveSchema(schema);
  const nullable = nullableSchema(resolved);
  if (nullable) return null;
  if (Object.hasOwn(resolved, 'default')) return structuredClone(resolved.default);
  if (Object.hasOwn(resolved, 'const')) return structuredClone(resolved.const);
  if (resolved.enum) return structuredClone(resolved.enum[0]);
  if (resolved.oneOf?.length) return createValue(resolved.oneOf[0]);
  if (resolved.anyOf?.length) return createValue(resolved.anyOf[0]);
  if (resolved.type === 'object' || resolved.properties) {
    const value = {};
    for (const name of resolved.required || []) value[name] = createValue(resolved.properties?.[name]);
    for (const [name, property] of Object.entries(resolved.properties || {})) {
      const item = resolveSchema(property);
      if (!Object.hasOwn(value, name) && Object.hasOwn(item, 'default')) value[name] = structuredClone(item.default);
    }
    return value;
  }
  if (resolved.type === 'array') return [];
  if (resolved.type === 'boolean') return false;
  if (resolved.type === 'integer' || resolved.type === 'number') return resolved.minimum || 0;
  return '';
}

function branchKey(schema) {
  const resolved = resolveSchema(schema);
  const properties = resolved.properties || {};
  const required = resolved.required || [];
  if (required.length === 1 && properties[required[0]]) return required[0];
  for (const [name, property] of Object.entries(properties)) {
    if (constValue(property) !== undefined) return name;
  }
  return null;
}

function branchLabel(schema, index) {
  const resolved = resolveSchema(schema);
  if (resolved.title) return resolved.title;
  const key = branchKey(resolved);
  if (key) {
    const constant = constValue(resolved.properties?.[key]);
    return humanize(constant ?? key);
  }
  if (Object.hasOwn(resolved, 'const')) return humanize(resolved.const);
  return `Option ${index + 1}`;
}

function branchMatches(schema, value) {
  const resolved = resolveSchema(schema);
  if (Object.hasOwn(resolved, 'const')) return value === resolved.const;
  if (resolved.type === 'string' && resolved.enum?.length === 1) return value === resolved.enum[0];
  if (!value || typeof value !== 'object') return false;
  for (const [name, property] of Object.entries(resolved.properties || {})) {
    const constant = constValue(property);
    if (constant !== undefined && value[name] !== constant) return false;
  }
  return (resolved.required || []).every(name => Object.hasOwn(value, name));
}

function fieldShell(schema, name, path) {
  const resolved = resolveSchema(schema);
  const shell = element('div', 'field');
  const label = element('label', '', resolved.title || humanize(name));
  label.htmlFor = `field-${path.join('-')}`;
  shell.append(label);
  if (resolved.description) shell.append(element('p', 'field-help', resolved.description));
  return shell;
}

function updateAndDiscover(path, value) {
  setPath(path, value);
  scheduleDiscovery();
}

function renderNullable(schema, value, path, name) {
  const inner = nullableSchema(schema);
  const wrapper = element('div', 'optional-block');
  const switchRow = element('label', 'optional-toggle');
  const input = document.createElement('input');
  input.type = 'checkbox';
  input.checked = value !== null && value !== undefined;
  const control = element('span', 'switch');
  switchRow.append(input, control, element('span', '', `Configure ${resolveSchema(schema).title || humanize(name)}`));
  wrapper.append(switchRow);
  input.onchange = () => {
    if (input.checked) setPath(path, createValue(inner)); else deletePath(path);
    renderEditor();
    scheduleDiscovery();
  };
  if (input.checked) wrapper.append(renderNode(inner, value, path, name, {nested: true}));
  return wrapper;
}

function renderUnion(schema, value, path, name) {
  const resolved = resolveSchema(schema);
  const choices = resolved.oneOf || resolved.anyOf;
  const selectedIndex = Math.max(0, choices.findIndex(choice => branchMatches(choice, value)));
  const selected = resolveSchema(choices[selectedIndex]);
  const wrapper = element('div', 'union');
  const shell = fieldShell(resolved, name, path);
  const select = document.createElement('select');
  select.id = `field-${path.join('-')}`;
  choices.forEach((choice, index) => {
    const option = element('option', '', branchLabel(choice, index));
    option.value = String(index);
    option.selected = index === selectedIndex;
    select.append(option);
  });
  shell.append(select);
  wrapper.append(shell);
  select.onchange = () => {
    const choice = choices[Number(select.value)];
    const key = branchKey(choice);
    let next = createValue(choice);
    if (path.length === 1 && (path[0] === 'source' || path[0] === 'sink') && key) {
      const presets = path[0] === 'source' ? definition.source_presets : definition.sink_presets;
      next = {[key]: structuredClone(presets[key] ?? next[key])};
    }
    updateAndDiscover(path, next);
    renderEditor();
  };

  if (selected.type === 'object' || selected.properties) {
    const discriminator = Object.entries(selected.properties || {}).find(([, property]) => constValue(property) !== undefined)?.[0];
    const body = element('div', 'union-body');
    renderObjectFields(selected, value || {}, path, body, new Set(discriminator ? [discriminator] : []));
    wrapper.append(body);
  }
  return wrapper;
}

function renderObjectFields(schema, value, path, target, excluded = new Set()) {
  const resolved = resolveSchema(schema);
  for (const [propertyName, propertySchema] of Object.entries(resolved.properties || {})) {
    if (excluded.has(propertyName)) continue;
    const propertyPath = [...path, propertyName];
    target.append(renderNode(propertySchema, value?.[propertyName], propertyPath, propertyName));
  }
}

function renderArray(schema, value, path, name) {
  const resolved = resolveSchema(schema);
  const items = Array.isArray(value) ? value : [];
  const shell = element('div', 'array-field');
  const heading = element('div', 'array-heading');
  const copy = element('div');
  copy.append(element('label', '', resolved.title || humanize(name)));
  if (resolved.description) copy.append(element('p', 'field-help', resolved.description));
  const add = element('button', 'icon-button', '＋ Add');
  add.type = 'button';
  heading.append(copy, add);
  shell.append(heading);
  add.onclick = () => {
    items.push(createValue(resolved.items));
    updateAndDiscover(path, items);
    renderEditor();
  };
  if (!items.length) shell.append(element('div', 'array-empty', 'No entries configured'));
  const list = element('div', 'array-list');
  items.forEach((item, index) => {
    const row = element('div', 'array-item');
    const itemSchema = resolveSchema(resolved.items);
    if (itemSchema.type === 'object' || itemSchema.properties || itemSchema.oneOf) row.append(element('span', 'item-number', String(index + 1).padStart(2, '0')));
    row.append(renderNode(resolved.items, item, [...path, index], `${humanize(name)} ${index + 1}`, {arrayItem: true}));
    const remove = element('button', 'remove', '×');
    remove.type = 'button';
    remove.title = `Remove ${humanize(name)} ${index + 1}`;
    remove.onclick = () => {
      items.splice(index, 1);
      updateAndDiscover(path, items);
      renderEditor();
    };
    row.append(remove);
    list.append(row);
  });
  shell.append(list);
  return shell;
}

function renderScalar(schema, value, path, name) {
  const resolved = resolveSchema(schema);
  const shell = fieldShell(resolved, name, path);
  const id = `field-${path.join('-')}`;
  if (resolved.type === 'boolean') {
    shell.classList.add('boolean-field');
    const label = shell.querySelector('label');
    label.htmlFor = id;
    const input = document.createElement('input');
    input.type = 'checkbox';
    input.id = id;
    input.checked = Boolean(value);
    const visual = element('span', 'switch');
    const row = element('div', 'switch-row');
    row.append(input, visual, element('span', '', input.checked ? 'Enabled' : 'Disabled'));
    input.onchange = () => {
      row.lastChild.textContent = input.checked ? 'Enabled' : 'Disabled';
      updateAndDiscover(path, input.checked);
    };
    shell.append(row);
    return shell;
  }
  if (resolved.enum) {
    const select = document.createElement('select');
    select.id = id;
    resolved.enum.forEach(optionValue => {
      const option = element('option', '', humanize(optionValue));
      option.value = optionValue;
      option.selected = value === optionValue;
      select.append(option);
    });
    select.onchange = () => updateAndDiscover(path, select.value);
    shell.append(select);
    return shell;
  }
  const input = document.createElement('input');
  input.id = id;
  const widget = resolved['x-ui']?.widget;
  input.type = widget === 'password' ? 'password' : (resolved.type === 'integer' || resolved.type === 'number' ? 'number' : 'text');
  input.value = value ?? '';
  if (resolved.minimum !== undefined) input.min = resolved.minimum;
  if (resolved.maximum !== undefined) input.max = resolved.maximum;
  if (widget === 'byte_size') input.placeholder = '128MiB';
  if (widget === 'duration') input.placeholder = '30s';
  input.oninput = () => {
    const next = input.type === 'number' ? (input.value === '' ? null : Number(input.value)) : input.value;
    updateAndDiscover(path, next);
  };
  shell.append(input);
  return shell;
}

function renderNode(schema, value, path, name, options = {}) {
  const resolved = resolveSchema(schema);
  if (nullableSchema(resolved)) return renderNullable(resolved, value, path, name);
  if (resolved.oneOf || resolved.anyOf) return renderUnion(resolved, value, path, name);
  if (resolved.type === 'array') return renderArray(resolved, value, path, name);
  if (resolved.type === 'object' || resolved.properties) {
    const group = element('fieldset', options.arrayItem ? 'object-group array-object' : 'object-group');
    if (!options.arrayItem) group.append(element('legend', '', resolved.title || humanize(name)));
    if (resolved.description) group.append(element('p', 'field-help group-help', resolved.description));
    renderObjectFields(resolved, value || {}, path, group);
    return group;
  }
  return renderScalar(resolved, value, path, name);
}

function renderEditor() {
  if (!definition || !formData) return;
  Object.values(containers).forEach(container => container.replaceChildren());
  const root = resolveSchema(definition.schema);
  const properties = root.properties;
  for (const name of ['delivery_id', 'durable_storage']) containers.identity.append(renderNode(properties[name], formData[name], [name], name));
  containers.source.append(renderNode(properties.source, formData.source, ['source'], 'Source type'));
  containers.sink.append(renderNode(properties.sink, formData.sink, ['sink'], 'Destination type'));
  for (const name of ['pipeline_memory_limit_bytes', 'keep_system_columns_in_sink', 'metrics', 'middlewares']) {
    containers.pipeline.append(renderNode(properties[name], formData[name], [name], name));
  }
  const source = Object.keys(formData.source || {})[0] || '—';
  const sink = Object.keys(formData.sink || {})[0] || '—';
  $('#provider-route').textContent = `${source} → ${sink}`;
}

function renderSchema(discovery) {
  const route = `${escapeHtml(discovery.source)} → ${escapeHtml(discovery.sink)}`;
  $('#provider-route').innerHTML = route;
  $('#schema').innerHTML = discovery.datasets.map(dataset => `
    <article class="dataset">
      <div class="dataset-heading"><div><span>${escapeHtml(dataset.role)}</span><h3>${escapeHtml(dataset.name)}</h3></div><b>${dataset.columns.length} columns</b></div>
      <div class="table-wrap"><table><thead><tr><th>Column</th><th>Arrow type</th><th>Contract</th></tr></thead><tbody>
      ${dataset.columns.map(column => `<tr><td><code>${escapeHtml(column.name)}</code></td><td>${escapeHtml(column.arrow_type)}</td><td>${escapeHtml([column.nullable && 'nullable', column.primary_key && 'primary key', column.low_cardinality && 'low cardinality', column.max_length && `max ${column.max_length}`].filter(Boolean).join(' · ') || 'required')}</td></tr>`).join('')}
      </tbody></table></div>
    </article>`).join('');
}

function setValidation(mode, message) {
  const state = $('#validation-state');
  state.className = `validation-state ${mode}`;
  state.innerHTML = `<i></i>${escapeHtml(message)}`;
}

function scheduleDiscovery() {
  clearTimeout(timer);
  lastDiscoveryValid = false;
  $('#save').disabled = true;
  setValidation('checking', 'Checking contract');
  timer = setTimeout(async () => {
    try {
      const result = await api('/api/discover', {method: 'POST', body: JSON.stringify({config: formData})});
      $('#error').textContent = '';
      renderSchema(result);
      lastDiscoveryValid = true;
      $('#save').disabled = false;
      setValidation('valid', 'Contract valid');
    } catch (error) {
      $('#schema').innerHTML = '<div class="empty-state"><span>!</span><p>Discovery will resume when the configuration is valid and the source is reachable.</p></div>';
      $('#error').textContent = error.message;
      setValidation('invalid', 'Needs attention');
    }
  }, 450);
}

function renderDeliveries(deliveries) {
  if (!deliveries.length) {
    $('#deliveries').innerHTML = '<div class="delivery-empty"><span>↗</span><h2>No routes yet</h2><p>Define a source-to-destination contract to begin.</p></div>';
    return;
  }
  $('#deliveries').innerHTML = deliveries.map(delivery => {
    const active = typeof delivery.status !== 'string';
    const status = active ? `active · pid ${Number(delivery.status.active.pid)}` : delivery.status;
    return `<article class="delivery"><span class="delivery-icon">↗</span><div><strong>${escapeHtml(delivery.name)}</strong><code>${escapeHtml(delivery.id)}</code></div><span class="status ${active ? 'active' : ''}"><i></i>${escapeHtml(status)}</span>${delivery.status === 'created' ? `<button data-activate="${escapeHtml(delivery.id)}">Activate</button>` : ''}</article>`;
  }).join('');
  document.querySelectorAll('[data-activate]').forEach(button => button.onclick = () => activate(button.dataset.activate));
}

async function loadDeliveries() {
  renderDeliveries(await api('/api/deliveries'));
}

async function activate(id) {
  await api(`/api/deliveries/${id}/activate`, {method: 'POST'});
  await loadDeliveries();
}

$('#create').onclick = () => {
  formData = structuredClone(definition.initial);
  $('#name').value = formData.delivery_id;
  $('#deliveries-view').classList.add('hidden');
  $('#editor').classList.remove('hidden');
  renderEditor();
  scheduleDiscovery();
};

$('#cancel').onclick = () => {
  clearTimeout(timer);
  $('#editor').classList.add('hidden');
  $('#deliveries-view').classList.remove('hidden');
};

$('#save').onclick = async () => {
  if (!lastDiscoveryValid) return;
  try {
    await api('/api/deliveries', {method: 'POST', body: JSON.stringify({name: $('#name').value, config: formData})});
    $('#editor').classList.add('hidden');
    $('#deliveries-view').classList.remove('hidden');
    await loadDeliveries();
  } catch (error) {
    $('#error').textContent = error.message;
    setValidation('invalid', 'Save failed');
  }
};

Promise.all([api('/api/config/schema'), loadDeliveries()])
  .then(([configDefinition]) => {
    definition = configDefinition;
    formData = structuredClone(definition.initial);
  })
  .catch(error => {
    $('#deliveries').innerHTML = `<div class="fatal">${escapeHtml(error.message)}</div>`;
  });
