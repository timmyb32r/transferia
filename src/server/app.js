const $ = selector => document.querySelector(selector);

let definition;
let formData;
let timer;
let yamlTimer;
let latestYaml = '';
let discoveryController;
let discoverySequence = 0;
let lastDiscoveryValid = false;
let activeDropdown;

const containers = {
  deliveryType: $('#delivery-type-form'),
  sourcePicker: $('#source-picker'),
  sinkPicker: $('#sink-picker'),
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

function closeActiveDropdown(restoreFocus = false) {
  activeDropdown?.close(restoreFocus);
}

function createDropdown(id, value, placeholder, options, onChange) {
  const root = element('div', 'select-control');
  const trigger = element('button', 'select-trigger');
  const label = element('span', value === undefined ? 'select-placeholder' : '', value === undefined ? placeholder : options.find(option => Object.is(option.value, value))?.label ?? placeholder);
  const chevron = element('span', 'select-chevron');
  const menu = element('div', 'select-menu');
  const search = document.createElement('input');
  const optionsList = element('div', 'select-options');
  const empty = element('div', 'select-empty', 'No matching options');
  const listboxId = `${id}-options`;

  trigger.type = 'button';
  trigger.id = id;
  trigger.setAttribute('aria-haspopup', 'listbox');
  trigger.setAttribute('aria-controls', listboxId);
  trigger.setAttribute('aria-expanded', 'false');
  chevron.setAttribute('aria-hidden', 'true');
  menu.id = `${id}-menu`;
  menu.hidden = true;
  search.type = 'search';
  search.className = 'select-search';
  search.placeholder = 'Search';
  search.setAttribute('aria-label', 'Search options');
  optionsList.id = listboxId;
  optionsList.setAttribute('role', 'listbox');
  empty.hidden = true;
  trigger.append(label, chevron);

  const optionButtons = options.map((option, index) => {
    const button = element('button', 'select-option', option.label);
    button.type = 'button';
    button.id = `${listboxId}-${index}`;
    button.setAttribute('role', 'option');
    button.setAttribute('aria-selected', String(Object.is(option.value, value)));
    if (Object.is(option.value, value)) button.classList.add('selected');
    button.onclick = () => {
      close(false);
      onChange(option.value);
    };
    button.onkeydown = event => {
      const visible = visibleOptions();
      const current = visible.indexOf(button);
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        visible[(current + 1) % visible.length].focus();
      } else if (event.key === 'ArrowUp') {
        event.preventDefault();
        visible[(current - 1 + visible.length) % visible.length].focus();
      } else if (event.key === 'Home') {
        event.preventDefault();
        visible[0].focus();
      } else if (event.key === 'End') {
        event.preventDefault();
        visible.at(-1).focus();
      } else if (event.key === 'Escape') {
        event.preventDefault();
        close(true);
      } else if (event.key === 'Tab') {
        close(false);
      }
    };
    optionsList.append(button);
    return button;
  });

  function visibleOptions() {
    return optionButtons.filter(button => !button.hidden);
  }

  function filterOptions() {
    const query = search.value.trim().toLocaleLowerCase();
    for (const button of optionButtons) {
      button.hidden = !button.textContent.toLocaleLowerCase().includes(query);
    }
    empty.hidden = visibleOptions().length !== 0;
  }

  function close(restoreFocus = false) {
    menu.hidden = true;
    root.classList.remove('open');
    trigger.setAttribute('aria-expanded', 'false');
    search.value = '';
    filterOptions();
    if (activeDropdown?.root === root) activeDropdown = undefined;
    if (restoreFocus) trigger.focus();
  }

  function open() {
    closeActiveDropdown(false);
    menu.hidden = false;
    root.classList.add('open');
    trigger.setAttribute('aria-expanded', 'true');
    activeDropdown = {root, close};
    search.focus({preventScroll: true});
  }

  search.oninput = filterOptions;
  search.onkeydown = event => {
    const visible = visibleOptions();
    if (event.key === 'ArrowDown' && visible.length) {
      event.preventDefault();
      visible[0].focus();
    } else if (event.key === 'ArrowUp' && visible.length) {
      event.preventDefault();
      visible.at(-1).focus();
    } else if (event.key === 'Escape') {
      event.preventDefault();
      close(true);
    } else if (event.key === 'Tab') {
      close(false);
    }
  };
  trigger.onclick = () => menu.hidden ? open() : close(false);
  trigger.onkeydown = event => {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      open();
      if (event.key === 'ArrowUp') optionButtons.at(-1)?.focus({preventScroll: true});
    } else if (event.key === 'Escape') {
      close(false);
    }
  };
  menu.append(search, optionsList, empty);
  root.append(trigger, menu);
  return root;
}

document.addEventListener('pointerdown', event => {
  if (activeDropdown && !activeDropdown.root.contains(event.target)) closeActiveDropdown(false);
});

function humanize(value) {
  if (value === 'batch_and_stream') return 'Batch + stream';
  return String(value)
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, character => character.toUpperCase())
    .replace(/Pqv1/gi, 'PQv1')
    .replace(/Ydb/gi, 'YDB')
    .replace(/S3/gi, 'S3');
}

function sourceBranch(providerKey) {
  if (!providerKey) return null;
  const sourceSchema = resolveSchema(resolveSchema(definition.schema).properties.source);
  return (sourceSchema.oneOf || sourceSchema.anyOf || [])
    .map(resolveSchema)
    .find(branch => branchKey(branch) === providerKey) || null;
}

function deliveryCompatibilityIssue() {
  const deliveryType = formData?.delivery_type;
  if (!deliveryType) return 'Choose a delivery type before configuring the route.';
  const providerKey = Object.keys(formData?.source || {})[0];
  if (!providerKey) {
    if (deliveryType === 'batch_and_stream') {
      return 'No source currently implements both batch and stream delivery. Choose Batch or Stream.';
    }
    return null;
  }
  const modes = sourceBranch(providerKey)?.['x-ui']?.delivery_modes || [];
  const compatible = deliveryType === 'batch_and_stream'
    ? modes.includes('batch') && modes.includes('stream')
    : modes.includes(deliveryType);
  if (compatible) return null;
  const supported = modes.length ? modes.map(humanize).join(' + ') : 'no declared delivery modes';
  return `${humanize(providerKey)} supports ${supported} delivery, not ${humanize(deliveryType)}. Choose a compatible delivery type or source.`;
}

function newDeliveryId() {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return `delivery-${Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('')}`;
}

function updateSaveState() {
  $('#save').disabled = !lastDiscoveryValid || !$('#name').value.trim();
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
  const labelRow = element('div', 'field-label');
  const label = element('label', '', resolved.title || humanize(name));
  label.htmlFor = `field-${path.join('-')}`;
  labelRow.append(label);
  if (resolved.description) {
    const help = element('button', 'help', '?');
    help.type = 'button';
    help.dataset.tooltip = resolved.description;
    help.setAttribute('aria-label', resolved.description);
    labelRow.append(help);
  }
  shell.append(labelRow);
  return shell;
}

function labelDropdown(shell, id) {
  const label = shell.querySelector('.field-label label');
  if (!label) return;
  label.removeAttribute('for');
  label.id = `${id}-label`;
  shell.querySelector('.select-trigger')?.setAttribute('aria-labelledby', label.id);
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

function renderUnion(schema, value, path, name, options = {}) {
  const resolved = resolveSchema(schema);
  const choices = resolved.oneOf || resolved.anyOf;
  const selectedIndex = choices.findIndex(choice => branchMatches(choice, value));
  const selected = selectedIndex >= 0 ? resolveSchema(choices[selectedIndex]) : null;
  const wrapper = element('div', 'union');
  if (!options.bodyOnly) {
    const shell = fieldShell(resolved, name, path);
    const id = `field-${path.join('-')}`;
    const dropdown = createDropdown(
      id,
      selectedIndex >= 0 ? selectedIndex : undefined,
      'Not selected',
      choices.map((choice, index) => ({value: index, label: branchLabel(choice, index)})),
      index => {
        const choice = choices[index];
        const key = branchKey(choice);
        let next = createValue(choice);
        if (path.length === 1 && (path[0] === 'source' || path[0] === 'sink') && key) {
          const presets = path[0] === 'source' ? definition.source_presets : definition.sink_presets;
          next = {[key]: structuredClone(presets[key] ?? next[key])};
        }
        updateAndDiscover(path, next);
        renderEditor();
        document.getElementById(id)?.focus();
      }
    );
    shell.append(dropdown);
    labelDropdown(shell, id);
    wrapper.append(shell);
  }

  if (!options.pickerOnly && selected && (selected.type === 'object' || selected.properties)) {
    const discriminator = Object.entries(selected.properties || {}).find(([, property]) => constValue(property) !== undefined)?.[0];
    const body = element('div', 'union-body');
    const providerKey = path.length === 1 ? branchKey(selected) : null;
    const providerSchema = providerKey ? selected.properties?.[providerKey] : null;
    if (providerKey && providerSchema && value?.[providerKey]) {
      renderObjectFields(resolveSchema(providerSchema), value[providerKey], [...path, providerKey], body);
    } else {
      renderObjectFields(selected, value || {}, path, body, new Set(discriminator ? [discriminator] : []));
    }
    if (path.at(-1) === 'parser' && value?.common && value?.json_parser) {
      body.append(renderSystemColumnsEditor(value, path));
    }
    wrapper.append(body);
  }
  return wrapper;
}

function renderObjectFields(schema, value, path, target, excluded = new Set()) {
  const resolved = resolveSchema(schema);
  const properties = Object.entries(resolved.properties || {})
    .filter(([propertyName, propertySchema]) => !excluded.has(propertyName) && resolveSchema(propertySchema)['x-ui']?.widget !== 'hidden');
  const regular = properties.filter(([, propertySchema]) => !resolveSchema(propertySchema)['x-ui']?.section);
  const sections = new Map();
  for (const property of properties) {
    const section = resolveSchema(property[1])['x-ui']?.section;
    if (section) sections.set(section, [...(sections.get(section) || []), property]);
  }
  for (const [propertyName, propertySchema] of regular) {
    const propertyPath = [...path, propertyName];
    target.append(renderNode(propertySchema, value?.[propertyName], propertyPath, propertyName));
  }
  for (const [section, fields] of sections) {
    const details = element('details', `advanced-settings ${section}-settings`);
    const summary = element('summary', '', section === 'system_columns' ? 'System columns' : 'Advanced settings');
    const body = element('div', 'advanced-settings-body');
    for (const [propertyName, propertySchema] of fields) {
      const propertyPath = [...path, propertyName];
      const property = resolveSchema(propertySchema);
      if (section === 'system_columns' && (property.type === 'object' || property.properties)) {
        renderObjectFields(property, value?.[propertyName] || {}, propertyPath, body);
      } else {
        body.append(renderNode(propertySchema, value?.[propertyName], propertyPath, propertyName));
      }
    }
    details.append(summary, body);
    target.append(details);
  }
}

function renderSystemColumnsEditor(parser, path) {
  const kinds = [
    ['topic', 'Topic', '_system_topic'],
    ['partition', 'Partition', '_system_partition'],
    ['offset', 'Offset', '_system_offset'],
    ['message_index', 'Message index', '_system_message_index'],
    ['write_timestamp_ms', 'Write timestamp', '_system_write_timestamp_ms']
  ];
  parser.common.system_columns ||= {};
  parser.json_parser.system_column_names ||= {};
  const enabled = parser.common.system_columns;
  const names = parser.json_parser.system_column_names;
  const details = element('details', 'advanced-settings system-columns-editor');
  const summary = element('summary', '', 'System columns');
  const body = element('div', 'system-columns-body');
  const header = element('div', 'system-column-row system-column-header');
  ['', 'Column', 'Output name'].forEach(text => header.append(element('span', '', text)));
  body.append(header);

  for (const [kind, label, defaultName] of kinds) {
    const row = element('div', 'system-column-row');
    const include = document.createElement('input');
    include.type = 'checkbox';
    include.checked = Boolean(enabled[kind]);
    include.setAttribute('aria-label', `Include ${label} system column`);
    include.onchange = () => {
      enabled[kind] = include.checked;
      scheduleDiscovery();
      renderEditor();
    };
    const check = element('label', 'table-check');
    check.append(include);
    row.append(check, element('span', 'system-column-name', label));
    const outputName = compactInput(names[kind] || '', `${label} output name`, next => {
      if (next) names[kind] = next; else delete names[kind];
      scheduleDiscovery();
    });
    outputName.placeholder = defaultName;
    outputName.disabled = !include.checked;
    row.append(outputName);
    body.append(row);
  }
  details.append(summary, body);
  return details;
}

function compactInput(value, ariaLabel, onInput) {
  const input = document.createElement('input');
  input.value = value ?? '';
  input.setAttribute('aria-label', ariaLabel);
  input.oninput = () => onInput(input.value);
  return input;
}

function renderColumnMappings(schema, value, path) {
  const resolved = resolveSchema(schema);
  const items = Array.isArray(value) ? value : [];
  const shell = element('section', 'column-editor');
  const heading = element('div', 'column-editor-heading');
  const title = element('div');
  title.append(element('strong', '', resolved.title || 'Data schema'));
  if (resolved.description) {
    const help = element('button', 'help', '?');
    help.type = 'button';
    help.dataset.tooltip = resolved.description;
    help.setAttribute('aria-label', resolved.description);
    title.append(help);
  }
  const add = element('button', 'icon-button', '＋ Field');
  add.type = 'button';
  heading.append(title, add);
  shell.append(heading);

  const parentPath = path.slice(0, -1);
  const primaryKeyPath = [...parentPath, 'primary_key'];
  const primaryKey = Array.isArray(atPath(primaryKeyPath)) ? atPath(primaryKeyPath) : [];
  add.onclick = () => {
    items.push(createValue(resolved.items));
    updateAndDiscover(path, items);
    renderEditor();
  };

  if (!items.length) {
    shell.append(element('div', 'column-editor-empty', 'Add at least one output column'));
    return shell;
  }

  const table = element('div', 'column-grid');
  const headers = ['', 'Name', 'JSON type', 'Arrow type', 'Key', 'Not null', 'Path', ''];
  const header = element('div', 'column-grid-row column-grid-header');
  headers.forEach(text => header.append(element('span', '', text)));
  table.append(header);

  items.forEach((item, index) => {
    const itemPath = [...path, index];
    const row = element('div', 'column-grid-entry');
    const main = element('div', 'column-grid-row');
    main.append(element('span', 'column-number', String(index + 1)));

    main.append(compactInput(item.column_name, `Column ${index + 1} name`, next => {
      const previous = item.column_name;
      item.column_name = next;
      const keyIndex = primaryKey.indexOf(previous);
      if (keyIndex >= 0) primaryKey[keyIndex] = next;
      setPath(primaryKeyPath, primaryKey);
      scheduleDiscovery();
    }));

    const jsonType = createDropdown(
      `field-${itemPath.join('-')}-json-data-type`,
      item.json_data_type,
      'Not selected',
      ['string', 'integer', 'unsigned_integer', 'number', 'boolean'].map(option => ({value: option, label: humanize(option)})),
      next => {
        item.json_data_type = next;
        scheduleDiscovery();
        renderEditor();
      }
    );
    main.append(jsonType);
    main.append(compactInput(item.arrow_type, `Column ${index + 1} Arrow type`, next => {
      item.arrow_type = next;
      scheduleDiscovery();
    }));

    const key = document.createElement('input');
    key.type = 'checkbox';
    key.checked = primaryKey.includes(item.column_name);
    key.setAttribute('aria-label', `Include column ${item.column_name || index + 1} in the primary key`);
    key.onchange = () => {
      const next = primaryKey.filter(name => name !== item.column_name);
      if (key.checked) next.push(item.column_name);
      updateAndDiscover(primaryKeyPath, next);
    };
    const keyCell = element('label', 'table-check');
    keyCell.append(key);
    main.append(keyCell);

    const required = document.createElement('input');
    required.type = 'checkbox';
    required.checked = !item.nullable;
    required.setAttribute('aria-label', `Column ${item.column_name || index + 1} is not null`);
    required.onchange = () => {
      item.nullable = !required.checked;
      scheduleDiscovery();
    };
    const requiredCell = element('label', 'table-check');
    requiredCell.append(required);
    main.append(requiredCell);

    main.append(compactInput(item.jsonpath, `Column ${index + 1} JSONPath`, next => {
      item.jsonpath = next;
      scheduleDiscovery();
    }));

    const remove = element('button', 'column-remove', '×');
    remove.type = 'button';
    remove.title = `Remove column ${item.column_name || index + 1}`;
    remove.setAttribute('aria-label', remove.title);
    remove.onclick = () => {
      items.splice(index, 1);
      setPath(primaryKeyPath, primaryKey.filter(name => name !== item.column_name));
      updateAndDiscover(path, items);
      renderEditor();
    };
    main.append(remove);
    row.append(main);

    const itemSchema = resolveSchema(resolved.items);
    const advanced = ['time_conversion', 'low_cardinality', 'max_length']
      .filter(name => itemSchema.properties?.[name]);
    if (advanced.length) {
      const details = element('details', 'column-row-advanced');
      const summary = element('summary', '', 'Column settings');
      const body = element('div', 'column-row-advanced-body');
      for (const name of advanced) {
        body.append(renderNode(itemSchema.properties[name], item[name], [...itemPath, name], name));
      }
      details.append(summary, body);
      row.append(details);
    }
    table.append(row);
  });
  shell.append(table);
  return shell;
}

function renderArray(schema, value, path, name) {
  const resolved = resolveSchema(schema);
  const items = Array.isArray(value) ? value : [];
  const shell = element('div', 'array-field');
  const heading = element('div', 'array-heading');
  const copy = element('div');
  const labelRow = element('div', 'field-label');
  labelRow.append(element('label', '', resolved.title || humanize(name)));
  if (resolved.description) {
    const help = element('button', 'help', '?');
    help.type = 'button';
    help.dataset.tooltip = resolved.description;
    help.setAttribute('aria-label', resolved.description);
    labelRow.append(help);
  }
  copy.append(labelRow);
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
    const row = element('label', 'switch-row');
    row.append(input, visual, element('span', '', input.checked ? 'Enabled' : 'Disabled'));
    input.onchange = () => {
      row.lastChild.textContent = input.checked ? 'Enabled' : 'Disabled';
      updateAndDiscover(path, input.checked);
    };
    shell.append(row);
    return shell;
  }
  if (resolved.enum) {
    const dropdown = createDropdown(
      id,
      resolved.enum.includes(value) ? value : undefined,
      'Not selected',
      resolved.enum.map(optionValue => ({value: optionValue, label: humanize(optionValue)})),
      optionValue => {
        updateAndDiscover(path, optionValue);
        renderEditor();
        document.getElementById(id)?.focus();
      }
    );
    shell.append(dropdown);
    labelDropdown(shell, id);
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
  if (resolved['x-ui']?.widget === 'column_mappings') return renderColumnMappings(resolved, value, path);
  if (nullableSchema(resolved)) return renderNullable(resolved, value, path, name);
  if (resolved.oneOf || resolved.anyOf) return renderUnion(resolved, value, path, name, options);
  if (resolved.type === 'array') return renderArray(resolved, value, path, name);
  if (resolved.type === 'object' || resolved.properties) {
    const group = element('fieldset', options.arrayItem ? 'object-group array-object' : 'object-group');
    if (!options.arrayItem) group.append(element('legend', '', resolved.title || humanize(name)));
    if (resolved.description) {
      const help = element('button', 'help group-help', '?');
      help.type = 'button';
      help.dataset.tooltip = resolved.description;
      help.setAttribute('aria-label', resolved.description);
      group.append(help);
    }
    renderObjectFields(resolved, value || {}, path, group);
    return group;
  }
  return renderScalar(resolved, value, path, name);
}

function renderEditor() {
  if (!definition || !formData) return;
  closeActiveDropdown(false);
  Object.values(containers).forEach(container => container.replaceChildren());
  const root = resolveSchema(definition.schema);
  const properties = root.properties;
  containers.deliveryType.append(renderNode(properties.delivery_type, formData.delivery_type, ['delivery_type'], 'delivery_type'));
  containers.sourcePicker.append(renderNode(properties.source, formData.source, ['source'], 'Source type', {pickerOnly: true}));
  containers.sinkPicker.append(renderNode(properties.sink, formData.sink, ['sink'], 'Destination type', {pickerOnly: true}));
  const compatibilityIssue = deliveryCompatibilityIssue();
  const callout = $('#compatibility-error');
  const providerPanels = document.querySelectorAll('.provider-panel');
  callout.textContent = compatibilityIssue || '';
  callout.classList.toggle('hidden', !compatibilityIssue);
  providerPanels.forEach(panel => panel.classList.toggle('hidden', Boolean(compatibilityIssue)));
  if (!compatibilityIssue) {
    containers.source.append(renderNode(properties.source, formData.source, ['source'], 'Source type', {bodyOnly: true}));
    containers.sink.append(renderNode(properties.sink, formData.sink, ['sink'], 'Destination type', {bodyOnly: true}));
  }
  for (const name of ['pipeline_memory_limit_bytes', 'keep_system_columns_in_sink', 'metrics', 'middlewares']) {
    containers.pipeline.append(renderNode(properties[name], formData[name], [name], name));
  }
  const source = Object.keys(formData.source || {})[0] || '—';
  const sink = Object.keys(formData.sink || {})[0] || '—';
  const parserSelected = Object.keys(formData.source?.[source]?.parser || {}).length > 0;
  $('.provider-grid').classList.toggle('parser-selected', parserSelected && !compatibilityIssue);
  $('#source-title').textContent = humanize(source);
  $('#sink-title').textContent = humanize(sink);
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

function renderDiscoveryLoading() {
  $('#error').textContent = '';
  $('#schema').innerHTML = '<div class="discovery-loading"><span class="spinner" aria-hidden="true"></span><strong>Checking connection</strong><p>Discovery runs in the background while the form remains editable.</p></div>';
}

function scheduleYaml() {
  clearTimeout(yamlTimer);
  yamlTimer = setTimeout(async () => {
    try {
      const result = await api('/api/config/yaml', {
        method: 'POST',
        body: JSON.stringify({config: formData})
      });
      latestYaml = result.yaml;
      $('#config-yaml').textContent = latestYaml;
      $('#copy-yaml').disabled = false;
    } catch (error) {
      latestYaml = '';
      $('#config-yaml').textContent = `Unable to render YAML: ${error.message}`;
      $('#copy-yaml').disabled = true;
    }
  }, 80);
}

function scheduleDiscovery() {
  scheduleYaml();
  clearTimeout(timer);
  discoveryController?.abort();
  discoveryController = undefined;
  const sequence = ++discoverySequence;
  lastDiscoveryValid = false;
  $('#save').disabled = true;
  const compatibilityIssue = deliveryCompatibilityIssue();
  if (compatibilityIssue) {
    $('#error').textContent = '';
    $('#schema').innerHTML = `<div class="empty-state endpoint-empty"><span>!</span><p>${escapeHtml(compatibilityIssue)}</p></div>`;
    setValidation('invalid', 'Choose a compatible delivery');
    return;
  }
  const hasSource = Object.keys(formData?.source || {}).length === 1;
  const hasSink = Object.keys(formData?.sink || {}).length === 1;
  if (!hasSource || !hasSink) {
    $('#error').textContent = '';
    $('#schema').innerHTML = '<div class="empty-state endpoint-empty"><span>↘</span><p>Select a source and destination to run discovery.</p></div>';
    setValidation('idle', 'Select source and destination');
    return;
  }
  setValidation('checking', 'Checking contract');
  renderDiscoveryLoading();
  timer = setTimeout(async () => {
    if (sequence !== discoverySequence) return;
    const controller = new AbortController();
    discoveryController = controller;
    try {
      const result = await api('/api/discover', {
        method: 'POST',
        body: JSON.stringify({config: formData}),
        signal: controller.signal
      });
      if (sequence !== discoverySequence) return;
      $('#error').textContent = '';
      renderSchema(result);
      lastDiscoveryValid = true;
      updateSaveState();
      setValidation('valid', 'Contract valid');
    } catch (error) {
      if (controller.signal.aborted || sequence !== discoverySequence) return;
      $('#schema').innerHTML = '<div class="empty-state"><span>!</span><p>Discovery will resume when the configuration is valid and the source is reachable.</p></div>';
      $('#error').textContent = error.message;
      setValidation('invalid', 'Needs attention');
    } finally {
      if (discoveryController === controller) discoveryController = undefined;
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
  formData.delivery_id = newDeliveryId();
  $('#name').value = '';
  $('#deliveries-view').classList.add('hidden');
  $('#editor').classList.remove('hidden');
  renderEditor();
  scheduleDiscovery();
};

$('#name').oninput = updateSaveState;

$('#copy-yaml').onclick = async () => {
  if (!latestYaml) return;

  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(latestYaml);
  } else {
    const textarea = document.createElement('textarea');
    textarea.value = latestYaml;
    textarea.setAttribute('readonly', '');
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.append(textarea);
    textarea.select();
    document.execCommand('copy');
    textarea.remove();
  }

  const button = $('#copy-yaml');
  button.textContent = 'Copied';
  setTimeout(() => { button.textContent = 'Copy'; }, 1200);
};

$('#cancel').onclick = () => {
  clearTimeout(timer);
  clearTimeout(yamlTimer);
  discoveryController?.abort();
  discoveryController = undefined;
  discoverySequence += 1;
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
