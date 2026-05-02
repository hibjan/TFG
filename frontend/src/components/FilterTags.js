// src/components/FilterTags.js
// A filter tag is a chip that represents one applied include/exclude
// filter. The colored left border carries the inc/exc meaning; the
// glyph (+ / minus) reinforces it for colour-blind readers. Tags now
// live inside their own section's container, so we no longer need a
// category data-attribute or a global "no active filters" hint.
import { escHtml } from '../utils.js';

/**
 * @param {HTMLElement} containerEl   Where the tag should be appended.
 * @param {string}      name          Internal name (attribute or composite key).
 * @param {*}           value         Internal value (for callbacks / a11y).
 * @param {'include'|'exclude'} type
 * @param {Function}    onRemove      Zero-arg callback fired when the remove button is clicked.
 * @param {string}     [displayLabel] Optional human-readable label override.
 * @param {string}     [displayValue] Optional human-readable value override.
 */
export function addFilterTag(containerEl, name, value, type, onRemove, displayLabel, displayValue) {
    const tag = document.createElement('span');
    tag.className = `filter-tag ${type === 'include' ? 'include-tag' : 'exclude-tag'}`;

    const labelText = displayLabel !== undefined ? displayLabel : name;
    const valueText = displayValue !== undefined ? displayValue : value;
    // Plus / minus glyphs: U+002B and U+2212. Use Unicode escape to keep the
    // source file pure ASCII and dodge any transport-encoding issues.
    const glyph = type === 'include' ? '+' : '−';

    tag.innerHTML = `
        <span class="tag-glyph" aria-hidden="true">${glyph}</span>
        <span class="tag-label">${escHtml(String(labelText))}</span>
        <span class="tag-sep" aria-hidden="true">&middot;</span>
        <span class="tag-value">${escHtml(String(valueText))}</span>
    `;

    const removeBtn = document.createElement('button');
    removeBtn.className = 'tag-remove';
    removeBtn.setAttribute('aria-label', `Remove ${type} filter ${labelText}: ${valueText}`);
    removeBtn.title = 'Remove this filter';
    removeBtn.innerHTML = '&times;';
    removeBtn.onclick = () => onRemove();
    tag.appendChild(removeBtn);

    containerEl.appendChild(tag);
}

export function clearTags(containerEl) {
    containerEl.innerHTML = '';
}
