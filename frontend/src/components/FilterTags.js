// src/components/FilterTags.js
// A filter tag is a chip that represents one applied include/exclude
// filter. The colored left border carries the inc/exc meaning; the
// glyph (+ / minus) reinforces it for colour-blind readers. Tags now
// live inside their own section's container, so we no longer need a
// category data-attribute or a global "no active filters" hint.
//
// Clicking the chip body (anywhere except the remove button) toggles
// the filter between include and exclude. The remove button still
// removes the filter outright.
import { escHtml } from '../utils.js';

/**
 * @param {HTMLElement} containerEl   Where the tag should be appended.
 * @param {string}      name          Internal name (attribute or composite key).
 * @param {*}           value         Internal value (for callbacks / a11y).
 * @param {'include'|'exclude'} type
 * @param {Function}    onRemove      Zero-arg callback fired when the user clicks the remove button.
 * @param {Function}   [onToggle]     Zero-arg callback fired when the user clicks the chip body.
 *                                    If omitted, the chip is not toggle-able.
 * @param {string}     [displayLabel] Optional human-readable label override.
 * @param {string}     [displayValue] Optional human-readable value override.
 */
export function addFilterTag(containerEl, name, value, type, onRemove, onToggle, displayLabel, displayValue) {
    const tag = document.createElement('span');
    tag.className = `filter-tag ${type === 'include' ? 'include-tag' : 'exclude-tag'}`;

    const labelText = displayLabel !== undefined ? displayLabel : name;
    const valueText = displayValue !== undefined ? displayValue : value;
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
    // mousedown so the chip's click handler doesn't also fire on the same gesture.
    removeBtn.addEventListener('mousedown', (e) => e.stopPropagation());
    removeBtn.onclick = (e) => {
        e.stopPropagation();
        onRemove();
    };
    tag.appendChild(removeBtn);

    if (typeof onToggle === 'function') {
        tag.classList.add('toggleable');
        tag.title = type === 'include'
            ? 'Click para cambiar a exclude'
            : 'Click para cambiar a include';
        tag.addEventListener('click', (e) => {
            // Defensive: ignore clicks on the remove button (already handled).
            if (e.target.closest('.tag-remove')) return;
            onToggle();
        });
    }

    containerEl.appendChild(tag);
}

export function clearTags(containerEl) {
    containerEl.innerHTML = '';
}
