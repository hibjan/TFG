// src/components/FilterTags.js
import { escHtml } from '../utils.js';

export function addFilterTag(containerEl, name, value, type, category, onRemove, displayLabel, displayValue) {
    const tag = document.createElement('span');
    tag.className = `filter-tag ${type === 'include' ? 'include-tag' : 'exclude-tag'}`;
    tag.dataset.category = category;

    const labelText = displayLabel !== undefined ? displayLabel : name;
    const valueText = displayValue !== undefined ? displayValue : value;

    tag.innerHTML = `
        <span class="tag-type">${type === 'include' ? 'INC' : 'EXC'}</span>
        <span class="tag-label">${escHtml(String(labelText))}:</span>
        <span class="tag-value">${escHtml(String(valueText))}</span>
    `;

    const removeBtn = document.createElement('button');
    removeBtn.className = 'tag-remove';
    removeBtn.textContent = '×';
    // Pass the remove action back to main.js via callback
    removeBtn.onclick = () => onRemove(name, value, type, category); 
    tag.appendChild(removeBtn);

    containerEl.appendChild(tag);
}

export function clearFilterTagsByCategory(containerEl, category) {
    Array.from(containerEl.querySelectorAll(`.filter-tag[data-category="${category}"]`))
        .forEach(el => el.remove());
    
    const hint = containerEl.querySelector('.empty-hint');
    if (hint) hint.remove();
}

export function refreshEmptyHint(containerEl) {
    if (containerEl.children.length === 0) {
        containerEl.innerHTML = '<span class="empty-hint">No active filters</span>';
    }
}