// src/utils.js

export const $$ = (id) => document.getElementById(id);

export function escHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}