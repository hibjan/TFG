// src/components/pagination.js
export function updatePagination(total, page, pageSize, prevBtn, nextBtn, infoEl) {
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    infoEl.textContent = `Page ${page + 1} of ${totalPages}`;
    prevBtn.disabled = page === 0;
    nextBtn.disabled = page >= totalPages - 1;
}