export default class PaginationHandler {
    /**
     * @param {HTMLElement} container - The DOM element where pagination will render
     * @param {Function} onPageChange - Callback function when a new page is clicked
     */
    constructor(container, onPageChange) {
        this.container = container;
        this.onPageChange = onPageChange;
    }

    render(currentPage, totalPages) {
        // Clear current pagination
        this.container.innerHTML = '';

        if (totalPages <= 1) return; // Hide if only 1 page

        const createButton = (text, pageNum, isActive = false, isDisabled = false) => {
            const btn = document.createElement('button');
            // Reusing your exact CSS classes!
            btn.className = `pg-btn ${typeof text === 'number' ? 'pg-num-btn' : ''} ${isActive ? 'active' : ''}`;
            btn.textContent = text;
            btn.disabled = isDisabled;
            
            if (!isActive && !isDisabled) {
                btn.addEventListener('click', () => this.onPageChange(pageNum));
            }
            return btn;
        };

        // 1. Previous Button
        this.container.appendChild(createButton('Prev', currentPage - 1, false, currentPage === 1));

        // 2. Numbered Buttons Wrapper
        const numbersWrapper = document.createElement('div');
        numbersWrapper.className = 'pg-numbers';

        // Logic to show numbers and ellipsis (Max 5 visible numbers for cleanliness)
        for (let i = 1; i <= totalPages; i++) {
            if (
                i === 1 || 
                i === totalPages || 
                (i >= currentPage - 1 && i <= currentPage + 1)
            ) {
                numbersWrapper.appendChild(createButton(i, i, i === currentPage));
            } else if (
                (i === currentPage - 2 && i > 1) || 
                (i === currentPage + 2 && i < totalPages)
            ) {
                const ellipsis = document.createElement('span');
                ellipsis.className = 'pg-ellipsis';
                ellipsis.textContent = '...';
                numbersWrapper.appendChild(ellipsis);
            }
        }
        
        this.container.appendChild(numbersWrapper);

        // 3. Next Button
        this.container.appendChild(createButton('Next', currentPage + 1, false, currentPage === totalPages));
    }
}