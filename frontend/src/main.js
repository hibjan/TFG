// Selecting the elements
const title = document.getElementById('main-title');
const button = document.getElementById('colorButton');

// Adding a click event listener
button.addEventListener('click', () => {
    // Check current color and toggle it
    if (title.style.color === 'red') {
        title.style.color = '#333';
    } else {
        title.style.color = 'red';
    }
    
    console.log("Button was clicked!");
});

fetch('http://localhost:8080/backend/api/users')
    .then(res => res.json())
    .then(users => {
        const table = document.getElementById('users');
        users.forEach(u => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${u.username}</td>
                <td>${u.email}</td>
                <td>${u.age}</td>
                <td>${u.admin}</td>
            `;
            table.appendChild(row);
        });
    });