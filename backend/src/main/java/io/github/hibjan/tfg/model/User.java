package io.github.hibjan.tfg.model;

public class User {
    private String username;
    private String email;
    private int age;
    private boolean isAdmin;

    public User(String username, String email, int age, boolean isAdmin) {
        this.username = username;
        this.email = email;
        this.age = age;
        this.isAdmin = isAdmin;
    }
    
    // --- REQUIRED FOR JSTL ---
    public String getUsername() { return username; }
    public String getEmail() { return email; }
    public int getAge() { return age; }
    public boolean isAdmin() { return isAdmin; } // Boolean getters often use "is" instead of "get"
}