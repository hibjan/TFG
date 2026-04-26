package io.github.hibjan.tfg.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseService {

    private static final String URL = "jdbc:postgresql://"
            + System.getenv().getOrDefault("DB_HOST", "localhost")
            + ":"
            + System.getenv().getOrDefault("DB_PORT", "5432")
            + "/"
            + System.getenv().getOrDefault("DB_NAME", "myapp");
    private static final String USER = System.getenv().getOrDefault("DB_USER", "myuser");
    private static final String PASS = System.getenv().getOrDefault("DB_PASS", "mypass");

    static {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL driver not found", e);
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASS);
    }
}
