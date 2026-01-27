package io.github.hibjan.tfg.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.github.hibjan.tfg.model.User;

public class UserDAO {

    public List<User> getUsers() throws SQLException {
        List<User> users = new ArrayList<>();
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC Driver JAR is missing from the classpath!", e);
        }
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/myapp", "myuser", "mypass");
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM users")) {
            while (rs.next()) {
                users.add(new User(rs.getString("username"), rs.getString("email"), rs.getInt("age"),
                        rs.getBoolean("is_admin")));
            }
        }
        return users;
    }
}
