package io.github.hibjan.tfg.controller;

import io.github.hibjan.tfg.dao.UserDAO;
import io.github.hibjan.tfg.model.User;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

@WebServlet("/api/users")
public class UserServlet extends HttpServlet {

    private final UserDAO userDAO = new UserDAO();
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        List<User> users = new ArrayList<>();
        try {
            users = userDAO.getUsers();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        mapper.writeValue(resp.getWriter(), users);
    }
}
