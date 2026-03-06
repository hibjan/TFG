package io.github.hibjan.tfg.controller;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import jakarta.servlet.annotation.*;
import java.io.*;
import java.nio.file.*;

import java.util.Properties;

/**
 * GET /api/resource/* - Serve self-hosted files from an external directory.
 *
 * The base directory is resolved automatically from the project root
 * (injected by Maven via app.properties). It can also be overridden
 * with the UPLOADS_DIR environment variable.
 *
 * Example: GET /api/resource/people/1/photo.jpg
 * -> streams {project.root}/uploads/people/1/photo.jpg
 */
@WebServlet("/api/resource/*")
public class ResourceServlet extends HttpServlet {

    private Path baseDir;

    @Override
    public void init() throws ServletException {
        // 1. Try environment variable (explicit override)
        String uploadsDir = System.getenv("UPLOADS_DIR");
        if (uploadsDir != null && !uploadsDir.isBlank()) {
            baseDir = Paths.get(uploadsDir).toAbsolutePath().normalize();
        } else {
            // 2. Read project root from Maven-filtered app.properties
            try (InputStream is = getClass().getClassLoader()
                    .getResourceAsStream("app.properties")) {
                if (is != null) {
                    Properties props = new Properties();
                    props.load(is);
                    String projectRoot = props.getProperty("project.root");
                    if (projectRoot != null && !projectRoot.isBlank()) {
                        baseDir = Paths.get(projectRoot, "uploads")
                                .toAbsolutePath().normalize();
                    }
                }
            } catch (IOException e) {
                System.err.println("[ResourceServlet] Failed to read app.properties: " + e.getMessage());
            }
            // 3. Fallback
            if (baseDir == null) {
                baseDir = Paths.get("./uploads").toAbsolutePath().normalize();
            }
        }
        System.out.println("[ResourceServlet] Serving files from: " + baseDir);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        String pathInfo = req.getPathInfo();

        // No path provided
        if (pathInfo == null || pathInfo.equals("/")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "No file path specified");
            return;
        }

        // Reject path traversal attempts
        if (pathInfo.contains("..")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
            return;
        }

        // Resolve and normalize the requested file path
        Path filePath = baseDir.resolve(pathInfo.substring(1)).normalize();

        // Verify the resolved path is still under the base directory
        if (!filePath.startsWith(baseDir)) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid path");
            return;
        }

        File file = filePath.toFile();

        // File not found
        if (!file.exists() || !file.isFile()) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "File not found");
            return;
        }

        // Detect MIME type
        String mimeType = getServletContext().getMimeType(file.getName());
        if (mimeType == null) {
            mimeType = "application/octet-stream";
        }

        // Set response headers
        resp.setContentType(mimeType);
        resp.setContentLengthLong(file.length());
        resp.setHeader("Cache-Control", "public, max-age=86400"); // 24h cache

        // Stream file to response
        try (InputStream in = new FileInputStream(file);
                OutputStream out = resp.getOutputStream()) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }
}
