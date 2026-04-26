# Stage 1: Build the Vite frontend
FROM node:22-alpine AS build
WORKDIR /app

# Install dependencies first for better caching
COPY package.json package-lock.json* ./
RUN npm install

# Copy source code
COPY . .

# Force VITE_API_BASE_URL to be empty so the frontend uses relative paths.
# This allows the Nginx proxy to handle the requests regardless of the domain (e.g. Cloudflare Quick Tunnels)
ENV VITE_API_BASE_URL=""
RUN npm run build

# Stage 2: Serve with Nginx
FROM nginx:alpine

# Copy the custom Nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy the built Vite output to Nginx's HTML folder
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
