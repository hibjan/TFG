# Stage 1: Build the application using Maven
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Copy pom.xml and source code
COPY pom.xml .
COPY src ./src

# Build the WAR file, skipping tests to speed up the process
RUN mvn clean package -DskipTests

# Stage 2: Run the application using Tomcat 10.1
FROM tomcat:10.1-jdk17

# Remove the default ROOT application to prevent conflicts (optional)
RUN rm -rf /usr/local/tomcat/webapps/ROOT

# Add CookieProcessor to context.xml for sameSite handling (from README.md)
RUN sed -i 's/<\/Context>/    <CookieProcessor className="org.apache.tomcat.util.http.Rfc6265CookieProcessor" sameSiteCookies="none" \/>\n<\/Context>/' /usr/local/tomcat/conf/context.xml

# Copy the built WAR file to Tomcat's webapps directory
COPY --from=build /app/target/backend.war /usr/local/tomcat/webapps/backend.war

EXPOSE 8080
CMD ["catalina.sh", "run"]
