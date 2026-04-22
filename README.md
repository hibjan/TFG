# TFG

[Bachelor's Degree Thesis (Memoria del TFG)](./docs/TFGTeXiS.pdf)

## Summary

This application is designed for the exploration of large-scale multimedia collections. By leveraging rich metadata and relational links between objects, it allows users to navigate complex datasets intuitively.

The mains components are: backend (Java/Tomcat), frontend (Vanilla JS) and the database (PostgreSQL). There is also an integration layer for public datasets available online, currently supporting TMDb and DBLP.

Below you'll find some recommendations on how to work in this project ([Dev steps](#development-set-up)), and on how to deploy it ([Prod steps](#production-set-up)).

## Team

- Juan Andrés Hibjan Cardona

- Leonardo Prado de Souza

## Development set-up

### Prerequisites

- Java JDK 17+ (java -version)
- Maven (mvn -v)
- Python 3.11 (python --version)
- Docker & Docker Compose (https://www.docker.com/products/docker-desktop/ -> docker --version; docker compose version )
- Apache Tomcat 10 (https://tomcat.apache.org/download-10.cgi - Only core)
- Eclipse IDE for Enterprise Java and Web Developers
- VS Code
- Node.js (node -v)

### 1. Clone the repository

In VS Code Terminal:

1. git clone https://github.com/hibjan/TFG.git
2. cd TFG

### 2. Database

Duplicate the ".env.db.example" file, name it ".env.db", and fill it with the desired credentials.

Open Docker Desktop

In VS Code Terminal:

1. docker compose up -d

This will create the database container, initialize it with the files in the database folder and it will be running.

In the Containers tab in Docker Desktop you can manually stop or start it.

> In case anything goes wrong, to wipe DB:
>
> 1. docker-compose down -v
> 2. docker-compose up -d

Now, to actually populate the database with some dataset, run the following:

1. cd scripts
2. pip install -r requirements.txt
3. python populate_db.py (Make sure JSON_FILE and DATASET_NAME are set properly)

This will take the contents of the json file and insert them into the database

### 3. Backend

Go to $TOMCAT_HOME/conf/context.xml, and make sure to include the cookie processor for handling sessions:

```
<Context>
  ...

  <CookieProcessor
      className="org.apache.tomcat.util.http.Rfc6265CookieProcessor"
      sameSiteCookies="none" />

  ...
</Context>
```

In VS Code Terminal:

1. cd backend
2. mvn clean package

This compiles the project, resolves dependencies and builds the WAR file.

Now, go to Eclipse, and make sure to have your workspace in a different location from where the repository is located, and create one specifically for this project

From now on, in Eclipse:

#### 3.1. Import the project

1. File -> Import
2. Maven -> Existing Maven Projects
3. Select TFG/backend

#### 3.2. Add Tomcat

1. Window -> Show View -> Servers
2. Create new server
3. Apache -> Tomcat v10.1 Server
4. Select the directory where Tomcat is installed

#### 3.3. Link project to Tomcat

1. Servers tab
2. Right-click Tomcat -> Add and Remove
3. Select backend

#### 3.4. Set-up DB credentials

1. Run -> Run Configurations...
2. Environment -> Add...
3. Create a new environment variable for each in the .env.db file

**Right-click on the project -> Run on Server** will get the backend live

> In case it doesn't work try:
>
> 1. $TOMCAT_HOME/bin/shutdown.sh
> 2. $TOMCAT_HOME/bin/startup.sh

### 4. Frontend

Duplicate the ".env.example" file, name it ".env.development.local", and fill it with the desired credentials.

In VS Code terminal:

1. cd frontend
2. npm install
3. npm run dev

## Production set-up

> To do:
>
> 1. copy setenv to bin
> 2. copy backend.war to webapps
> 3. Docker container
> 4. npm run build
> 5. npm run dist
> 6. cloudfare tunnel
