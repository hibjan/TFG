# TFG

[Memoria del TFG](./docs/TFGTeXiS.pdf)

---

Juan Andrés Hibjan Cardona

Leonardo Prado de Souza

---

## Production set-up

## Development set-up

### Prerequisites

- Java JDK 17+ (java -version)
- Maven (mvn -v)
- Python 3.11 (python --version)
- Docker & Docker Compose (https://www.docker.com/products/docker-desktop/ -> docker --version; docker compose version )
- Apache Tomcat 10 (https://tomcat.apache.org/download-10.cgi - Only core)
- Eclipse IDE for Enterprise Java and Web Developers
- VS Code

### 1. Clone the repository

In VS Code Terminal:

1. git clone https://github.com/hibjan/TFG.git
2. cd TFG

### 2. Database

Duplicate the ".env.example" file, name it ".env", and fill it with the desired credentials.

Open Docker Desktop

In VS Code Terminal:

1. docker compose up -d

This will create the database container, initialize it with the files in the database folder and it will be running.

In the Containers tab in Docker Desktop you can manually stop or start it.

> In case anything goes wrong, to wipe DB:
>
> 1. docker-compose down -v
> 2. docker-compose up -d

Open scripts/populate_db.py

1. Make sure JSON_FILE and DATASET_NAME are set properly
2. Run the python script

This will take the contents of the json file and insert them into the

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

**Right-click on the project -> Run on Server** will get the backend live

> In case it doesn't work try:
>
> 1. $TOMCAT_HOME/bin/shutdown.sh
> 2. $TOMCAT_HOME/bin/startup.sh

## 4. Frontend

1. Install extension Live Server https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer
2. Go to the extension settings and look for Live Server > Settings: Host (vscode://settings/liveServer.settings.host) and set it to localhost instead of 127.0.0.0
3. Open frontend/index.html
4. Click button "Go Live" on the bottom-dright
