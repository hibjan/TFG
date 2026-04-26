# Multimedia Collection Explorer (TFG)

[Bachelor's Degree Thesis (Memoria del TFG)](./docs/TFGTeXiS.pdf)

## Summary

This application is designed for the exploration of large-scale multimedia collections. By leveraging rich metadata and relational links between objects, it allows users to navigate complex datasets intuitively.

### Main Components

- **Backend:** Java / Tomcat
- **Frontend:** Vanilla JS / Vite
- **Database:** PostgreSQL
- **Integration Layer:** Support for public datasets available online (currently supporting TMDb and DBLP).

## Team

- Juan Andrés Hibjan Cardona
- Leonardo Prado de Souza

---

## Table of Contents

- [Development Set-up](#development-set-up)
- [Production Set-up](#production-set-up)

---

## Development Set-up

### Prerequisites

- **Java JDK 17+** (`java -version`)
- **Maven** (`mvn -v`)
- **Python 3.11** (`python --version`)
- **Docker & Docker Compose** ([Docker Desktop](https://www.docker.com/products/docker-desktop/))
- **Apache Tomcat 10** ([Download](https://tomcat.apache.org/download-10.cgi) - Only core)
- **Node.js** (`node -v`)
- **IDE:** Eclipse IDE for Enterprise Java and Web Developers, VS Code

### 1. Clone the repository

```bash
git clone https://github.com/hibjan/TFG.git
cd TFG
```

### 2. Database

1. Duplicate the `.env.db.example` file that is in the root directory, name it `.env.db`, and fill it with your desired credentials.
2. Open Docker Desktop.
3. Start the database container:

   ```bash
   docker compose --env-file .env.db -f docker-compose.dev.yml up -d
   ```

   This will create the database container, initialize it with the files in the database folder, and start it.

_Note: In the Containers tab in Docker Desktop, you can manually stop or start it._

> **Troubleshooting:**
> In case anything goes wrong, to wipe the DB:
>
> ```bash
> docker compose --env-file .env.db -f docker-compose.dev.yml down -v
> docker compose --env-file .env.db -f docker-compose.dev.yml up -d
> ```

To populate the database with a dataset:

```bash
cd scripts
pip install -r requirements.txt
python populate_db_jsonl.py  # or python populate_db.py
```

By default, `populate_db_jsonl.py` contains the `JSON_FILE` and `DATASET_NAME` parameters for the TMDb Top 500 sample dataset. For using different values, they can be passed as arguments to the script.

The `populate_db.py` script contains our test dataset by default, which is a small dataset based on the TMDb dataset.

### 3. Backend

1. Go to the file inside your Apache Tomcat instance `$TOMCAT_HOME/conf/context.xml`, and make sure to include the cookie processor for handling sessions:

   ```xml
   <Context>
     ...
     <CookieProcessor className="org.apache.tomcat.util.http.Rfc6265CookieProcessor" sameSiteCookies="none" />
     ...
   </Context>
   ```

2. Compile the project, resolve dependencies, and build the WAR file:

   ```bash
   cd backend
   mvn clean package
   ```

3. Open **Eclipse**. Make sure to have your workspace in a different location from where the repository is located, and create one specifically for this project.

#### 3.1. Import the project

1. `File` -> `Import`
2. `Maven` -> `Existing Maven Projects`
3. Select the `TFG/backend` folder.

#### 3.2. Add Tomcat

1. `Window` -> `Show View` -> `Servers`
2. Click to create a new server.
3. `Apache` -> `Tomcat v10.1 Server`
4. Select the directory where Tomcat is installed.

#### 3.3. Link project to Tomcat

1. Open the `Servers` tab.
2. Right-click Tomcat -> `Add and Remove...`
3. Select the `backend` project and add it.

#### 3.4. Set-up DB credentials

1. `Run` -> `Run Configurations...`
2. `Environment` -> `Add...`
3. Create a new environment variable for each entry in your `.env.db` file.

To run the backend, **Right-click on the project -> Run on Server**.

> **Troubleshooting 1:**
> In case you see an error related to missing classes (e.g. ObjectMapper), make sure that Maven Dependencies are included in the Web Deployment Assembly:
>
> 1. Right-click on the project -> `Properties` -> `Deployment Assembly`
> 2. Maven Dependencies should be under the Source column
> 3. If not there, `Add` -> `Java Build Path Entries`
> 4. Select `Maven dependencies` -> `Finish`
> 5. `Apply and Close`

> **Troubleshooting 2:**
> In case it doesn't work, try restarting Eclipse manually, and restarting Tomcat by running the following scripts:
>
> ```bash
> $TOMCAT_HOME/bin/shutdown.sh
> $TOMCAT_HOME/bin/startup.sh
> ```

### 4. Frontend

1. Duplicate the `.env.example` file that is inside the frontend directory, and name it `.env.development.local`. It contains the default backend endpoint (`VITE_API_BASE_URL`), which you only need to modify if your backend runs on a different port.
2. Install dependencies and run the development server:
   ```bash
   cd frontend
   npm install
   npm run dev
   ```

---

## Production Set-up

The entire stack (PostgreSQL database, Tomcat Java backend, Nginx Vite frontend, and Cloudflare secure tunnel) is fully containerized for a one-click deployment.

### 1. Configure Environment Variables

Ensure your `.env.db` file is correctly filled out with your desired database credentials (you can duplicate `.env.db.example` if you haven't already).

### 2. Deploy the Stack

Spin up the entire production environment in the background by running:

```bash
docker compose --env-file .env.db -f docker-compose.prod.yml up -d --build
```

### 3. Get your Public URL

The `cloudflared` container automatically establishes a secure tunnel and generates a random public URL. This means your app is securely exposed to the internet without opening any ports!

To easily extract your public URL from the logs, use the following command:

**On Windows (PowerShell):**

```powershell
docker logs tfg-cloudflared 2>&1 | Select-String "https://.*\.trycloudflare\.com"
```

**On Mac / Linux / Git Bash:**

```bash
docker logs tfg-cloudflared 2>&1 | grep -o 'https://.*\.trycloudflare\.com'
```

Simply click the resulting `https://...trycloudflare.com` link to access your live production application. All backend requests are automatically handled and proxied via Nginx.

### 4. Populate the Database

To populate the database with a dataset, you must run the Python script locally from your host machine. Make sure your `.env.db` file is configured with `DB_HOST=localhost`, as port 5432 is mapped and exposed by the Postgres container:

```bash
cd scripts
pip install -r requirements.txt
python populate_db_jsonl.py  # or python populate_db.py
```

This will take the contents of the json/jsonl file and insert them into the database.
