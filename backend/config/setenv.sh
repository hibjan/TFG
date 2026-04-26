#!/bin/sh

# $CATALINA_BASE points directly to your apache-tomcat folder.
# We go up two levels: ../ (backend) -> ../ (TFG root) to find the file.
ENV_FILE="$CATALINA_BASE/../../.env.db"

if [ -f "$ENV_FILE" ]; then
    echo "SUCCESS: Loading DB variables from $ENV_FILE"
    
    # grep '^DB_' ensures we ONLY load database credentials and ignore other keys
    # xargs formats them correctly for the export command
    export $(grep '^DB_' "$ENV_FILE" | xargs)
else
    echo "ERROR: .env.db file not found at $ENV_FILE"
fi