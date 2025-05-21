# Modifier le Dockerfile
docker exec -it airflow-webserver cat > ./dbt/Dockerfile << 'EOL'
FROM python:3.9-slim

WORKDIR /usr/app/dbt

# Install dbt-core, PostgreSQL adapter, and pytz
RUN pip install --upgrade pip && \
    pip install dbt-postgres==1.3.0 pytz

# Copy dbt profile
COPY profiles.yml /root/.dbt/profiles.yml

# Copy dbt project files
COPY . .

# Set entry point
ENTRYPOINT ["dbt"]
CMD ["run"]
EOL

# Reconstruire l'image dbt
docker-compose build dbt

# Redémarrer le conteneur dbt
docker-compose up -d dbt