# Modifier le Dockerfile

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