FROM bitnami/postgresql:16.3.0-debian-12-r0
USER root
RUN apt-get update && apt-get install -y postgresql-16-cron
USER 1001