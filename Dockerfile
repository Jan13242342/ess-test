FROM bitnami/postgresql:16.3.0-debian-12-r0
USER root
RUN install_packages postgresql-cron
USER 1001