FROM bitnami/postgresql:15.7.0-debian-12-r0
USER root
RUN install_packages postgresql-15-cron
USER 1001