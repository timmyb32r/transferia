FROM airbyte/destination-postgres:3.0.18 AS destination
FROM airbyte/source-postgres:3.8.5
USER root
COPY --from=destination /airbyte /target
