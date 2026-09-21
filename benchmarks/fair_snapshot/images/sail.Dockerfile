FROM transferia/benchmark-runtime:20260921
COPY wheels /tmp/sail-wheels
RUN python3 -m venv /opt/sail && /opt/sail/bin/pip install --no-index --find-links=/tmp/sail-wheels 'pysail[jdbc]==0.7.1' pyspark-client==4.2.0 'pandas<3' 'psycopg[binary]' clickhouse-connect adbc-driver-postgresql==1.12.0 && /opt/sail/bin/pip freeze > /opt/sail/versions.txt && rm -rf /tmp/sail-wheels
ENV PATH=/opt/sail/bin:$PATH

COPY stunnel4.deb libwrap0.deb /tmp/
RUN dpkg -x /tmp/stunnel4.deb / && dpkg -x /tmp/libwrap0.deb / && rm /tmp/stunnel4.deb /tmp/libwrap0.deb && stunnel -version
