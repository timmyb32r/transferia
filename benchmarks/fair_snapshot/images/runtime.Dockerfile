FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates python3 python3-venv libssl3t64 libxml2 libzstd1 libsasl2-2 libsnappy1v5 curl time && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /opt/meltano && /opt/meltano/bin/pip install --no-cache-dir meltano==4.2.2 meltanolabs-tap-postgres==0.10.0 meltanolabs-target-postgres==0.8.0 && /opt/meltano/bin/pip freeze > /opt/meltano/versions.txt
