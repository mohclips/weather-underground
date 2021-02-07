FROM python:slim

# RUN apt-get update -y && \
#     DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
#     cron vim-tiny s-nail time wget \
#     && apt -y --auto-remove purge $build_deps \
#     && rm -rf /var/lib/apt/lists/* \
    
RUN pip install pymysql paho-mqtt requests

CMD tail -f /dev/null
