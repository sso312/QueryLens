FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends libnsl2 fonts-noto-cjk \
  && (apt-get install -y --no-install-recommends libaio1 \
    || apt-get install -y --no-install-recommends libaio1t64) \
  && if [ ! -e /lib/x86_64-linux-gnu/libaio.so.1 ]; then \
       if [ -e /lib/x86_64-linux-gnu/libaio.so.1t64 ]; then \
         ln -s /lib/x86_64-linux-gnu/libaio.so.1t64 /lib/x86_64-linux-gnu/libaio.so.1; \
       elif [ -e /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]; then \
         ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /lib/x86_64-linux-gnu/libaio.so.1; \
       fi; \
     fi \
  && rm -rf /var/lib/apt/lists/*

COPY backend/query-visualization/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY backend/query-visualization /app

ENV PYTHONPATH=/app
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

EXPOSE 8080

CMD ["uvicorn", "src.api.server:app", "--host", "0.0.0.0", "--port", "8080"]
