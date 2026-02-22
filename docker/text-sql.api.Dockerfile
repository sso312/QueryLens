FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends libnsl2 \
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

COPY backend/text-to-sql/backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend/text-to-sql/backend /app/backend
COPY backend/text-to-sql/var /app/var
COPY backend/text-to-sql/docs/query_visualization_eval_aside.jsonl /app/docs/query_visualization_eval_aside.jsonl

ENV PYTHONPATH=/app/backend
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
