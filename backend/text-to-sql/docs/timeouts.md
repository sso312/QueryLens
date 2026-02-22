# Timeout Synchronization Guide

## DB timeout
- `DB_TIMEOUT_SEC`: default execution timeout (seconds)
- `DB_TIMEOUT_SEC_ACCURACY`: accuracy-mode timeout (seconds, default 180)
- Oracle `conn.call_timeout` is applied per connection in milliseconds.

## API timeout
- `API_REQUEST_TIMEOUT_SEC` should be >= DB timeout + safety margin.
- Current middleware enforces minimum 190s.

## Proxy timeout
- If nginx/gunicorn is used, set read/request timeout >= API timeout.
