FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY duplex_voice ./duplex_voice
COPY scripts ./scripts
RUN pip install --no-cache-dir '.[vad,rtc,cluster]' && useradd --create-home voice && mkdir -p /app/data && chown voice:voice /app/data
USER voice
EXPOSE 8000
CMD ["uvicorn","duplex_voice.main:app","--host","0.0.0.0","--port","8000","--workers","1","--ws-max-size","65536","--ws-max-queue","16","--no-access-log"]
