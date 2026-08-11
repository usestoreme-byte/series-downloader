FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    aria2 mediainfo ffmpeg mkvtoolnix tesseract-ocr tesseract-ocr-eng curl && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/tessdata_best && \
    curl -L --max-time 120 -o /root/tessdata_best/eng.traineddata \
      https://cdn.jsdelivr.net/gh/tesseract-ocr/tessdata_best@main/eng.traineddata
ENV TESSDATA_PREFIX=/root/tessdata_best

RUN pip install --no-cache-dir --prefer-binary \
    gspread google-auth pymediainfo requests requests-toolbelt pgsrip

WORKDIR /app
