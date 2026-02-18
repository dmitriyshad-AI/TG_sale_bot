FROM node:20-alpine AS webapp-builder

WORKDIR /webapp

COPY webapp/package.json webapp/package-lock.json ./
RUN npm ci
COPY webapp/ ./
RUN npm run build


FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=webapp-builder /webapp/dist /app/webapp/dist
RUN chmod +x /app/start.sh

CMD ["/app/start.sh"]
