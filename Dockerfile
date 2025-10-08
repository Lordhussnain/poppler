FROM node:20-bullseye-slim
RUN apt-get update && apt-get install -y --no-install-recommends poppler-utils imagemagick && rm -rf /var/lib/apt/lists/*
RUN useradd -m -s /bin/bash appuser
WORKDIR /home/appuser
USER appuser
COPY package.json package-lock.json ./
RUN npm ci --production
COPY . .
CMD ["node", "worker.js"]
