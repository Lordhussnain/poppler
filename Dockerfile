# Dockerfile (pnpm-friendly)
FROM node:20-bullseye-slim

# install OS packages required for poppler + imagemagick
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      poppler-utils \
      imagemagick \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# create runtime user
RUN useradd -m -s /bin/bash appuser

WORKDIR /home/appuser/app

# Use production node env (optional) and quiet npm logs
ENV NODE_ENV=production
ENV NPM_CONFIG_LOGLEVEL=warn

# Copy only manifest + lockfile for layer caching
# Ensure this matches your actual lockfile name: pnpm-lock.yaml (common)
# If your lockfile is named pnpm-lock.yml, copy that instead.
COPY package.json pnpm-lock.yaml* ./

# Enable corepack (should be available in Node 20) and prepare pnpm
# Then install production dependencies using pnpm with frozen lockfile
RUN corepack enable && corepack prepare pnpm@latest --activate && \
    pnpm install --prod --frozen-lockfile --reporter=silent

# Copy rest of the project
COPY . .

# Fix ownership so appuser can read files & run the app
RUN chown -R appuser:appuser /home/appuser

# Switch to non-root user
USER appuser

# Run the worker
CMD ["node", "worker.js"]
