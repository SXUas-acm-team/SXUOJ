# syntax=docker/dockerfile:1
FROM node:22-bookworm-slim AS build
WORKDIR /workspace
COPY hoj-vue/package.json hoj-vue/package-lock.json ./
RUN --mount=type=cache,id=sxuoj-npm,target=/root/.npm,sharing=locked npm ci --no-audit --no-fund
COPY hoj-vue/babel.config.js hoj-vue/vue.config.js ./
COPY hoj-vue/public ./public
COPY hoj-vue/src ./src
COPY hoj-vue/tests ./tests
COPY hoj-scrollBoard/js/jquery.min.js /hoj-scrollBoard/js/jquery.min.js
COPY hoj-scrollBoard/js/scrollboard.js /hoj-scrollBoard/js/scrollboard.js
RUN npm test && npm run build

FROM nginx:1.28-alpine AS frontend
LABEL io.sxuoj.component="frontend"
COPY docker/frontend.conf.template /etc/nginx/templates/default.conf.template
COPY docker/frontend-app.conf.template /opt/hoj/nginx/app.conf.template
COPY docker/frontend-tls.conf.template /opt/hoj/nginx/tls.conf.template
COPY --chmod=755 docker/frontend-env.envsh /docker-entrypoint.d/19-sxuoj-env.envsh
COPY --from=build /workspace/dist /opt/hoj/frontend
# Preserve the former extraction path; active assets stay outside legacy bind mounts.
COPY --from=build /workspace/dist /usr/share/nginx/html
COPY hoj-scrollBoard /opt/hoj/frontend/scrollBoard
EXPOSE 80 443
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=5 \
    CMD wget -q -O /dev/null http://127.0.0.1/healthz || exit 1
