# The Vanguard relay: plain Node, no packages.
FROM node:20-alpine
WORKDIR /app
COPY relay.js .
ENV PORT=8787
EXPOSE 8787
USER node
CMD ["node", "relay.js"]
