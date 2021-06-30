FROM node:16.2.0

ADD ./package.json /app/package.json
ADD ./src/index.js /app/index.js

WORKDIR /app

RUN npm i

ENTRYPOINT ["node", "index.js"]
