FROM adexchangegrp/node:latest

ENV NODE_ENV development

COPY . /src
WORKDIR /src
RUN npm install

ENV NODE_ENV production