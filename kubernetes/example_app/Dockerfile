FROM binocarlos/yarn-base
MAINTAINER kaiyadavenport@gmail.com
WORKDIR /app
COPY ./api/package.json /app/api/package.json
COPY ./api/yarn.lock /app/api/yarn.lock
RUN yarn install
COPY ./api /app/api
ENTRYPOINT ["node", "api/test/app.js"]
