FROM node:18-bullseye 

RUN mkdir -p /code
ADD . /code
WORKDIR /code

RUN npm ci

CMD npm start