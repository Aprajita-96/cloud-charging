# Cloud Charging Assessment

This is a small TypeScript project that allows you to locally run the Cloud Charging engine. Do not worry if you are not familiar with TypeScript, the codebase is small and easy to follow. The bulk of the logic is in `backend/app.ts`.

Prerequisites:
 - Docker (https://docs.docker.com/get-docker/)
 - NodeJS 18 (https://nodejs.org/en/download/)
 - Code Editor (e.g., VSCode - https://code.visualstudio.com/)

Getting started:
 - Install the dependencies: `cd backend && npm ci`.
 - Run `docker-compose up --build` in the root folder of the stack to start the cache.
 - You can now adjust the code as needed.
 - Run `cd backend && npm start` to start the backend.
 - Check the requests.http for some examples of how to use the backend via the API. Optionally, you can use the [VSCode Rest Client plugin](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) to execute this file.
 - Run `cd backend && npm test` to run tests against the API to determine the current latency.
