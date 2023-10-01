import { buildApp } from "./app";

const app = buildApp();
const port = parseInt(process.env.EXPRESS_PORT ?? "3000");

app.listen(port, () => console.log(`Backend listening on port ${port}`));
