import { createProxy } from "node-tcp-proxy";
const serviceHost = process.env.SERVICE_HOST;
const servicePort = parseInt(process.env.SERVICE_PORT);
const delay = parseInt(process.env.TCP_DELAY ?? "1000");

const sleep = () => new Promise((resolve => setTimeout(resolve, delay)));

createProxy(servicePort, [serviceHost], [servicePort], {
    upstream: (_, data) => sleep().then(() => data),
    downstream: (_, data) => sleep().then(() => data),
});