import express from "express";
import { createClient } from "redis";
import { json } from "body-parser";

const DEFAULT_BALANCE = 100;

interface ChargeResult {
    isAuthorized: boolean;
    remainingBalance: number;
    charges: number;
}

async function connect(): Promise<ReturnType<typeof createClient>> {
    const url = `redis://${process.env.REDIS_HOST ?? "localhost"}:${process.env.REDIS_PORT ?? "6379"}`;
    console.log(`Using redis URL ${url}`);
    const client = createClient({ url });
    await client.connect();
    return client;
}

async function reset(account: string): Promise<void> {
    const client = await connect();
    try {
        await client.set(`${account}/balance`, DEFAULT_BALANCE);
    } finally {
        await client.disconnect();
    }
}

// Change 0
// async function charge(account: string, charges: number): Promise<ChargeResult> {
//     const client = await connect();
//     try {
//         const balance = parseInt((await client.get(`${account}/balance`)) ?? "");
//         if (balance >= charges) {
//             client.set(`${account}/balance`, balance - charges);
//             const remainingBalance = parseInt((await client.get(`${account}/balance`)) ?? "");
//             return { isAuthorized: true, remainingBalance, charges };
//         } else {
//             return { isAuthorized: false, remainingBalance: balance, charges: 0 };
//         }
//     } finally {
//         await client.disconnect();
//     }
// }

// Change 1
// async function charge(account: string, charges: number): Promise<ChargeResult> {
//     const client = await connect();

//     try {
//         const startingBalance = parseInt(
//             (await client.get(`${account}/balance`)) ?? ""
//         );
//         if (startingBalance < charges){
//             // Transaction failed, handle the error
//             console.error("Transaction failed");
//             return { isAuthorized: false, remainingBalance: 0, charges: 0 };
//         }
        
//         // Start a multi (transaction) block
//         const multi = client.multi();

//         // Deduct charges from the balance
//         multi.decrBy(`${account}/balance`, charges);

//         // Execute the transaction
//         const transactionResult = await multi.exec();

//         // Check if the transaction was successful
//         if (transactionResult) {
//             // Retrieve the updated balance
//             const remainingBalance = parseInt(
//                 (await client.get(`${account}/balance`)) ?? ""
//             );

//             // Check if the remaining balance is sufficient
//             if (remainingBalance >= 0) {
//                 return { isAuthorized: true, remainingBalance, charges };
//             } else {
//                 // Rollback the transaction if the balance is negative
//                 await multi.incrBy(`${account}/balance`, charges);
//                 return { isAuthorized: false, remainingBalance, charges: 0 };
//             }
//         } else {
//             // Transaction failed, handle the error
//             console.error("Transaction failed");
//             return { isAuthorized: false, remainingBalance: 0, charges: 0 };
//         }
//     } finally {
//         await client.disconnect();
//     }
// }


async function charge(account: string, charges: number): Promise<ChargeResult> {
    const client = await connect();

    try {
        const startingBalance = parseInt(
            (await client.get(`${account}/balance`)) ?? ""
        );
        if (startingBalance < charges){
            // Transaction failed, handle the error
            console.error("Transaction failed");
            return { isAuthorized: false, remainingBalance: startingBalance, charges: 0 };
        }
        
        // We need lock on data resorce for set operation to avoid data read discrepency
        const lockKey = `${account}/lock`;
        const lockValue = account;
        // const lockDuration = 2; 

        const isLockAcquired = await client.set(lockKey, lockValue);

        if (isLockAcquired === "OK") {
            try {
                // Start a multi (transaction) block
                const multi = client.multi();

                // Deduct charges from the balance
                multi.decrBy(`${account}/balance`, charges);

                // Execute the transaction
                const transactionResult = await multi.exec();

                // Check if the transaction was successful
                if (transactionResult) {
                    // Retrieve the updated balance
                    const remainingBalance = parseInt(
                        (await client.get(`${account}/balance`)) ?? ""
                    );

                    // Check if the remaining balance is sufficient
                    if (remainingBalance >= 0) {
                        return { isAuthorized: true, remainingBalance, charges };
                    } else {
                        // Rollback the transaction if the balance is negative
                        await multi.incrBy(`${account}/balance`, charges);
                        return { isAuthorized: false, remainingBalance, charges: 0 };
                    }
                } else {
                    // Transaction failed, handle the error
                    console.error("Transaction failed");
                    return { isAuthorized: false, remainingBalance: startingBalance, charges: 0 };
                }
            } finally {
                // Release the lock when done
                await client.del(lockKey);
            }
        } else {
            // Lock acquisition failed, another client is modifying the data
            console.error("Transaction failed due to lock aquisition failure, possible parallel write");
            return { isAuthorized: false, remainingBalance: startingBalance, charges: 0 };
        }
    } finally {
        await client.disconnect();
    }
}

export function buildApp(): express.Application {
    const app = express();
    app.use(json());
    app.post("/reset", async (req, res) => {
        try {
            const account = req.body.account ?? "account";
            await reset(account);
            console.log(`Successfully reset account ${account}`);
            res.sendStatus(204);
        } catch (e) {
            console.error("Error while resetting account", e);
            res.status(500).json({ error: String(e) });
        }
    });
    app.post("/charge", async (req, res) => {
        try {
            const account = req.body.account ?? "account";
            const result = await charge(account, req.body.charges ?? 10);
            console.log(`Successfully charged account ${account}`);
            res.status(200).json(result);
        } catch (e) {
            console.error("Error while charging account", e);
            res.status(500).json({ error: String(e) });
        }
    });
    return app;
}
