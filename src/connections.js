import { MongoClient } from 'mongodb';
import PGPromise from 'pg-promise';
import { Pool } from 'pg';

const DB_NAME = "benchmark";

export const connectToMongo = async () => {
    return new Promise((resolve, reject) => {
        const user = encodeURIComponent('mongo');
        const password = encodeURIComponent('mongo');
        const authMechanism = 'DEFAULT';
        const MONGO_URL = `mongodb://${user}:${password}@localhost:27017/?authMechanism=${authMechanism}`;

        const client = new MongoClient(MONGO_URL);

        client.connect((err) => {
            if (!err) {
                console.log("Successfully connected to mongo");

                const db = client.db(DB_NAME);

                resolve({
                    db,
                    disconnect: () => client.close()
                });
            } else {
                reject(err);
            }
        });
    });
}

const pgCn = {
    host: 'localhost',
    port: 5432,
    database: 'benchmark',
    user: 'postgres',
    password: 'postgres',
    max: 30,
    idleTimeoutMillis: 0,
};
export const pgp = PGPromise({});
export const postgresDB = pgp(pgCn);
export const pool = new Pool(pgCn);