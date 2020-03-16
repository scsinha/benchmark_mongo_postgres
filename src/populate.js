import { connectToMongo, pgp, postgresDB } from './connections';

import assert from 'assert';
import { 
    EVENT_TYPES,
    EVENT_TYPE_BOUNCE, 
    EVENT_TYPE_CLICK, 
    EVENT_TYPE_DELIVERED, 
    EVENT_TYPE_OPEN, 
    EVENT_TYPE_SPAM,
    websiteId,
    emailContextId,
    randomEventGenerator
} from './generatedEvents';
import { msleep } from 'sleep';

console.log(websiteId);

export const populateMongo = async () => {
    try {
        const { db, disconnect } = await connectToMongo();

        const coll = db.collection('EmailMessageEvents');

        //create index here
        coll.createIndex(
            { type: 1, url: 1 },
            { name: 'event_url' }
        );

        coll.count(async (err, count) => {
            if (!err && count === 0) {
                const eventGenerator = randomEventGenerator({chunkSize: 100000});
                let next = eventGenerator.next();

                while (!next.done) {
                    const events = next.value;
                    
                    try {
                        const res = await coll.insertMany(events);
                        console.log(res.insertedCount, " Rows inserted.");
                    } catch (err) {
                        console.error(err);
                        disconnect();
                        throw err;
                    }

                    next = eventGenerator.next();
                }

                disconnect();
            } else {
                disconnect();
                console.log(count, " Rows exist.");
            }
        });
    } catch (err) {
        console.log(err);
    }
}

export const populatePostgres = () => {
    console.log("");
    console.log("");
    console.log(Array(150).join("#"));
    console.log(Array(150).join("#"));
    console.log("");
    console.log("");

    makeMetaData()
        .then(() => {
            console.log("Successfully created meta data");

            populateTables()
                .then(() => {
                    console.log("Successfully populated tables");
                })
                .catch(err => {
                    console.log("Error populating tables", err);
                })
        })
        .catch(err => {
            console.error("Error creating meta data", err);
        })
}

const makeMetaData = () => {
    return postgresDB.tx(t => {
        return t.batch([
            t.none('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'),
            ...createTables(t),
            t.none(
                `INSERT INTO email_events(website_id, email_context_id) 
                values('${websiteId}', '${emailContextId}')`
            )
        ]);
    })
};

const mappedTableName = {};
mappedTableName[EVENT_TYPE_BOUNCE] = 'email_bounce_events';
mappedTableName[EVENT_TYPE_DELIVERED] = 'email_delivered_events';
mappedTableName[EVENT_TYPE_OPEN] = 'email_opened_events';
mappedTableName[EVENT_TYPE_SPAM] = 'email_spam_events';

const populateTables = async () => {
    const eventGenerator = randomEventGenerator({chunkSize: 100000});

    let i = 0;
    let next = eventGenerator.next();

    while(!next.done) {
        let chunk = next.value;
        console.log(`Processing chunk ${i++}`);

        let queryBuckets = {};

        chunk.forEach(async (event) => {
            switch(event.type) {
                case EVENT_TYPE_BOUNCE:
                case EVENT_TYPE_OPEN:
                case EVENT_TYPE_DELIVERED:
                case EVENT_TYPE_SPAM:
                    let bucket = queryBuckets[event.type];
                    if (!bucket) {
                        bucket = queryBuckets[event.type] = [];
                    }
                    bucket.push(event);
                    break;
                default: break;
            }
        });

        try {
            await postgresDB.tx((t) => {
                console.log("Begining transaction");
    
                const queries = EVENT_TYPES.reduce((arr, eventType) => {
                    const bucket = queryBuckets[eventType];
    
                    if (bucket && bucket.length) {
                        console.log('Bucket size: ', eventType, bucket.length);
                        const cs = new pgp.helpers.ColumnSet([
                            {name: "website_id", prop: "websiteId"},
                            {name: "email_message_id", prop: "messageId"},
                            {name: "email_context_id", prop: "emailContextId"}
                        ], {table: mappedTableName[eventType]});
    
                        arr.push(t.none(pgp.helpers.insert(bucket, cs)));
                    }
    
                    return arr;
                }, []);
    
                console.log("Ending transaction");
    
                return t.batch(queries);
            });                    
        } catch(e) {
            console.error(e);
            throw e;
        }

        msleep(100);

        next = eventGenerator.next();
    }
};

const createTables = (t) => [
    t.none(
        `CREATE TABLE IF NOT EXISTS email_events(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4() not null,
            website_id uuid not null,
            email_context_id uuid not null UNIQUE
        )`
    ),   
    t.none(
        `CREATE INDEX ee on email_events (id, email_context_id)`
    ),
    t.none(
        `CREATE TABLE IF NOT EXISTS email_delivered_events(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            website_id uuid not null,
            email_context_id uuid not null,
            email_message_id uuid not null,
            FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
        )`
    ),
    t.none(
        `CREATE INDEX ed on email_delivered_events (id, email_context_id)`
    ),
    t.none(
        `CREATE INDEX ed1 on email_delivered_events (id, email_context_id, email_message_id)`
    ),
    t.none(
        `CREATE TABLE IF NOT EXISTS email_opened_events(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            website_id uuid not null,
            email_context_id uuid not null,
            email_message_id uuid not null,
            FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
        )`
    ),  
    t.none(
        `CREATE INDEX eo on email_opened_events (id, email_context_id)`
    ),
    t.none(
        `CREATE INDEX eo1 on email_opened_events (id, email_context_id, email_message_id)`
    ),
    t.none(
        `CREATE TABLE IF NOT EXISTS email_spam_events(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            website_id uuid not null,
            email_context_id uuid not null,
            email_message_id uuid not null,
            FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
        )`
    ),  
    t.none(
        `CREATE INDEX es on email_spam_events (id, email_context_id)`
    ),
    t.none(
        `CREATE INDEX es1 on email_spam_events (id, email_context_id, email_message_id)`
    ),
    t.none(
        `CREATE TABLE IF NOT EXISTS email_bounce_events(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            website_id uuid not null,
            email_context_id uuid not null,
            email_message_id uuid not null,
            FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
        )`
    ),
    t.none(
        `CREATE INDEX eb on email_bounce_events (id, email_context_id)`
    ),
    t.none(
        `CREATE INDEX eb1 on email_bounce_events (id, email_context_id, email_message_id)`
    ),
    t.none(
        `CREATE TABLE IF NOT EXISTS email_urls(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            website_id uuid not null,
            email_context_id uuid not null,
            url varchar not null,
            FOREIGN KEY (email_context_id) REFERENCES email_events (email_context_id)
        )`
    ),
    t.none(
        `CREATE TABLE IF NOT EXISTS email_click_events(
            id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
            website_id uuid not null,
            email_url_id uuid not null,
            email_message_id uuid not null,
            FOREIGN KEY (email_url_id) REFERENCES email_urls (id)
        )`
    )];