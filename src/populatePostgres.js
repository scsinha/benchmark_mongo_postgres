import { pool } from './connections';
import {from as copyFrom} from 'pg-copy-streams';
import { Readable } from 'stream';
import fs from 'fs';

import { 
    EVENT_TYPES,
    EVENT_TYPE_BOUNCE, 
    EVENT_TYPE_DELIVERED, 
    EVENT_TYPE_OPEN, 
    EVENT_TYPE_SPAM,
    generateWebsite,
    NO_OF_EVENTS,
    CHUNK_SIZE,
    generateWebsiteDataFiles,
    EVENT_TYPE_CLICK
} from './generatedEvents';
import { msleep } from 'sleep';

export const populatePostgres = (options) => {
    console.log("");
    console.log("");
    console.log(Array(150).join("#"));
    console.log(Array(150).join("#"));
    console.log("");
    console.log("");

    return new Promise((resolve, reject) => {
        makeMetaData()
        .then(() => {
            console.log('Setup metadata');

            createTables()
                .then(() => {
                    if (options.fromCSV) {
                        populateTablesFromCSV(options)
                            .then(() => {
                                console.log("Successfully populated tables");
                                resolve();
                            })
                            .catch(reject);
                    } else {
                        populateTables(options)
                            .then(() => {
                                console.log("Successfully populated tables");
                                resolve();
                            })
                            .catch(reject);
                    }
                })
                .catch(reject);
        })
        .catch(reject);
    }); 
}

const executeQuery = async (queryFn) => {
    const client = pool.connect();
    
    try {
        await queryFn(client);
    } catch(e) {
        console.error(e);
        throw e;
    } finally {
        client && await (await client).release();
    }
}

export const makeMetaData = 
    async () => await executeQuery(
        async (client) => {
            (await client).query('create extension if not exists "uuid-ossp"');
            console.log("Finished chaning meta data...");
        }
    );

const mappedTableName = {};
mappedTableName[EVENT_TYPE_BOUNCE] = 'email_bounce_events';
mappedTableName[EVENT_TYPE_DELIVERED] = 'email_delivered_events';
mappedTableName[EVENT_TYPE_OPEN] = 'email_opened_events';
mappedTableName[EVENT_TYPE_SPAM] = 'email_spam_events';

const populateTables = async ({noOfEvents = NO_OF_EVENTS, chunkSize = CHUNK_SIZE}) => {
    const {websiteId, emailContextIds, eventsChunks} = generateWebsite({noOfEvents, chunkSize});

    await executeQuery(async (client) => {
        await emailContextIds.forEach(async emailContextId => 
            (await client).query(`INSERT INTO email_events(website_id, email_context_id) values('${websiteId}', '${emailContextId}')`)
        );
    });
    console.log('Successfully created email_event');


    let i = 0;
    for (let chunk of eventsChunks) {
        console.log("Begining transaction");

        i++;
        let queryBuckets = {};

        chunk.forEach((event) => {
            switch(event.type) {
                case EVENT_TYPE_BOUNCE:
                case EVENT_TYPE_OPEN:
                case EVENT_TYPE_DELIVERED:
                case EVENT_TYPE_SPAM:
                    let bucket = queryBuckets[event.type];
                    if (!bucket) {
                        bucket = queryBuckets[event.type] = []; //['website_id\temail_context_id\temail_message_id\n'];
                    }
                    const { websiteId, emailContextId, messageId } = event; 
                    bucket.push(`${websiteId}\t${emailContextId}\t${messageId}\n`);
                    break;
                default: break;
            }
        });

        await executeQuery(async (client) => {
            for (let eventType of EVENT_TYPES) {
                const bucket = queryBuckets[eventType];
    
                if (bucket && bucket.length) {
                    const tableName = mappedTableName[eventType];
                    const bucketIterable = async function * generate(bckt) {
                        for (let idx in bckt) {
                            yield bckt[idx];
                        }
                    };
    
                    const readable = Readable.from(bucketIterable(bucket));
                    readable.on('error', (err) => {
                        console.error("Stream error: ", err);
                    });
    
                    const stream = (await client).query(copyFrom(`COPY ${tableName}(website_id,email_context_id,email_message_id) FROM STDIN`));
                    stream.on('error', (error) => {
                        console.log(`Error in copy command: ${error}`);
                    });
                    stream.on('end', () => {
                        console.log(`Completed loading ${bucket.length} records into ${tableName}`);
                    });
                    readable.pipe(stream);
                }
            }
        });

        console.log("End Transaction");
    } 
};

const populateTablesFromCSV = async ({noOfEvents = NO_OF_EVENTS}) => {
    const {websiteId, emailContextIds} = await generateWebsiteDataFiles({noOfEvents});

    await executeQuery(async (client) => {
        await emailContextIds.forEach(async emailContextId => 
            (await client).query(`INSERT INTO email_events(website_id, email_context_id) values('${websiteId}', '${emailContextId}')`)
        );
    });
    console.log('Successfully created email_event\n');


    for await (let emailContextId of populateEventsFromCSVs({websiteId,emailContextIds})) {
        console.log('Processed emailContext: ', emailContextId, '\n');
    }
}

const populateEventsFromCSVs = async function * ({websiteId, emailContextIds}) {
    for (let emailContextId of emailContextIds) {
        console.log('\nProcessing emailContext: ', emailContextId);
        // load all the files for the events
        for (let eventType of EVENT_TYPES) {
            if (eventType === EVENT_TYPE_CLICK) {
                continue;
            }

            const client = await pool.connect();

            await new Promise((resolve, reject) => {
                const tableName = mappedTableName[eventType];
                const path = `./generated/website-${websiteId}/email-${emailContextId}/${eventType}.csv`;

                
                var fileStream = fs.createReadStream(path);
                fileStream.on('error', (err) => {
                    console.error("FileStream error: ", err);
                    client.release();
                    reject(err);
                });

                const stream = client.query(copyFrom(`COPY ${tableName}(website_id,email_context_id,email_message_id) FROM STDIN WITH (FORMAT csv)`));
                
                stream.on('error', (error) => {
                    console.log(`Error in copy command: ${error}`);
                    client.release();
                    reject(error);
                });
                stream.on('end', () => {
                    console.log(`Completed loading ${eventType} records into ${tableName}`);
                    client.release();
                    resolve();
                });
                fileStream.pipe(stream);
            });
        }

        emailContextId = yield emailContextId;
    }
};

export const createTables = async () => 
    await executeQuery(async (client) => {
        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_events(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_context_id uuid not null UNIQUE
            )`
        );

        (await client).query(`CREATE INDEX IF NOT EXISTS ee on email_events (id, website_id, email_context_id)`);

        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_delivered_events(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_context_id uuid not null,
                email_message_id uuid not null,
                FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
            )`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS ed on email_delivered_events (id, website_id, email_context_id)`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS ed1 on email_delivered_events (id, website_id, email_context_id, email_message_id)`
        );

        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_opened_events(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_context_id uuid not null,
                email_message_id uuid not null,
                FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
            )`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS eo on email_opened_events (id, website_id, email_context_id)`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS eo1 on email_opened_events (id, website_id, email_context_id, email_message_id)`
        );

        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_spam_events(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_context_id uuid not null,
                email_message_id uuid not null,
                FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
            )`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS es on email_spam_events (id, website_id, email_context_id)`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS es1 on email_spam_events (id, website_id, email_context_id, email_message_id)`
        );

        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_bounce_events(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_context_id uuid not null,
                email_message_id uuid not null,
                FOREIGN KEY (email_context_id) REFERENCES email_events(email_context_id)
            )`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS eb on email_bounce_events (id, website_id, email_context_id)`
        );

        (await client).query(
            `CREATE INDEX IF NOT EXISTS eb1 on email_bounce_events (id, website_id, email_context_id, email_message_id)`
        );

        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_urls(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_context_id uuid not null,
                url varchar not null,
                FOREIGN KEY (email_context_id) REFERENCES email_events (email_context_id)
            )`
        );

        (await client).query(
            `CREATE TABLE IF NOT EXISTS email_click_events(
                id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
                website_id uuid not null,
                email_url_id uuid not null,
                email_message_id uuid not null,
                FOREIGN KEY (email_url_id) REFERENCES email_urls (id)
            )`
        );

        console.log("Finished creating tables...");
    });