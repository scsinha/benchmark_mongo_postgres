import { connectToMongo } from './connections';

import { 
    generateWebsite,
    NO_OF_EVENTS,
    CHUNK_SIZE
} from './generatedEvents';


export const populateMongo = async ({noOfEvents = NO_OF_EVENTS, chunkSize = CHUNK_SIZE}) => {
    const { db, disconnect } = await connectToMongo();

    try {
        const coll = db.collection('EmailMessageEvents');

        //create index here
        coll.createIndex(
            { websiteId: 1, emailContextId: 1, type: 1, url: 1 },
            { name: 'event_url' }
        );

        const { eventsChunks } = generateWebsite({noOfEvents, chunkSize});

        for (let chunk of eventsChunks) {
            console.log(`Chunk size: ${chunk.length}`);
            const res = await coll.insertMany(chunk);
            console.log(res.insertedCount, " Rows inserted.");
        }
    } catch (err) {
        console.log(err);
    } finally {
        disconnect();
    }
}