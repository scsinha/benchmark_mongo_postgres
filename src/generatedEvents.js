'use strict';

import { integer, MersenneTwister19937 } from 'random-js';
import { uuid } from 'uuidv4';
import fs from 'fs';

export const EVENT_TYPE_CLICK = "click";
export const EVENT_TYPE_OPEN = "open";
export const EVENT_TYPE_DELIVERED = "delivered";
export const EVENT_TYPE_BOUNCE = "bounce";
export const EVENT_TYPE_SPAM = "spam";

const engine = MersenneTwister19937.autoSeed();
const random0to2 = integer(0, 2);
const random0to4 = integer(0, 4);
const random1to5 = integer(1, 5);

export const websiteId = uuid();
export const emailContextId = uuid();

export const EVENT_TYPES = [
    EVENT_TYPE_OPEN,
    EVENT_TYPE_DELIVERED,
    EVENT_TYPE_CLICK,
    EVENT_TYPE_BOUNCE,
    EVENT_TYPE_SPAM
];

export const NO_OF_EVENTS = 5000000;
export const CHUNK_SIZE = 100000;

const urls = [
    "https://foo.bar",
    "https://bar.foo",
    "https://somethingelse",
];

const createRandomEvent = (websiteId, emailContextId, messageId) => {
    const eventType = EVENT_TYPES[random0to4(engine)];
    const url = EVENT_TYPE_CLICK === eventType ? urls[random0to2(engine)] : '';

    return {
        type: eventType,
        url,
        messageId,
        websiteId,
        emailContextId
    }
};

const createRandomEvents = (websiteId, emailContextId, messageIds, size) => {
    let events = [];
    const randomMessageIdIdx = integer(0, messageIds.length - 1);

    for (let i = 0; i < size; i++) {
        events.push(createRandomEvent(websiteId, emailContextId, messageIds[randomMessageIdIdx(engine)]));
    }

    return events;
};

const createStream = (websiteId, emailContextId, type) => {
    const dirpath = `./generated/website-${websiteId}/email-${emailContextId}`;
    fs.mkdirSync(dirpath, { recursive: true });

    const path = `${dirpath}/${type}.csv`;
    const stream = fs.createWriteStream(path, {
        flags: 'wx'
    });
    stream.on('finish', () => {
        console.log(`wrote file for website: ${websiteId}, email: ${emailContextId}, path: ${path}`);
    });

    // switch(type) {
    //     case EVENT_TYPE_BOUNCE:
    //     case EVENT_TYPE_OPEN:
    //     case EVENT_TYPE_DELIVERED:
    //     case EVENT_TYPE_SPAM:
    //         stream.write('website_id,email_context_id,email_message_id\n');
    //         break;
    //     case EVENT_TYPE_CLICK:
    //         stream.write('website_id,email_context_id,email_message_id,url\n');
    // }

    return stream;
};

const createRandomEventsCSVFile = (websiteId, emailContextId, messageIds, size) => {
    const streamMap = {};
    streamMap[EVENT_TYPE_OPEN] = createStream(websiteId, emailContextId, EVENT_TYPE_OPEN);
    streamMap[EVENT_TYPE_DELIVERED] = createStream(websiteId, emailContextId, EVENT_TYPE_DELIVERED);
    streamMap[EVENT_TYPE_SPAM] = createStream(websiteId, emailContextId, EVENT_TYPE_SPAM);
    streamMap[EVENT_TYPE_BOUNCE] = createStream(websiteId, emailContextId, EVENT_TYPE_BOUNCE);
    streamMap[EVENT_TYPE_CLICK] = createStream(websiteId, emailContextId, EVENT_TYPE_CLICK);

    const randomMessageIdIdx = integer(0, messageIds.length - 1);

    for (let i = 0; i < size; i++) {
        const event = createRandomEvent(websiteId, emailContextId, messageIds[randomMessageIdIdx(engine)]);
        const { type, url, messageId } = event;
        
        switch(type) {
            case EVENT_TYPE_OPEN:
            case EVENT_TYPE_DELIVERED:
            case EVENT_TYPE_SPAM:
            case EVENT_TYPE_BOUNCE:
                streamMap[type].write(`${websiteId},${emailContextId},${messageId}\n`);
                break;
            case EVENT_TYPE_CLICK:
                streamMap[type].write(`${websiteId},${emailContextId},${messageId},${url}\n`);
        }
    }

    // Now close streams
    return Promise.all(
        Object.keys(streamMap).map(key => new Promise(resolve => {
            streamMap[key].end(resolve);
        }))
    );
};

export const generateWebsite = ({ noOfEvents=NO_OF_EVENTS, chunkSize=CHUNK_SIZE }) => {
    const websiteId = uuid();
    const emailContextIds = Array(random1to5(engine)).fill().map(() => uuid());
    const eventsChunks = (function* () {
        const factor = noOfEvents / (emailContextIds.length * (emailContextIds.length + 1) / 2);
        for (let idx = 0; idx < emailContextIds.length; idx++) {
            const emailContextId = emailContextIds[idx];
            console.log(`\n\nGenerating for website: ${websiteId} and email context: ${emailContextId}`);
            
            const nOfEvts = Math.floor(factor * (idx + 1));
            let iterations = Math.floor(nOfEvts / chunkSize);
            let chnkSize = chunkSize;
            let messageIds = Array(Math.min(Math.floor(noOfEvents * 0.1), 10000)).fill().map(() => uuid());

            if (iterations < 1) {
                iterations = 1;
                chnkSize = nOfEvts;
            }

            console.log(`Generating ${nOfEvts} for email: ${emailContextId}`);
            console.log(`Total chunks: ${iterations}`);
            let i = 0;
            while (i < iterations) {
                console.log(`\nGenerating Chunk: ${++i}`);
                const randomEvents = createRandomEvents(websiteId, emailContextId, messageIds, chnkSize);
                yield randomEvents;
            }
        }
    })();

    return {
        websiteId,
        emailContextIds,
        eventsChunks
    }
};

export const generateWebsiteDataFiles = async ({ noOfEvents = NO_OF_EVENTS }) => {
    const websiteId = uuid();
    const emailContextIds = Array(random1to5(engine)).fill().map(() => uuid());
    const factor = noOfEvents / (emailContextIds.length * (emailContextIds.length + 1) / 2);
    
    for (let idx = 0; idx < emailContextIds.length; idx++) {
        const emailContextId = emailContextIds[idx];
        console.log(`\n\nGenerating for website: ${websiteId} and email context: ${emailContextId}`);
        
        const nOfEvts = Math.floor(factor * (idx + 1));
        let messageIds = Array(Math.min(Math.floor(noOfEvents * 0.1), 10000)).fill().map(() => uuid());

        console.log(`Generating ${nOfEvts} for email: ${emailContextId}`);
        await createRandomEventsCSVFile(websiteId, emailContextId, messageIds, nOfEvts);
    }

    return { websiteId, emailContextIds };
};