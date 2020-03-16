import { ObjectId } from 'mongodb';
import { integer, MersenneTwister19937 } from 'random-js';
import { uuid } from 'uuidv4';

export const EVENT_TYPE_CLICK = "click";
export const EVENT_TYPE_OPEN = "open";
export const EVENT_TYPE_DELIVERED = "delivered";
export const EVENT_TYPE_BOUNCE = "bounce";
export const EVENT_TYPE_SPAM = "spam";

const engine = MersenneTwister19937.autoSeed();
const random0to2 = integer(0, 2);
const random0to4 = integer(0, 4);
const random0to9999 = integer(0, 9999);

export const websiteId = uuid();
export const emailContextId = uuid();

const urls = [
    "https://foo.bar",
    "https://bar.foo",
    "https://somethingelse",
];

export const EVENT_TYPES = [
    EVENT_TYPE_OPEN,
    EVENT_TYPE_DELIVERED,
    EVENT_TYPE_CLICK,
    EVENT_TYPE_BOUNCE,
    EVENT_TYPE_SPAM
];

const messageIds = (() => {
    let ids = [];
    for (let i = 0; i < 10000; i++) {
        ids.push(uuid());
    }

    return ids;
})();

const createRandomEvent = () => {
    const eventType = EVENT_TYPES[random0to4(engine)];
    const url = EVENT_TYPE_CLICK === eventType ? urls[random0to2(engine)] : '';
    const messageId = messageIds[random0to9999(engine)];

    return {
        type: eventType,
        url,
        messageId,
        websiteId,
        emailContextId
    }
};

const NO_OF_EVENTS = 1000000;
const CHUNK_SIZE = 100000;

export const randomEventGenerator = function* ({noOfEvents=NO_OF_EVENTS, chunkSize=CHUNK_SIZE}) {
    const iterations = noOfEvents / chunkSize;

    console.log("Iterations: ", iterations);

    let i = 0;

    while (i < iterations) {
        console.log("Iteration: ", i++);
        yield createRandomEvents(chunkSize);
    }
};

const createRandomEvents = (size) => {
    let events = [];

    for (let i = 0; i < size; i++) {
        events.push(createRandomEvent());
    }

    return events;
}