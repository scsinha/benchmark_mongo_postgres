import { randomEventGenerator } from './src/generatedEvents';

const gen = randomEventGenerator({ noOfEvents: 100, chunkSize: 10 });

let next = gen.next();
while(!next.done) {
    console.log(JSON.stringify(next.value));
    next = gen.next();
}