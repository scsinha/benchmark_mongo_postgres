import { populatePostgres } from './src/populatePostgres';

var noOfEvents = process.argv.length >= 3 ? parseInt(process.argv[2]) : undefined;
var chunkSize = process.argv.length > 3 ? parseInt(process.argv[3]) : undefined;

console.time("Populating Postgres");
populatePostgres({noOfEvents, chunkSize, fromCSV: true})
    .then(() => {
        console.timeEnd("Populating Postgres");
    });