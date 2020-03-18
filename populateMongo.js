import { populateMongo } from "./src/populateMongo";

var noOfEvents = process.argv.length >= 3 ? parseInt(process.argv[2]) : undefined;
var chunkSize = process.argv.length > 3 ? parseInt(process.argv[3]) : undefined;

populateMongo({noOfEvents, chunkSize});