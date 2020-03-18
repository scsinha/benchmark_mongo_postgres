import { generateWebsiteDataFiles } from './src/generatedEvents';
import { makeMetaData, createTables } from './src/populatePostgres';

var noOfEvents = process.argv.length >= 3 ? parseInt(process.argv[2]) : undefined;

(async function() {
    console.log(await generateWebsiteDataFiles({noOfEvents}));
    await makeMetaData();
    await createTables();
})();