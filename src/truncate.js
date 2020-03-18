import { connectToMongo, postgresDB } from "./connections"

export const truncateMongo = async () => {
    const { db, disconnect } = await connectToMongo();

    try {
        await db.collection('EmailMessageEvents').drop();
        console.log("MongoDB truncated");
    } catch(err) {
        console.error(err);
    } finally {
        disconnect();
    }
};

export const truncatePostgres = () => {
    postgresDB.tx(t => {
        return t.batch([
            t.none("drop table email_opened_events"),
            t.none("drop table email_delivered_events"),
            t.none("drop table email_spam_events"),
            t.none("drop table email_bounce_events"),
            t.none("drop table email_click_events"),
            t.none("drop table email_urls"),
            t.none("drop table email_events"),
        ]);
    })
    .then(() => {
        console.log("Successfully truncated tables");
    })
    .catch(err => {
        console.error("Error truncating tables");
    });
};