import { connectToMongo, pgp, postgresDB } from "./connections";
import assert from 'assert';

export default async () => {
    const {db, disconnect} = await connectToMongo();

    console.time("Mongo aggregate query");
    db.collection('EmailMessageEvents')
        .aggregate([
            {$match: {type: {$ne: 'click'}}},
            {$group: {
                _id: {
                    websiteId: '$websiteId', 
                    emailContextId: '$emailContextId', 
                    type:'$type', 
                    url: '$url'
                }, 
                total: { 
                    $sum: 1 
                }
            }
        }])
        .toArray((err, docs) => {
            assert.equal(err, null);

            const outJson = docs.reduce((out, doc) => {
                const { websiteId, emailContextId, type, url } = doc._id;
                let website = out[websiteId];
                if (!website) {
                    website = out[websiteId] = {};
                }
                let emailContext = website[emailContextId];
                if (!emailContext) {
                    emailContext = website[emailContextId] = {};
                }
                emailContext[type] = doc.total;

                return out;
            },{});
            console.log(outJson);
            console.timeEnd("Mongo aggregate query");

            disconnect();
        });

    console.time("Postgres aggregate query");
    postgresDB.any(`
        select ee.email_context_id, edc.total as Delivered, ebc.total as Bounced, eoc.total as Opened, esc.total as Spam
        from email_events ee,
            (select ed.email_context_id ,count(id) as total from email_delivered_events ed group by ed.email_context_id) edc,
            (select eb.email_context_id, count(id) as total from email_bounce_events eb group by eb.email_context_id) ebc,
            (select eo.email_context_id, count(id) as total from email_opened_events eo group by eo.email_context_id) eoc,
            (select es.email_context_id, count(id) as total from email_spam_events es group by es.email_context_id) esc
        where ee.email_context_id = edc.email_context_id
        and ee.email_context_id = ebc.email_context_id
        and ee.email_context_id = eoc.email_context_id
        and ee.email_context_id = esc.email_context_id
    `)
    .then(res => {
        console.log(res);
        console.timeEnd("Postgres aggregate query");
    });
}