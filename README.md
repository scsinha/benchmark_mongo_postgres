# **This project compares execution times of MongoDB and PostgresDB with 1 million records.**

**Dependencies**
- Node JS
- Docker
- Docker Compose

**Steps:**
- Go to project root
- run ```./bootstrap```
- run ```npm i```
- run ```npm run populate-postgres```
- run ```npm run populate-mongo```
- run ```npm run time:Q```

The last command will execute the aggregate queries in both the databases and will return the result and execution times.

**Note**: The data population scripts can be given one/two arguments specifying the Event Data size and chunk size for loading them to database.

```npm run populate-postgres <no-of-events> <chunk-size>```
```npm run populate-mongo <no-of-events> <chunk-size>```

The population code will generate events for a website with 1-5 campaigns. And the number of events will be distributed among the campaigns based on the order. For example, if the total number of events to be generated for the website is 100 and the website has 2 campaigns. The numbers of events associated with each campaign are shown below.

The formula used is (#campaign_numer * 100 / ((no_of_campaigns * (no_of_campaigns + 1)) / 2))

- Campaign 1:  Math.floor( 1 * 100 / ((2 * 3) / 2) ) = 33 events
- Campaign 2:  Math.floor( 2 * 100 / ((2 * 3) / 2) ) = 66 events


**Example:**
```
ssinha@SSINHA-C02Z47JHLVDQ bench_mark_postgres_mongo % npm run time:Q

> bench_mark_postgres_mongo@1.0.0 time:Q /Users/ssinha/work/bench_mark_postgres_mongo
> node -r esm queryTimes.js

Successfully connected to mongo
[
  {
    email_context_id: '3f4ff265-fa54-40b7-9454-23d812f37c7a',
    delivered: '199610',
    bounced: '199490',
    opened: '199892',
    spam: '200449'
  }
]
Postgres aggregate query: 315.404ms
[
  [ { type: 'bounce', url: '' }, 199668 ],
  [ { type: 'spam', url: '' }, 200493 ],
  [ { type: 'delivered', url: '' }, 200348 ],
  [ { type: 'open', url: '' }, 199685 ]
]
Mongo aggregate query: 1127.891ms
```