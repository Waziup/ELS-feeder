"use strict";

const elasticsearch = require('elasticsearch');
const es = new elasticsearch.Client({
  host: 'elasticsearch.waziup.io',
  // log: 'trace'

});

async function main() {
  const results = await es.search({
    index: 'test-ws',
    sort: 'time:asc',
    size: 500
  });

  const hits = results.hits.hits;

  for (const hit of hits) {
    const data = {
      index: 'farm1',
      id: hit._id,
      type: hit._type,
      body: hit._source
    };

    console.log(data);
    await es.index(data);
  }
}

main();