"use strict";

const elasticsearch = require('elasticsearch');
const es = new elasticsearch.Client({
  host: 'elasticsearch.waziup.io',
  // log: 'trace'

});

//FARM1 first 5000 asc, then 5000 desc
//FARM2 10000 desc, 10000 asc
//FARM3 indices of farm3, chohan donot have any data, cohan does not exist

const ssf = 'watersense-ssf-tests';
const calci = 'watersense-calci-tests';
const chohan = 'watersense-chohan-tests';

async function main() {
  const results = await es.search({
    index: 'chohan',
    sort: 'time:desc',
    size: 10000
  });

  const hits = results.hits.hits;

  for (const hit of hits) {
    const data = {
      index: chohan,
      id: hit._id,
      type: hit._type,
      body: hit._source
    };

    console.log(data);
    await es.index(data);
  }
}

main();
