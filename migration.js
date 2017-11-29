"use strict";
const body = require('./elasticsearchIndex.js');
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
const mitchells = 'watersense-mitchells_farm-tests';
const dest = 'mitchells';

async function createTempIndex(index) {
  try {
    await es.indices.create({
      index: index,
      body: body.mappings
    });
    /*updateAllTypes
        Boolean — Whether to update the mapping for all fields with the same name across all types or not */
  } catch (err) {
    console.log("ERROR in creating index", err);
  }
}

async function main() {
  await createTempIndex(dest);
  let results;
  try {
    results = await es.search({
      index: mitchells,
      sort: 'time:desc',
      size: 1000
    });
  } catch (err) {
      console.log("ERROR in indexing", err);
  }

  const hits = results.hits.hits;

  for (const hit of hits) {
    const data = {
      index: dest,
      id: hit._id,
      type: hit._type,
      body: hit._source
    };

    //console.log(data);
    try {
      await es.index(data);
    } catch (err) {
      console.log("ERROR in indexing", err);
    }
  }
}

main();