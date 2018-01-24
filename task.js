const Promise = require('bluebird');
const config = require('./config');
const body = require('./elasticsearchIndex.js');
const rp = require('request-promise');
const elasticsearch = require('elasticsearch');
const log = require('./log');
const shortid = require('shortid');
var Orion = require('./orion.js');

const excludedAttributes = new Set(['gateway_id', 'type', 'owner', 'dateModified']);
const TriggerTypes = {
    Time: 'time',
    Subscription: 'subscription'
};

let el = false;

module.exports = class Task {
    constructor(conf) {
        this.conf = conf;
        this.orionConfig = config.mergeWith(conf.orion, 'orion');
        this.orion = new Orion(this.orionConfig);
        this.esConfig = config.mergeWith(conf.elasticsearch, 'elasticsearch');

        this.indexExists = new Map();
        this.cid = shortid.generate();
    }


    connectElasticsearch() {
        this.es = new elasticsearch.Client({
            host: `${this.esConfig.uri}`,
            requestTimeout: 150000,
        });
    }
    /*
    pingElasticsearch() {
        this.es.ping({
            requestTimeout: 150000,
        }, function (error) {
            if (error) {
                log.error('elasticsearch cluster is down!', error.message);
                el = false;
            } else {
                log.info('All is well');
                el = true;
            }
        });
    }*/

    async init() {
        log.info("ElasticSearch:", this.esConfig.uri, "Orion:", this.orionConfig.uri)
        
        try {
            this.connectElasticsearch();
            /*this.pingElasticsearch();
            log.error(`el: ${el} `);
            
            while (el === false) {
                log.error('Retrying to connect to elasticsearch...');
                setInterval(() => { this.connectElasticsearch(); this.pingElasticsearch();}, 15000);                
                log.error(`el: ${el} `);
            }

            if (el === true) {
                log.info('DONE')
            }*/ 
        } catch (err) {
            log.error('CONNECT:', JSON.stringify(err));
        }

        // Discard all existing subscriptions that could relate to this task
        // (or some other from this instance of feeder)
        try {
            await this.createIndex(this.orionConfig.service);
            const expectedDesc = this._getSubscriptionDesc();
            const resp = await this.orion.getSubscriptions();
            for (const entry of resp) {
                if (entry.description === expectedDesc) {
                    await this.orion.deleteSubscription(entry.id);
                }
            }
        } catch (err) {
            log.error(JSON.stringify(err));
        }
    }

    //Create an index in Elasticsearch if it does not exist yet
    //This is to support automatic discovery of sensors and service paths
    async createIndex(index) {
        //automatic discovery: has been moved to other parts: subscriptions pattern, and doPeriod
        log.info('index:', JSON.stringify(index));
        if (this.indexExists.has(index) === false) {
            log.info('Creating/updating an index for', index);
            let flag = false;
            try {
                flag = await this.es.indices.exists({ index: index });
            } catch (err) {
                log.error("ERROR in checking index", JSON.stringify(err));
            }

            if (!flag) {
                log.info('Creating an index for ', index);
                try {
                    await this.es.indices.create({
                        index: index,
                        body: body.mappings
                    });
                    this.indexExists.set(index, true);
                    
                    /*updateAllTypes
                        Boolean — Whether to update the mapping for all fields with the same name across all types or not */
                } catch (err) {
                    log.error("ERROR in creating index", JSON.stringify(err));
                }
            } else {
                log.info('Updating mappings of index for ', index);
                // do this based on task's index type
                for (let mapType in body.mappings) {
                    log.info(mapType, body.mappings[mapType]);
                    try {
                        const ret = await this.es.indices.putMapping({
                            index: index,
                            body: body.mappings[mapType],
                            type: mapType
                        });
                        log.info('putMapping operation: ', JSON.stringify(ret));
                    } catch (err) {
                        log.error("ERROR in putMapping", JSON.stringify(err));
                    }
                }
            }
        }
    }

    async createIndexes0() {
        //automatic discovery
        let allSps = await this.orion.fetchServicePaths();
        for (let indexName of allSps) {
            //log.info('indexName:', JSON.stringify(indexName));
            //let indexName = this.generateIndex(sp);
            if (this.indexExists.has(indexName) === false) {
                log.info('Creating/updating index for', indexName);
                let flag = await this.es.indices.exists({ index: indexName });
                this.indexExists.set(indexName, true);
                if (!flag) {
                    log.info('Creating an index for ', indexName);
                    try {
                        await this.es.indices.create({
                            index: indexName,
                            body: body.mappings
                        });
                    } catch (err) {
                        log.error("ERROR in creating index", JSON.stringify(err));
                        continue;
                    }

                } else {
                    log.info('Updating mappings of index for ', indexName);
                    // do this based on task's index type
                    for (let mapType in body.mappings) {
                        log.info(mapType, body.mappings[mapType]);
                        try {
                            const ret = await this.es.indices.putMapping({
                                index: indexName,
                                body: body.mappings[mapType],
                                type: mapType
                            });
                            log.info('putMapping operation: ', JSON.stringify(ret));
                        } catch (err) {
                            log.error("ERROR in putMapping", JSON.stringify(err));
                            continue;
                        }
                    }
                }
            }
        }
    }

    //sensors data are filtered via ...
    async feedToElasticsearch(sensors) {
        const bulkBody = [];
        const bulkBodyGlobal = [];
        let attrType;
        let attrVal;
        let value;
        let index;
        let received_time = new Date();
        received_time = received_time.toISOString();

        // do this based on task's index type
        for (const sensor of sensors) {
            //FIXME: check domain existence
            index = this.generateIndex(sensor.domain);
            try {
                await this.createIndex(index);
            } catch (err) {
                log.error(JSON.stringify(err));
            }

            for (const attribute of sensor.attributes) {
                switch (attribute.type.toLowerCase()) {
                    case 'number': attrType = 'sensingNumber'; value = 'value'; attrVal = attribute.value; break;
                    case 'geo:point': attrType = 'sensingGeo'; value = 'geo'; attrVal = attribute.value.coordinates; break;
                    case 'geo:json':
                        if (attribute.value.hasOwnProperty("type") &&
                            attribute.value.type === "Point") {
                            attrType = 'sensingGeo'; value = 'geo'; attrVal = attribute.value.coordinates; break;
                        }
                    case 'string': attrType = 'sensingKeyword'; value = 'keyword'; attrVal = attribute.value; break;
                    case 'text': attrType = 'sensingText'; value = 'text'; attrVal = attribute.value; break;
                    case 'datetime': attrType = 'sensingDate'; value = 'date'; attrVal = attribute.value; break;
                    case 'object': attrType = 'sensingObject'; value = 'object'; attrVal = attribute.value; break;
                    default:
                        log.error(`Unsupported attribute type: ${attribute.type} in ${index}/${sensor.name}.${attribute.name}`);
                        continue;
                }

                let doc = {
                    entity_id: sensor.entity_id,
                    measurement_id: attribute.measurement_id,
                    received_time: received_time,
                    [value]: attrVal
                }

                if (sensor.hasOwnProperty('entity_name'))
                    doc.entity_name = sensor.entity_name
                if (sensor.hasOwnProperty('domain'))
                    doc.domain = sensor.domain

                if (attribute.hasOwnProperty('measurement_dimension'))
                    doc.measurement_dimension = attribute.measurement_dimension
                if (attribute.hasOwnProperty('measurement_name'))
                    doc.measurement_name = attribute.measurement_name
                if (attribute.hasOwnProperty('sensor_kind'))
                    doc.sensor_kind = attribute.sensor_kind
                if (attribute.hasOwnProperty('measurement_unit'))
                    doc.measurement_unit = attribute.measurement_unit
                if (attribute.hasOwnProperty('measurement_timestamp')) {
                    doc['measurement_timestamp'] = attribute.measurement_timestamp;
                    //log.info(`${attribute.name} has a timestamp ${attribute.timestamp}`);
                    log.info(`Feeding sensor value: ${index}/${sensor.entity_id}.${attribute.measurement_id} @ RT ${received_time} AT ${attribute.measurement_timestamp} =`, JSON.stringify(attrVal));
                } else
                    log.info(`Feeding sensor value: ${index}/${sensor.entity_id}.${attribute.measurement_id} @ RT ${received_time} =`, JSON.stringify(attrVal));

                bulkBody.push({
                    index: {
                        _index: index,
                        _type: attrType
                    }
                });
                bulkBody.push(doc);

                //doc['servicePath'] = sensor.servicePath;
                bulkBodyGlobal.push({
                    index: {
                        _index: this.orionConfig.service,
                        _type: attrType
                    }
                });
                bulkBodyGlobal.push(doc);
            }
        }

        if (bulkBody.length > 0) {
            try {
                await this.es.bulk({ body: bulkBody },
                    function (err, resp) {

                        if ( err || (
                            resp.hasOwnProperty('errors') && 
                            resp.errors === true) )
                            log.info(`Error happened during bulk operation:`,
                                JSON.stringify(err));
                        else
                            log.info(`Bulk operation executed successfully.`,
                                JSON.stringify(resp));
                    });

                await this.es.bulk({ body: bulkBodyGlobal },
                    function (err, resp) {
                        if (err)
                            log.info(`Error happened during bulk operation.`, JSON.stringify(err),
                                JSON.stringify(resp));
                    });
            } catch (err) {
                log.error("ERROR in bulk operation", JSON.stringify(err));
            }
        }
    }

    async filterSensors(sensors, servicePaths) {
        try {
            const filter = this.conf.filter || {};
            const idsSet = filter.ids ? new Set(filter.ids) : null;
            const typesSet = filter.types ? new Set(filter.types) : null;
            const attributesSet = filter.attributes ? new Set(filter.attributes) : null;
            const results = [];
            let attrVal;
            //let spIndex = -1;
            for (const sensor of sensors) {
                //spIndex++;
                if ((!idsSet || idsSet.has(sensor.id))
                    && (!typesSet || typesSet.has(sensor.type))) {
                    const attributes = [];
                    //build attributes part
                    // !excludedAttributes.has(attrName) &&
                    for (const attrId in sensor) {
                        if (sensor[attrId].hasOwnProperty('type') &&
                            sensor[attrId].type === 'Measurement' &&
                            (!attributesSet || attributesSet.has(attrId))) {

                            if (sensor[attrId].hasOwnProperty('value'))
                                attrVal = sensor[attrId].value;
                            else
                                attrVal = 'NA'
                            //log.info(`attrName value: ${attrName} ${attrVal}`);

                            let attr = {
                                measurement_id: attrId,
                                type: 'number',//sensor[attrName].value.type,
                                value: attrVal
                            }

                            if (sensor[attrId].hasOwnProperty('metadata')) {
                                if (sensor[attrId].metadata.hasOwnProperty('timestamp'))
                                    attr['measurement_timestamp'] = sensor[attrId].metadata.timestamp.value
                                if (sensor[attrId].metadata.hasOwnProperty('dimension'))
                                    attr['measurement_dimension'] = sensor[attrId].metadata.dimension.value
                                if (sensor[attrId].metadata.hasOwnProperty('name'))
                                    attr['measurement_name'] = sensor[attrId].metadata.name.value
                                if (sensor[attrId].metadata.hasOwnProperty('sensor_kind'))
                                    attr['sensor_kind'] = sensor[attrId].metadata.sensor_kind.value
                                if (sensor[attrId].metadata.hasOwnProperty('unit'))
                                    attr['measurement_unit'] = sensor[attrId].metadata.unit.value
                                //if (sensor[attrId].metadata.hasOwnProperty(''))
                                //   attr[] = sensor[attrId].metadata..value
                            }

                            attributes.push(attr);
                        }
                    }

                    let doc = {
                        entity_id: sensor.id,
                        //servicePath: servicePaths[spIndex],
                        attributes
                    }

                    if (sensor.hasOwnProperty('domain'))
                        doc.domain = sensor.domain.value;
                    if (sensor.hasOwnProperty('name'))
                        doc.entity_name = sensor.name.value;
                    /*if (sensor.hasOwnProperty(''))
                        doc[] = sensor..value;
                    if (sensor.hasOwnProperty(''))
                        doc[] = sensor..value;*/

                    results.push(doc);
                }
            }
            return results;
        } catch (err) {
            log.error("ERROR in filterSensors", JSON.stringify(err));
            return [];
        }
    }

    generateIndex(domain) {
        let index = this.orionConfig.service;
        if (domain) {
            index = index.concat(domain);
        }

        return index.toLowerCase();
    }

    generateIndexSp(servicePath) {
        let index = this.orionConfig.service;
        if (servicePath !== '/') {
            const spPart = servicePath.replace(/\//g, "-");
            index = index.concat(spPart);
        } else
            index = index.concat('-root');

        return index.toLowerCase();
    }

    _getSubscriptionDesc() {
        return `Orion-Elasticsearch Feeder instance ${this.orionConfig.service} ${config.get('endpoint.id')}`;
    }

    async doPeriod() {
        await this.orion.unsubscribe();
        const data = await this.orion.fetchSensors();
        const servicePaths = data.map(entity => entity.servicePath.value)
        //log.info(`doPeriod ${servicePaths}`);
        const sensors = await this.filterSensors(data, servicePaths);
        if (this.conf.trigger === TriggerTypes.Subscription) {
            const filter = this.conf.filter || {};
            await this.orion.subscribe(this._getSubscriptionDesc(), this.cid, config.get('endpoint.url'), filter);
        } else {
            await this.feedToElasticsearch(sensors);
        }
    }
}
