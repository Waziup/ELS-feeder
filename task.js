const Promise = require('bluebird');
const config = require('./config');
const body = require('./elasticsearchIndex.js');
const rp = require('request-promise');
const elasticsearch = require('elasticsearch');
const log = require('./log');
const shortid = require('shortid');
var Orion = require('./orion.js');

const excludedAttributes = new Set(['id', 'type', 'owner', 'dateModified']);
const TriggerTypes = {
    Time: 'time',
    Subscription: 'subscription'
};

esOK = false;

module.exports = class Task {
    constructor(conf) {
        this.conf = conf;
        this.orionConfig = config.mergeWith(conf.orion, 'orion');
        this.orion = new Orion(this.orionConfig);

        this.esConfig = config.mergeWith(conf.elasticsearch, 'elasticsearch');

        this.indexExists = new Map();
        this.cid = shortid.generate();
        this.es = null;
        this.orionOK = false;


    }

    async init() {
        const id = setInterval(async () => {
            this.es = new elasticsearch.Client({
                host: `${this.esConfig.host}:${this.esConfig.port}`
                // , log: 'trace'
                , apiVersion: "6.0"
            });
            this.es.ping({
                requestTimeout: 3000,
            }, async function (error) {
                if (error) {
                    log.error('elasticsearch cluster is down!catch of task.init for elasticsearch:', error);
                    esOK = false;
                } else {
                    log.info('elasticsearch OK.');
                    esOK = true;
                    clearInterval(id);
                }
            });
        }, 3000 /*every minute*/);

        // Discard all existing subscriptions that could relate to this task
        // (or some other from this instance of feeder)
        const expectedDesc = this._getSubscriptionDesc();
        const id2 = setInterval(async () => {
            try {
                const resp = await this.orion.getSubscriptions();
                log.info('orion OK.');
                for (const entry of resp) {
                    if (entry.description === expectedDesc) {
                        log.info('Deleting subscription', entry.description)
                        await this.orion.deleteSubscription(entry.id);
                    }
                }
                this.orionOK = true;
                clearInterval(id2);
            } catch (err) { //OrionException_getSubscriptions
                log.error(err);
                this.orionOK = false;
            }
        }, 30000 /*every minute*/);
    }

    //Create an index in Elasticsearch if it does not exist yet
    //This is to support automatic discovery of sensors and service paths
    async createIndex(index) {
        //automatic discovery: has been moved to other parts: subscriptions pattern, and doPeriod
        if (esOK === false)
            throw 'esException';

        log.info('index:', JSON.stringify(index));
        if (this.indexExists.has(index) === false) {
            this.indexExists.set(index, true);
            log.info('Creating/updating an index for', index);
            let flag = false;
            try {
                flag = await this.es.indices.exists({ index: index });
            } catch (err) {
                log.error("ERROR in checking index", err);
            }

            if (!flag) {
                log.info('Creating an index for ', index);
                try {
                    await this.es.indices.create({
                        index: index,
                        body: body.mappings
                    });
                    /*updateAllTypes
                        Boolean — Whether to update the mapping for all fields with the same name across all types or not */
                } catch (err) {
                    log.error("ERROR in creating index", err);
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
                        log.error("ERROR in putMapping", err);
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
                        log.error("ERROR in creating index", err);
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
                            log.error("ERROR in putMapping", err);
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
        let value, attribute_timestamp;
        let index;

        // do this based on task's index type
        for (const sensor of sensors) {
            index = this.generateIndex(sensor.servicePath);
            try {
                await this.createIndex(index);
            } catch (err) {
                log.error(err);
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

                let received_time = new Date();
                received_time = received_time.toISOString();

                let doc = {
                    name: sensor.name,
                    attribute: attribute.name,
                    time: received_time,
                    [value]: attrVal
                }

                if (attribute.hasOwnProperty('attribute_timestamp')) {
                    attribute_timestamp = attribute.attribute_timestamp;
                    doc['attribute_timestamp'] = attribute_timestamp;
                    //log.info(`${attribute.name} has a timestamp ${attribute.timestamp}`);
                    log.info(`Feeding sensor value: ${index}/${sensor.name}.${attribute.name} @ RT ${received_time} AT ${attribute_timestamp} =`, JSON.stringify(attrVal));
                } else
                    log.info(`Feeding sensor value: ${index}/${sensor.name}.${attribute.name} @ RT ${received_time} =`, JSON.stringify(attrVal));

                bulkBody.push({
                    index: {
                        _index: index,
                        _type: attrType
                    }
                });
                bulkBody.push(doc);

                doc['servicePath'] = sensor.servicePath;
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
                        if (err)
                            log.info(`Error happened during bulk operation sensor index.`, JSON.stringify(err));
                        log.info('resp', JSON.stringify(resp));
                        /*else
                            log.info(`Bulk operation executed successfully.`,
                                JSON.stringify(resp));*/
                    });

                await this.es.bulk({ body: bulkBodyGlobal },
                    function (err, resp) {
                        if (err)
                            log.info(`Error happened during bulk operation waziup global index.`, JSON.stringify(err));
                        log.info('resp', JSON.stringify(resp));
                    });
            } catch (err) {
                log.error("ERROR in bulk operation", err);
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
            let spIndex = -1;
            for (const sensor of sensors) {
                spIndex++;
                if ((!idsSet || idsSet.has(sensor.id))
                    && (!typesSet || typesSet.has(sensor.type))) {
                    const attributes = [];
                    //build attributes part
                    for (const attrName in sensor) {
                        if (!excludedAttributes.has(attrName) &&
                            (!attributesSet || attributesSet.has(attrName))) {

                            if (sensor[attrName].hasOwnProperty('value'))
                                attrVal = sensor[attrName].value;
                            else
                                attrVal = 'NA'
                            //log.info(`attrName value: ${attrName} ${attrVal}`);

                            if (sensor[attrName].hasOwnProperty('metadata')
                                && sensor[attrName].metadata.hasOwnProperty('timestamp'))
                                attributes.push({
                                    name: attrName,
                                    type: sensor[attrName].type,
                                    value: attrVal,
                                    attribute_timestamp: sensor[attrName].metadata.timestamp.value
                                });
                            else
                                attributes.push({
                                    name: attrName,
                                    type: sensor[attrName].type,
                                    value: attrVal
                                });
                        }
                    }

                    //if there is no data update time at sensor level, use the
                    //receiving data time
                    /*let dateModified;
                    if (sensor.hasOwnProperty('dateModified'))
                        dateModified = sensor.dateModified.value;
                    else {
                        dateModified = new Date();
                        dateModified = dateModified.toISOString();
                        log.info(`${sensor.id} dateModified does not exist: nowDate `, dateModified, " sensor.dateModified:", JSON.stringify(sensor.dateModified));
                    }*/
                    /*if (sensor.hasOwnProperty('name'))
                        log.info(`sensor id ${sensor.id} has sensor name ${sensor.name.value}`)*/

                    //IMPORTANT id and name
                    results.push({
                        name: sensor.id,
                        servicePath: servicePaths[spIndex],
                        attributes
                    });
                }
            }
            return results;
        } catch (err) {
            log.error("ERROR in filterSensors", err);
            return [];
        }
    }

    generateIndex(servicePath) {
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
        try {
            await this.orion.unsubscribe();
            const data = await this.orion.fetchSensors();
            const servicePaths = data.map(entity => entity.servicePath.value)
            //log.info(`doPeriod ${servicePaths}`);
            const sensors = await this.filterSensors(data, servicePaths);
            if (this.conf.trigger === TriggerTypes.Subscription) {
                if (this.orionOK === false)
                    throw 'doPeriodException';

                const filter = this.conf.filter || {};
                try {
                    const ret = await this.orion.subscribe(this._getSubscriptionDesc(),
                        this.cid, config.get('endpoint.url'), filter);
                    return ret;
                } catch (err) {
                    log.error('catch of doPeriod:', err);
                    throw 'doPeriodException'
                }
            } else {
                await this.feedToElasticsearch(sensors);
            }
        } catch (err) {
            log.error('catch of doPeriod:', err);
            throw 'doPeriodException'
        }


    }
}