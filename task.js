const Promise = require('bluebird');
const config = require('./config');
const body = require('./elasticsearchIndex.js');
const rp = require('request-promise');
const elasticsearch = require('elasticsearch');
const log = require('./log');
const shortid = require('shortid');
var Orion = require('./orion.js');

const excludedAttributes = new Set(['id', 'type', 'owner']);
const TriggerTypes = {
    Time: 'time',
    Subscription: 'subscription'
};

module.exports = class Task {
    constructor(conf) {
        this.conf = conf;
        this.orionConfig = config.mergeWith(conf.orion, 'orion');
        this.orion = new Orion(this.orionConfig);

        this.esConfig = config.mergeWith(conf.elasticsearch, 'elasticsearch');
        this.es = new elasticsearch.Client({
            host: `${this.esConfig.host}:${this.esConfig.port}`
            // , log: 'trace'
        });

        this.indexExists = new Map();
        this.cid = shortid.generate();
    }

    async init() {
        await this.createIndexes();
        // Discard all existing subscriptions that could relate to this task
        // (or some other from this instance of feeder)
        const expectedDesc = this._getSubscriptionDesc();
        try {
            const resp = await this.orion.getSubscriptions();
            for (const entry of resp) {
                if (entry.description === expectedDesc) {
                    await this.orion.deleteSubscription(entry.id);
                }
            }
        } catch (err) {
            log.error(err);
        }
    }

    // Create an index in Elasticsearch if it does not exist yet
    //This is to support automatic discovery of sensors and service paths
    async createIndexes() {
        //automatic discovery
        let allSps = await this.orion.fetchServicePaths();
        for (let indexName of allSps) {
            log.info('indexName:', JSON.stringify(indexName));
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
        const docTime = new Date();
        const bulkBody = [];
        let attrType;
        let attrVal;
        let value, timestamp;
        let index;
        // do this based on task's index type
        for (const sensor of sensors) {
            index = this.generateIndex(sensor.servicePath);
            await this.createIndexes();

            for (const attribute of sensor.attributes) {
                switch (attribute.type.toLowerCase()) {
                    case 'number': attrType = 'sensingNumber'; value = 'value'; attrVal = attribute.value; break;
                    case 'geo:json': attrType = 'sensingGeo'; value = 'geo'; attrVal = attribute.coordinates; break;
                    case 'string': attrType = 'sensingKeyword'; value = 'keyword'; attrVal = attribute.value; break;
                    case 'text':
                        attrType = 'sensingText';
                        value = 'text';
                        attrVal = attribute.value;
                        break;
                    case 'datetime': attrType = 'sensingDate'; value = 'date'; attrVal = attribute.value; break;
                    case 'object': attrType = 'sensingObject'; value = 'object'; attrVal = attribute.value; break;
                    default:
                        log.error(`Unsupported attribute type: ${attribute.type} in ${this.orionConfig.service} ${sensor.name}.${attribute.name}`);
                        continue;
                }

                if (!!attribute.timestamp) {
                    timestamp = attribute.timestamp;
                    log.info(`${attribute.name} has a timestamp ${attribute.timestamp}`);
                }
                else {
                    timestamp = sensor.dateModified
                    log.info(`${attribute.name} uses sensor.dateModified ${timestamp}`);
                }

                log.info(`Feeding sensor value: ${this.orionConfig.service}/${sensor.name}.${attribute.name} @ ${timestamp} =`, JSON.stringify(attrVal));
                //${docTime} dateModified: sensor.dateModified docTime.getTime()
                bulkBody.push({
                    index: {
                        _index: index,
                        _type: attrType
                    }
                });

                bulkBody.push({
                    name: sensor.name,
                    attribute: attribute.name,
                    time: timestamp,
                    [value]: attrVal
                });
            }
        }

        if (bulkBody.length > 0) {
            try {
                await this.es.bulk({ body: bulkBody },
                    function (err, resp) {
                        if (!!err)
                            log.info(`Error happened during bulk operation.`, JSON.stringify(err),
                                JSON.stringify(resp));
                        /*else
                            log.info(`Bulk operation executed successfully.`,
                                JSON.stringify(resp));*/
                    });
            } catch (err) {
                log.error("ERROR in bulk operation", err);
            }
        }
    }

    async filterSensors(sensors) {
        try {
            const filter = this.conf.filter || {};
            const idsSet = filter.ids ? new Set(filter.ids) : null;
            const typesSet = filter.types ? new Set(filter.types) : null;
            const attributesSet = filter.attributes ? new Set(filter.attributes) : null;
            const results = [];
            let attrVal;

            for (const sensor of sensors) {
                if ((!idsSet || idsSet.has(sensor.id))
                    && (!typesSet || typesSet.has(sensor.type))) {
                    const attributes = [];
                    //log.info('sensor id: ', sensor.id, sensor.type);
                    //build attributes part
                    for (const attrName in sensor) {
                        if (!excludedAttributes.has(attrName) &&
                            (!attributesSet || attributesSet.has(attrName))) {
                            
                            if(!!sensor[attrName].value)
                                attrVal = sensor[attrName].value;
                            else
                                attrVal = 'NA'
                            log.info('attrName value: ', attrName, attrVal);

                            if (!!sensor[attrName].metadata
                                && !!sensor[attrName].metadata.timestamp)
                                attributes.push({
                                    name: attrName,
                                    type: sensor[attrName].type,
                                    value: attrVal,
                                    timestamp: sensor[attrName].metadata.timestamp.value
                                });
                            else
                                attributes.push({
                                    name: attrName,
                                    type: sensor[attrName].type,
                                    value: attrVal
                                });
                        }
                    }

                    let sp;
                    if (!!sensor.servicePath)
                        sp = sensor.servicePath.value;
                    else {
                        log.info(`sensor ${sensor.id} does not have a sp`);
                        sp = "/"
                    }

                    let dateModified;
                    if (sensor.hasOwnProperty('dateModified'))
                        dateModified = sensor.dateModified.value;
                    else { //if there is no data update time at sensor level.
                        dateModified = new Date();
                        dateModified = dateModified.toISOString();
                        log.info(`${sensor.id} dateModified does not exist: nowDate `, dateModified, " sensor.dateModified:", JSON.stringify(sensor.dateModified));
                    }
                    if (sensor.hasOwnProperty('name'))
                        log.info(`sensor id ${sensor.id} has sensor name ${sensor.name.value}`)

                    //IMPORTANT id and name
                    results.push({
                        name: sensor.id,
                        servicePath: sp,
                        dateModified: dateModified,
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
        }
        return index.toLowerCase();
    }

    _getSubscriptionDesc() {
        return `Orion-Elasticsearch Feeder instance ${this.orionConfig.service} ${config.get('endpoint.id')}`;
    }

    async doPeriod() {
        //log.info(this.conf);        
        await this.orion.unsubscribe();
        const data = await this.orion.fetchSensors();
        const sensors = await this.filterSensors(data);
        if (this.conf.trigger === TriggerTypes.Subscription) {
            await this.orion.subscribe(sensors, this._getSubscriptionDesc(), this.cid, config.get('endpoint.url'));
        } else {
            await this.feedToElasticsearch(sensors);
        }
    }
}