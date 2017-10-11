"use strict";

const Promise = require('bluebird');
const config = require('./config');
const rp = require('request-promise');
const elasticsearch = require('elasticsearch');
const helpers = require('./helpers');
const log = require('./log');
const shortid = require('shortid');

const delay = 500; // time between requests at start (in ms)

const excludedAttributes = new Set(['id', 'type', 'owner']);

const tasks = {};

const TriggerTypes = {
    Time: 'time',
    Subscription: 'subscription'
};

// do this based on task's index type
const body = {
    mappings: {
        sensingNumber: {
            properties: {
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                time: {
                    type: 'date'
                },
                value: {
                    type: 'double'
                }
            }
        },
        sensingGeo: {
            properties: {
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                time: {
                    type: 'date'
                },
                geo: {
                    type: 'geo_point'
                }
            }
        },
        sensingDate: {
            properties: {
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                time: {
                    type: 'date'
                },
                date: {
                    type: 'date'
                }
            }
        },
        sensingText: {
            properties: {
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                time: {
                    type: 'date'
                },
                text: {
                    type: 'text'
                }
            }
        },
        sensingObject: {
            properties: {
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                time: {
                    type: 'date'
                },
                object: {
                    type: 'object'
                }
            }
        },
    }
}

class Task {
    constructor(conf) {
        this.conf = conf;
        this.subscriptionId = 0;
        this.orionConfig = config.mergeWith(conf.orion, 'orion');

        this.esConfig = config.mergeWith(conf.elasticsearch, 'elasticsearch');
        this.es = new elasticsearch.Client({
            host: `${this.esConfig.host}:${this.esConfig.port}`
            // , log: 'trace'
        });

        this.indexExists = new Map();
        this.cid = shortid.generate();
    }

    async init() {
        // Discard all existing subscriptions that could relate to this task (or some other from this instance of feeder)
        const expectedDesc = this._getSubscriptionDesc();
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/subscriptions`,
                headers: {
                    'Fiware-Service': this.orionConfig.service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                json: true
            });

            for (const entry of resp) {
                if (entry.description === expectedDesc) {
                    const resp = await rp({
                        method: 'DELETE',
                        uri: `${this.orionConfig.uri}/v2/subscriptions/${entry.id}`,
                        headers: {
                            'Fiware-Service': this.orionConfig.service,
                            'Fiware-ServicePath': this.orionConfig.servicePath
                        },
                        json: true
                    });
                }
            }
        } catch (err) {
            log.error(err);
        }
    }

    async createIndexes() {
        // Create index in Elasticsearch if it does not exist yet
        let allSps = await this.fetchSps();
        for (let sp of allSps) {
            let indexName = this.getIndex(sp);
            if (this.indexExists.has(indexName) === false) {
                log.info('Creating/updating index for', indexName);
                let indexExists = await this.es.indices.exists({ index: indexName });
                this.indexExists.set(indexName, true);
                if (!indexExists) {
                    log.info('Creating index for ', indexName);
                    await this.es.indices.create({
                        index: indexName,
                        body: body
                    })
                } else {
                    log.info('Updating mappings of index for ', indexName);
                    // do this based on task's index type
                    for (let mapType in body.mappings) {
                        log.info(mapType, body.mappings[mapType]);
                        const ret = await this.es.indices.putMapping({
                            index: indexName,
                            body: body.mappings[mapType],
                            type: mapType
                        });
                        log.info('putMapping operation: ', JSON.stringify(ret));
                    }
                }
            }
        }
    }

    async fetchSps() {
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/entities?attrs=servicePath`,
                headers: {
                    'Fiware-Service': this.orionConfig.service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                json: true
            });

            const spSet = new Set();

            for (const entry of resp) {
                spSet.add(entry.servicePath.value);
            }

            //console.log(spSet);
            return spSet;
        } catch (err) {
            log.error(err);
            return [];
        }
    }

    async fetchSensors() {
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/entities?attrs=dateModified,servicePath,*`,
                headers: {
                    'Fiware-Service': this.orionConfig.service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                json: true
            });

            return resp;
        } catch (err) {
            log.error(err);
            return [];
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
        await this.createIndexes();
        // do this based on task's index type
        for (const sensor of sensors) {
            index = this.getIndex(sensor.servicePath);

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
                    //FIXME: might be needed 
                }

                if (!!attribute.timestamp) {
                    timestamp = attribute.timestamp;
                    log.info('attribute.timestamp', attribute.timestamp);
                }

                else {
                    timestamp = sensor.dateModified
                    log.info('sensor.dateModified', timestamp);
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

                /*console.log(JSON.stringify({
                    name: sensor.name,
                    attribute: attribute.name,
                    time: timestamp,
                    [value]: attrVal
                }));*/
            }
        }

        if (bulkBody.length > 0) {
            await this.es.bulk({ body: bulkBody },
                function (err, resp) {
                    if (!!err)
                        log.info(`Error happened during bulk operation.`, JSON.stringify(err),
                            JSON.stringify(resp));
                    /*else
                        log.info(`Bulk operation executed successfully.`,
                            JSON.stringify(resp));*/
                });
        }
    }

    async filterSensors(data) {
        try {
            const filter = this.conf.filter || {};
            const idsSet = filter.ids ? new Set(filter.ids) : null;
            const attributesSet = filter.attributes ? new Set(filter.attributes) : null;
            const results = [];
            let attrVal;

            for (const entry of data) {
                if (!idsSet || idsSet.has(entry.id)) {
                    const attributes = [];

                    //build attributes part
                    for (const attrName in entry) {
                        if (!excludedAttributes.has(attrName) &&
                            (!attributesSet || attributesSet.has(attrName))) {

                            attrVal = entry[attrName].value;

                            /*if (attrName === 'farmingAction')
                                attrVal = attrVal.concat(' Quantity: ' +
                                    entry[attrName].metadata.quantity.value
                                    + ', Description: ' + entry[attrName].metadata.description.value);*/

                            if (!!entry[attrName].metadata
                                && !!entry[attrName].metadata.timestamp)
                                attributes.push({
                                    name: attrName,
                                    type: entry[attrName].type,
                                    value: attrVal,
                                    timestamp: entry[attrName].metadata.timestamp.value
                                });
                            else
                                attributes.push({
                                    name: attrName,
                                    type: entry[attrName].type,
                                    value: attrVal
                                });
                        }
                    }

                    let sp;
                    if (!!entry.servicePath)
                        sp = entry.servicePath.value;

                    let dateModified;
                    if (!!entry.dateModified)
                        dateModified = entry.dateModified.value;
                    else { //if there is no data update time.
                        log.info('dateModified does not exist', JSON.stringify(entry.dateModified));
                        dateModified = new Date();
                        dateModified = dateModified.toISOString();
                    }

                    results.push({
                        name: entry.id,
                        servicePath: sp,
                        dateModified: dateModified,
                        attributes
                    });
                }
            }

            return results;

        } catch (err) {
            log.error(err);
            return [];
        }
    }

    getIndex(servicePath) {
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

    subscribe(sensors) {
        const entities = sensors.map(sensor => {
            return {
                id: sensor.name
            };
        });

        log.info(`Subscribing to entities: ${this.orionConfig.service} ${this.orionConfig.servicePath} ${entities.map(entity => entity.id).join(', ')}`);

        const sub = {
            description: this._getSubscriptionDesc(),
            subject: {
                entities
            },
            notification: {
                http: {
                    url: `${config.get('endpoint.url')}/api/update/${this.cid}`
                }
            }
        };

        if (this.conf.throttling) {
            sub.throttling = this.conf.throttling;
        }

        return new Promise(resolve => {
            rp({
                method: 'POST',
                uri: `${this.orionConfig.uri}/v2/subscriptions`,
                headers: {
                    'Fiware-Service': this.orionConfig.service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                body: sub,
                json: true
            }, (err, msg, body) => {
                if (err) {
                    log.error(err);
                } else {
                    if (!msg.headers.location) {
                        log.error('Subscription failed.')
                    } else {
                        this.subscriptionId = msg.headers.location.replace(/.*v2\/subscriptions\/(.*)/, '$1');
                    }
                }
                resolve();
            });
        });
    }

    async unsubscribe() {
        try {
            const resp = await rp({
                method: 'DELETE',
                uri: `${this.orionConfig.uri}/v2/subscriptions/${this.subscriptionId}`,
                headers: {
                    'Fiware-Service': this.orionConfig.service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                json: true
            });
        } catch (err) {
            log.error(err);
        }
    }

    async doPeriod() {
        if (this.subscriptionId) {
            await this.unsubscribe();
        }

        const data = await this.fetchSensors();
        const sensors = await this.filterSensors(data);
        if (this.conf.trigger === TriggerTypes.Subscription) {
            await this.subscribe(sensors);
        } else {
            await this.feedToElasticsearch(sensors);
        }
    }
}

async function feedData(taskCid, data) {
    const task = tasks[taskCid];
    const sensors = await task.filterSensors(data);
    await task.feedToElasticsearch(sensors);
}

async function run() {
    const taskConfs = config.get('tasks');

    /* http://handlebarsjs.com/
     const hbs = require('handlebars');
    
    const tmpl = hbs.compile(config.elasticsearch.index);
    
    const vars = {
      servicePath: ....
      day: .... // 01 - 31
      month: ... // 01 - 12
      year: ... // YYYY
    };
    
    tmpl(vars);
    */
    for (const conf of taskConfs) {
        const task = new Task(conf);
        await task.init();
        tasks[task.cid] = task;
    }

    let accumulatedDelay = 0;
    for (const taskCid in tasks) {
        const task = tasks[taskCid];
        setTimeout(async () => {
            await task.doPeriod();
            if (task.conf.period) {
                setInterval(() => task.doPeriod(), task.conf.period);
            }
        }, accumulatedDelay);

        accumulatedDelay += delay;
    }
}

module.exports.run = run;
module.exports.feedData = feedData;