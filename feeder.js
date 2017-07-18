"use strict";

const Promise = require('bluebird');
const config = require('./config');
const rp = require('request-promise');
const elasticsearch = require('elasticsearch');
const helpers = require('./helpers');
const log = require('./log');
const shortid = require('shortid');

const delay = 500; // time between requests at start (in ms)

const excludedAttributes = new Set(['id', 'type']);

const tasks = {};

const TriggerTypes = {
    Time: 'time',
    Subscription: 'subscription'
};

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

        this.cid = shortid.generate();
    }

    async init() {
        // Create index in Elasticsearch if it does not exist yet
        //const indexExists = await this.es.indices.exists({index: this.esConfig.index});
        console.log(this.conf.orion.service);
        for(let service of this.conf.orion.service) {
            console.log(service);
            let allSps = await this.fetchSps(service);
            for(let sp of allSps) {
                console.log(sp);
                let indexName = this.getIndex(service, sp);
                const indexExists = await this.es.indices.exists({ index: indexName });

                if (!indexExists) {
                    await this.es.indices.create({
                        index: indexName,
                        body: {
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
                                }
                            }
                        }
                    })
                }
            }

            //based on indexName
            // Discard all existing subscriptions that could relate to this task (or some other from this instance of feeder)
            const expectedDesc = this._getSubscriptionDesc(service);
            try {
                const resp = await rp({
                    uri: `${this.orionConfig.uri}/v2/subscriptions`,
                    headers: {
                        'Fiware-Service': service,
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
                                'Fiware-Service': service,
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
    }

    async fetchSps(service) {
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/entities?attrs=servicePath`,
                headers: {
                    'Fiware-Service': service,
                    'Fiware-ServicePath': '/#'
                },
                json: true
            });

            const spSet = new Set();

            for (const entry of resp) {
                spSet.add(entry.servicePath.value);
            }

            console.log(spSet);
            return spSet;
        } catch (err) {
            log.error(err);
            return [];
        }
    }

    async fetchSensors(service) {
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/entities?attrs=dateModified,servicePath,*`,
                headers: {
                    'Fiware-Service': service,
                    'Fiware-ServicePath': '/#'
                },
                json: true
            });

            return resp;
        } catch (err) {
            log.error(err);
            return [];
        }
    }

    async filterSensors(data) {
        try {
            const filter = this.conf.filter || {};
            const idsSet = filter.ids ? new Set(filter.ids) : null;
            const attributesSet = filter.attributes ? new Set(filter.attributes) : null;

            const results = [];

            for (const entry of data) {
                if (!idsSet || idsSet.has(entry.id)) {
                    const attributes = [];

                    for (const attrName in entry) {
                        if (!excludedAttributes.has(attrName) && (!attributesSet || attributesSet.has(attrName))) {
                            attributes.push({
                                name: attrName,
                                type: entry[attrName].type,
                                value: entry[attrName].value
                            });
                        }
                    }

                    results.push({
                        name: entry.id,
                        servicePath: entry.servicePath.value,
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

    getIndex(service, servicePath) {
        let index = service;
        if (servicePath !== '/') {
            const spPart = servicePath.replace(/\//g, "-");
            index = index.concat(spPart);
        }
        return index.toLowerCase();
    }

    async feedToElasticsearch(service, sensors) {
        const docTime = new Date();
        const bulkBody = [];

        for (const sensor of sensors) {
            let index = this.getIndex(service, sensor.servicePath);

            for (const attribute of sensor.attributes) {
                if (attribute.type === 'Number') {
                    log.info(`Feeding sensor number value: ${service} ${sensor.name}.${attribute.name} @ ${docTime} = ${attribute.value}`);

                    bulkBody.push({
                        index: {
                            _index: index,
                            _type: 'sensingNumber'
                        }
                    });

                    bulkBody.push({
                        name: sensor.name,
                        attribute: attribute.name,
                        time: docTime.getTime(),
                        value: attribute.value
                    });

                } else {
                    log.error(`Unsupported attribute type: ${attribute.type} in ${service} ${sensor.name}.${attribute.name}`);
                }
            }
        }

        if (bulkBody.length > 0) {
            await this.es.bulk({ body: bulkBody });
        }
    }

    _getSubscriptionDesc(service) {
        return `Orion-Elasticsearch Feeder instance ${service} ${config.get('endpoint.id')}`;
    }

    subscribe(service, sensors) {
        const entities = sensors.map(sensor => {
            return {
                id: sensor.name
            };
        });

        log.info(`Subscribing to entities: ${service} ${this.orionConfig.servicePath} ${entities.map(entity => entity.id).join(', ')}`);

        const sub = {
            description: this._getSubscriptionDesc(service),
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

        console.log(sub);

        return new Promise(resolve => {
            rp({
                method: 'POST',
                uri: `${this.orionConfig.uri}/v2/subscriptions`,
                headers: {
                    'Fiware-Service': service,
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

    async unsubscribe(service) {
        try {
            const resp = await rp({
                method: 'DELETE',
                uri: `${this.orionConfig.uri}/v2/subscriptions/${this.subscriptionId}`,
                headers: {
                    'Fiware-Service': service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                json: true
            });
        } catch (err) {
            log.error(err);
        }
    }

    async doPeriod() {
        for(let service of this.conf.orion.service) {

            if (this.subscriptionId) {
                await this.unsubscribe(service);
            }
            
            const data = await this.fetchSensors(service);
            const sensors = await this.filterSensors(data);
            if (this.conf.trigger === TriggerTypes.Subscription) {
                await this.subscribe(service, sensors);
            } else {
                await this.feedToElasticsearch(service, sensors);
            }
        }
    }
}

async function feedData(taskCid, data) {
    const task = tasks[taskCid];
    for(let service of task.service) {
        const sensors = await task.filterSensors(data);
        await task.feedToElasticsearch(service, sensors);
    }
}

async function run() {
    const taskConfs = config.get('tasks');

    //log.info(`Run method: taskConfs ${taskConfs}`);
    for (const conf of taskConfs) {
        const task = new Task(conf);
        await task.init();
        tasks[task.cid] = task;
    }

    //log.info(`Run method: tasks ${tasks}`);

    let accumulatedDelay = 0;
    for (const taskCid in tasks) {
        const task = tasks[taskCid];
        //log.info(`Run method: task ${task} taskCid ${taskCid}`);
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
