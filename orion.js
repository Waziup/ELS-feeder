const log = require('./log');
const rp = require('request-promise');

module.exports = class Orion {
    constructor(conf) {
        this.subscriptionId = 0;
        this.orionConfig = conf;
    }

    async deleteSubscription(id) {
        const resp = await rp({
            method: 'DELETE',
            uri: `${this.orionConfig.uri}/v2/subscriptions/${id}`,
            headers: {
                'Fiware-Service': this.orionConfig.service,
                'Fiware-ServicePath': this.orionConfig.servicePath
            },
            json: true
        });
        return resp;
    }

    async getSubscriptions() {
        const resp = await rp({
            uri: `${this.orionConfig.uri}/v2/subscriptions`,
            headers: {
                'Fiware-Service': this.orionConfig.service,
                'Fiware-ServicePath': this.orionConfig.servicePath
            },
            json: true
        });

        return resp;
    }

    generateIndex(servicePath) {
        let index = this.orionConfig.service;
        if (servicePath !== '/') {
            const spPart = servicePath.replace(/\//g, "-");
            index = index.concat(spPart);
        }
        return index.toLowerCase();
    }

    async fetchServicePaths() {
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/entities/?limit=1000&attrs=servicePath`,
                headers: {
                    'Fiware-Service': this.orionConfig.service,
                    'Fiware-ServicePath': this.orionConfig.servicePath
                },
                json: true
            });

            //log.info('resp', JSON.stringify(resp));
            let spSet = new Set();

            for (const entry of resp) {
                let sp = this.generateIndex(entry.servicePath.value)
                //log.info('sp', JSON.stringify(sp), sp);
                spSet.add(sp);
            }

            //log.info('spSet', spSet);

            return spSet;
        } catch (err) {
            log.error(err);
            return [];
        }
    }

    async fetchSensors() {
        try {
            const resp = await rp({
                uri: `${this.orionConfig.uri}/v2/entities?limit=1000&attrs=dateModified,servicePath,*`,
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

    subscribe(description, cid, endpointUrl, filter) {
        const entities = [];
        /*id or idPattern: Id or pattern of the affected entities. 
        Both cannot be used at the same time, but at least one of them must 
        be present.type or typePattern: Type or type pattern of the affected 
        entities. Both cannot be used at the same time. If omitted, it means 
        "any entity type".
        */
        
        //type pattern filter
        if (filter.types) {
            const typesSet = new Set(filter.types);
            let typPattern = "";
            let or = ''
            for (const typ of typesSet) {
                typPattern = typPattern.concat(or, typ);
                or = '|'
            }
            entities.push({
                "typePattern": typPattern,
                "idPattern": ".*"
            });
        }

        //id filter
        if (filter.ids) {
            const idsSet = new Set(filter.ids);
            //const ids = []
            for(const id of idsSet)
                entities.push({id: id});
            //entities.push(ids);
        } else if (filter.hasOwnProperty('types') === false) { 
            //otherwise, apply idPattern for all entities
            entities.push({
                "idPattern": ".*"
            });
        }

        log.info(`Subscribing to entities: ${this.orionConfig.service} 
            ${this.orionConfig.servicePath} `, JSON.stringify(entities));

        const sub = {
            description: description,
            subject: {
                entities
            },
            notification: {
                http: {
                    url: `${endpointUrl}/api/update/${cid}`
                }
            }
        };

        if (this.orionConfig.throttling) {
            sub.throttling = this.orionConfig.throttling;
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

    subscribe0(sensors, description, cid, endpointUrl) {
        const entities = sensors.map(sensor => {
            return {
                id: sensor.name
            };
        });

        log.info(`Subscribing to entities: ${this.orionConfig.service} ${this.orionConfig.servicePath} ${entities.map(entity => entity.id).join(', ')}`);

        const sub = {
            description: description,
            subject: {
                entities
            },
            notification: {
                http: {
                    url: `${endpointUrl}/api/update/${cid}`
                }
            }
        };

        if (this.orionConfig.throttling) {
            sub.throttling = this.orionConfig.throttling;
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
        if (this.subscriptionId) {
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
    }

}