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

    async fetchServicePaths() {
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

    subscribe(sensors, description, cid, endpointUrl) {
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