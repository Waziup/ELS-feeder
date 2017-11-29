// do this based on task's index type
const body = {
    mappings: {
        sensingNumber: {
            properties: {
                entity_id: {
                    type: 'keyword'
                },
                measurement_id: {
                    type: 'keyword'
                },
                measurement_timestamp: {
                    type: 'date'
                },
                time: {
                    type: 'date'
                },
                value: {
                    type: 'double'
                }, 
                servicePath: {
                    type: 'keyword'
                }
            }
        },
        sensingGeo: {
            properties: {
                entity_id: {
                    type: 'keyword'
                },
                measurement_id: {
                    type: 'keyword'
                },
                measurement_timestamp: {
                    type: 'date'
                },
                time: {
                    type: 'date'
                },
                geo: {
                    type: 'geo_point'
                }, 
                servicePath: {
                    type: 'keyword'
                }
            }
        },
        sensingText: {
            properties: {
                entity_id: {
                    type: 'keyword'
                },
                measurement_id: {
                    type: 'keyword'
                },
                measurement_timestamp: {
                    type: 'date'
                },
                time: {
                    type: 'date'
                },
                text: {
                    type: 'text'
                }, 
                servicePath: {
                    type: 'keyword'
                }
            }
        },
        sensingObject: {
            properties: {
                entity_id: {
                    type: 'keyword'
                },
                measurement_id: {
                    type: 'keyword'
                },
                measurement_timestamp: {
                    type: 'date'
                },
                time: {
                    type: 'date'
                },
                object: {
                    type: 'object'
                }, 
                servicePath: {
                    type: 'keyword'
                }
            }
        },
        sensingDate: {
            properties: {
                entity_id: {
                    type: 'keyword'
                },
                measurement_id: {
                    type: 'keyword'
                },
                measurement_timestamp: {
                    type: 'date'
                },
                time: {
                    type: 'date'
                },
                date: {
                    type: 'date'
                }, 
                servicePath: {
                    type: 'keyword'
                }
            }
        }
    }
}

module.exports = {body}