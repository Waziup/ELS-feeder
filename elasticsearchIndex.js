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
                attribute_timestamp: {
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
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                attribute_timestamp: {
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
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                attribute_timestamp: {
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
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                attribute_timestamp: {
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
                name: {
                    type: 'keyword'
                },
                attribute: {
                    type: 'keyword'
                },
                attribute_timestamp: {
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