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

module.exports = {body}