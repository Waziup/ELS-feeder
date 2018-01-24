
function addMappingType(field) {
    const fields = {
        entity_id: {
            type: 'keyword'
        },
        entity_name: {
            type: 'keyword'
        },
        measurement_id: {
            type: 'keyword'
        },
        measurement_timestamp: {
            type: 'date'
        },
        measurement_dimension: {
            type: 'keyword'
        },
        measurement_name: {
            type: 'keyword'
        },
        sensor_kind: {
            type: 'keyword'
        },
        measurement_unit: {
            type: 'keyword'
        },
        received_time: {
            type: 'date'
        },
        domain: {
            type: 'keyword'
        }
    }

    return Object.assign({}, fields, field);
}

// do this based on task's index type
const body = {
    mappings: {
        sensingNumber: {
            properties: addMappingType({
                measurement_value: {
                    type: 'double'
                }
            })
        },
        sensingGeo: {
            properties: addMappingType({
                measurement_geo: {
                    type: 'geo_point'
                }
            })
        },
        sensingText: {
            properties: addMappingType({
                measurement_text: {
                    type: 'keyword'
                }
            })
        },
        sensingObject: {
            properties: addMappingType({
                measurement_object: {
                    type: 'object'
                }
            })
        },
        sensingDate: {
            properties: addMappingType({
                measurement_date: {
                    type: 'date'
                }
            })
        }
    }
}

module.exports = { body }