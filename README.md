curl 'http://broker.waziup.io/v2/entities?attrs=dateModified,dateCreated,servicePath,*' --header 'Fiware-ServicePath:/#, /Calci/#' --header 'Fiware-Service: watersense' -X GET | jq "."


  {
    "id": "WS_Calci_Sensor7",
    "type": "SensingDevice",
    "SM1": {
      "type": "Number",
      "value": 1002,
      "metadata": {}
    },
    "SM2": {
      "type": "Number",
      "value": 1002,
      "metadata": {}
    },
    "dateCreated": {
      "type": "DateTime",
      "value": "2017-07-18T07:05:55.00Z",
      "metadata": {}
    },
    "dateModified": {
      "type": "DateTime",
      "value": "2017-07-18T07:05:55.00Z",
      "metadata": {}
    },
    "servicePath": {
      "type": "Text",
      "value": "/Calci/TESTS",
      "metadata": {}
    }
  }

curl 'http://broker.waziup.io/v2/entities?attrs=servicePath' --header 'Fiware-ServicePath:/#' --header 'Fiware-Service:waziup' -X GET | jq "."

  {
    "id": "Sensor8",
    "type": "SensingDevice",
    "servicePath": {
      "type": "Text",
      "value": "/ISPACE/WEATHER",
      "metadata": {}
    }
  },
  {
    "id": "testCDU",
    "type": "SensingDevice",
    "servicePath": {
      "type": "Text",
      "value": "/",
      "metadata": {}
    }
  },
  {
    "id": "SensorTestAbdur",
    "type": "SensingDevice",
    "servicePath": {
      "type": "Text",
      "value": "/",
      "metadata": {}
    }
  }
]


curl 'http://broker.waziup.io/v2/entities?attrs=servicePath' --header 'Fiware-ServicePath:/#' --header 'Fiware-Service:watersense' -X GET | jq "[.[].servicePath.value] | unique"

[
  "/FARM1/TESTS",
  "/SSF/TESTS",
  "/UPPA/TESTS"
]



    /*  tasks:
      - trigger: time
        period: 300000
        orion:
          service: 
          - watersense
          - waziup
  
      curl 'http://broker.waziup.io/v2/entities?attrs=servicePath' 
      --header 'Fiware-ServicePath:/#' --header 'Fiware-Service:waziup' -X GET
      this.orionConfig.servicePath
      attrs=dateModified,dateCreated,servicePath,*
      */


      error: Unsupported attribute type: DateTime in waziup Sensor30.dateModified
error: Unsupported attribute type: Text in waziup Sensor30.servicePath
error: Unsupported attribute type: geo:json in waziup c4a_sensor444.location
error: Unsupported attribute type: string in waziup c4a_sensor444.owner
error: Unsupported attribute type: DateTime in waziup c4a_sensor444.dateModified
error: Unsupported attribute type: Text in waziup c4a_sensor444.servicePath
error: Unsupported attribute type: geo:json in waziup c4a_sensor555.location
error: Unsupported attribute type: string in waziup c4a_sensor555.owner
error: Unsupported attribute type: DateTime in waziup c4a_sensor555.dateModified
error: Unsupported attribute type: Text in waziup c4a_sensor555.servicePath
error: Unsupported attribute type: geo:json in waziup c4a_sensor46.location
error: Unsupported attribute type: String in waziup c4a_sensor46.owner
error: Unsupported attribute type: DateTime in waziup c4a_sensor46.dateModified
error: Unsupported attribute type: Text in waziup c4a_sensor46.servicePath
error: Unsupported attribute type: geo:json in waziup c4a_sensor47.location
error: Unsupported attribute type: string in waziup c4a_sensor47.owner
error: Unsupported attribute type: DateTime in waziup c4a_sensor47.dateModified
error: Unsupported attribute type: Text in waziup c4a_sensor47.servicePath
info: Feeding sensor number value: waziup Sensor8.AZO @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = -1074730000
info: Feeding sensor number value: waziup Sensor8.DO @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = -999
info: Feeding sensor number value: waziup Sensor8.HU @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = 998
info: Feeding sensor number value: waziup Sensor8.LU @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = 3.48
info: Feeding sensor number value: waziup Sensor8.RC @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = 43.31
info: Feeding sensor number value: waziup Sensor8.TC @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = 998
info: Feeding sensor number value: waziup Sensor8.WC @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = 0
info: Feeding sensor number value: waziup Sensor8.WD @ Tue Jul 18 2017 15:58:38 GMT+0200 (CEST) = -1
error: Unsupported attribute type: geo:json in waziup Sensor8.location
error: Unsupported attribute type: DateTime in waziup Sensor8.dateModified
error: Unsupported attribute type: Text in waziup Sensor8.servicePath
error: Unsupported attribute type: geo:json in waziup testCDU.location
error: Unsupported attribute type: String in waziup testCDU.owner
error: Unsupported attribute type: DateTime in waziup testCDU.dateModified
error: Unsupported attribute type: Text in waziup testCDU.servicePath
error: Unsupported attribute type: geo:json in waziup SensorTestAbdur.location
error: Unsupported attribute type: String in waziup SensorTestAbdur.owner
error: Unsupported attribute type: DateTime in waziup SensorTestAbdur.dateModified
error: Unsupported attribute type: Text in waziup SensorTestAbdur.servicePath


dateModified DateTime
location geo:json
owner String
servicePath Text

                
/*
dateModified DateTime
location geo:json
owner String
servicePath Text*/