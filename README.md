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