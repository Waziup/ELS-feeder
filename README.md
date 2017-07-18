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