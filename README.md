# Kafka Connect Tombstoner SMT

Kafka Connect SMT to replace the incoming message with a tombstone (null value) based on a predicate specifying JSON Path condition matching the events' key or value.

## Installation:

Please follow the instructions provided in https://docs.confluent.io/platform/current/connect/transforms/custom.html#custom-transformations

## Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`tombstoner.condition`| String specifying condition to match record to be replaced with tombstone | String |  | High |

## Examples:
```
transforms=tombstoner
transforms.tombstoner.type=com.github.thecrazymonkey.kafka.connect.smt.Tombstoner$Value
transforms.tombstoner.condition="$.headers[?(@.operation == 'DELETE')]"
```

```
transforms=tombstoner
transforms.tombstoner.type=com.github.thecrazymonkey.kafka.connect.smt.Tombstoner$Key
transforms.tombstoner.condition="$.headers[?(@.operation == 'DELETE')]"
```

Inspired by Confluent® `Filter` SMT

Dependency (when deploying you need to supply the JsonPath jar file along with the SMT):
https://github.com/json-path/JsonPath

## Releasing

Change the version
```bash
mvn versions:set -DnewVersion=1.3.0-SNAPSHOT
```

Build the package

```bash
mvn clean package
```

TODO - headers support?
