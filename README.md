Kafka Connect SMT to replace the incoming message with a tombstone (null value) based on a predicate specifying JSON Path condition matching the events' key or value.

Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`tombstoner.condition`| String specifying condition to match record to be replaced with tombstone | String |  | High |

Examples:
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

TODO - headers support?