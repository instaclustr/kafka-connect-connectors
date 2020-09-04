# Instaclustr Kafka Connect Connectors


## How to build

Executing 
 
```
mvn clean package
```

at the top level will build all connectors within the project and output separate jars in their respective target folders.

It will also generate an uber jar at the top level target directory which will contain all connectors and their dependencies.

If you want to just build one connector, run the same command from within the sub module.


## Versioning

#### Connector Version

Each individual connector (sub module) version is defined in accordance to it's pom.

#### Distribution Version

This defines the version of the uber jar that consolidates all selected connectors in to one self contained jar.

A tag is used to mark a distribution and it should be in sync with the distribution.xml version.


## Further information and Documentation

For connector documentation please see https://www.instaclustr.com/support/documentation/kafka-connect/pre-built-kafka-connect-plugins/

For Instaclustr support status of this project please see https://www.instaclustr.com/support/documentation/announcements/instaclustr-open-source-project-status/

## License

Apache2 - See the included License file for more details.
