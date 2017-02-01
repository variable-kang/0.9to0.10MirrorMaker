Kafka v0.9 Broker to v0.10 Broker MirrorMaker
==============
This enables mirroring between different Broker versions.
A mirrored version of the 0.10 broker can use all of the features in version 0.10.

Installation
--------------
#### git clone https://github.com/variable-kang/0.9to0.10MirrorMaker.git
#### mvn clean && mvn package

Start
--------------
It is used in the same way as existing mirror maker.

#### Ex. DIR_PATH/bin/mirrormaker-start.sh -daemon -loggc -name mirror-example --producer.config DIR_PATH/conf/producer.properties --consumer.config DIR_PATH/conf/consumer.properties --whitelist test-topic
