## MQTT DB Nodes

The MQTT DB Nodes are a clone of the built in MQTT nodes modified to optionally persist
messages in a lightweight local database in case of a lost connection, crash or Node-RED restart.

### How it works

The mqtt client used by the node is configured to store incoming and outgoing messages using the NeDB database.
Messages are stored in the mqttdb directory in the Node-RED directory.  In there, for each
broker configuration node, here is a directory containing incoming and outgoing message collections.

To enable persistent storage for input and output messages, click on the appropriate check boxes.  You can change the *compaction interval time* to compact the internal database and remove deleted messages.

Database files for a connection are stored in the Node-RED directory under the *mqttdb* directory.  These can be safely removed after stopping Node-RED to clear unsent messages.  The database
used is [NeDB](https://github.com/louischatriot/nedb).

### Known Issues

1. When a broker connection node is configured to persist incoming messages, the messages can be corrupted.  We recommend only configuring outgoing persistence for now.

2. The database is similar to REDIS in that it maintains data in RAM and writes to a file in case it goes down.  Since it is RAM based, can only go so long storing messages before it may run into trouble.
