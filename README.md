# Kafkacli

Kafkacli is a consumer and producer client for Apache Kafka.

## Usage:
```
Usage: kafkacli consume [--from-beginning] [-g] [-e] [-p] [--filter...] TOPIC...

consume and display messages from 1 or more topics

Arguments:
  TOPIC                  topic(s) to consume from

Options:                 
  -p, --pretty-print     pretty print the messages
      --from-beginning   start with the earliest message
  -g, --consumer-group   consumer group id. If unset, a random one will be generated
  -e, --exit             exit when last message received
      --filter           filter incoming messages against a set of conditions. Syntax: <filter-type>:<filter-condition>.
                         The currently supported filters:
                         * header filter: syntax: "header:<key>=<value>", e.g. --filter header:correlation-id=ac123-fds456
```

The options listed above must be set before the command, e.g:

`kafkacli -b <broker> consume notification`

### Consume:

```
kafkacli consume --help

Usage: kafkacli consume [--from-beginning] [-g] [-e] [-p] [--filter...] TOPIC...

consume and display messages from 1 or more topics
                         
Arguments:               
  TOPIC                  topic(s) to consume from
                         
Options:                 
  -p, --pretty-print     pretty print the messages
      --from-beginning   start with the earliest message
  -g, --consumer-group   consumer group id. If unset, a random one will be generated
  -e, --exit             exit when last message received
      --filter           filter incoming messages against a set of conditions. Syntax: <filter-type>:<filter-condition>.
The currently supported filters:
* header filter: syntax: "header:<key>=<value>", e.g. --filter header:correlation-id=ac123-fds456

```

### Produce

```
kafkacli produce --help

Usage: kafkacli produce [-H...] [-m=<message|->] TOPIC...

produce a message into 1 or more topics
                  
Arguments:        
  TOPIC           topic(s) to consume from
                  
Options:          
  -H, --header    message header <key=value>
  -m, --message   message message
```

### Examples

#### Consumer

Read all messages from 'my-topic' topic:

    $ kafkacli -b localhost:9092 consume my-topic

Read all messages from 'my-topic' topic with consumer group 'my-group' then exit after last message:

    $ kafkacli -b localhost:9092 consume -g my-group -e my-topic

Read all messages from 'my-topic' topic and filter messages when header key1 equal value1:

    $ kafkacli -b localhost:9092 consume --filter header:key1=value1 my-topic
    
Use env var to define the default broker:

    $ KAFKACLI_BROKER=localhost:9092 kafkacli consume my-topic
    
Use SSL file to access secured broker:

    $ kafkacli --ssl-cafile ca.pem --ssl-keyfile service.key --ssl-certfile service.cert -b my-secured-broker consume my-topic
    
You can also define env variables:

    $ export KAFKACLI_BROKER=my-secured-broker
    $ export KAFKACLI_SSL_CA_FILE=ca.pem
    $ export KAFKACLI_SSL_CERT_FILE=service.cert
    $ export KAFKACLI_SSL_KEY_FILE=service.key
    $ kafkacli consume my-topic

#### Producer

Produce a message with 2 headers on 'my-topic' topic:

    $ kafkacli -b localhost:9092 produce -H key1=value1 -H key2=value2 -m "payload content" my-topic

Produce a message from a file with 1 header on 'my-topic' topic:

    $ cat payload.json | kafkacli -b localhost:9092 produce -H key1=value1 my-topic
