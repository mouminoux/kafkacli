# Kafkacli [![Build Status](https://travis-ci.org/mouminoux/kafkacli.svg?branch=master)](https://travis-ci.org/mouminoux/kafkacli)

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

Usage: kafkacli consume [--from-beginning] [-g] [-e] [-p] TOPIC...

consume and display messages from 1 or more topics
                         
Arguments:               
  TOPIC                  topic(s) to consume from
                         
Options:                 
  -p, --pretty-print     pretty print the messages
      --from-beginning   start with the earliest message
  -g, --consumer-group   consumer group id. If unset, a random one will be generated
  -e, --exit             exit when last message received
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
