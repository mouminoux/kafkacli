# Kafkacli [![Build Status](https://travis-ci.org/mouminoux/kafkacli.svg?branch=master)](https://travis-ci.org/mouminoux/kafkacli)

Kafkacli is a consumer and producer client for Apache Kafka.

## Usage:
```
Usage: kafkacli [-b...] [--ssl-cafile] [--ssl-certfile] [--ssl-keyfile] COMMAND [arg...]

Kafkacli
                       
Options:               
  -b, --broker         brokers (default ["localhost:9092"])
  -s, --secure         use SSL
      --ssl-cafile     filename of CA file to use in certificate verification (env $KAFKACLI_SSL_CA_FILE)
      --ssl-certfile   filename of file in PEM format containing the client certificate (env $KAFKACLI_SSL_CERT_FILE)
      --ssl-keyfile    filename containing the client private key (env $KAFKACLI_SSL_KEY_FILE)
                       
Commands:              
  consume              consume and display messages from 1 or more topics
  produce              produce a message into 1 or more topics
                       
Run 'kafkacli COMMAND --help' for more information on a command.
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
