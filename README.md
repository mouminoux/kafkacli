# Kafkacli [![Build Status](https://travis-ci.org/mouminoux/kafkacli.svg?branch=master)](https://travis-ci.org/mouminoux/kafkacli)

Kafkacli is a consumer and producer client for Apache Kafka.

### Usage:
```
kafkacli [-b] -t... [--from-beginning] [-g] [-m] [-h...]

Options:                 
  -b, --broker           brokers (default "localhost:9092")
  -t, --topic            topic
      --from-beginning   start with the earliest message
  -g, --consumer-group   consumer group id
  -m, --message          message message
  -h, --header           message header <key=value>

```
