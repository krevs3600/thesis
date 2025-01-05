# Nexmark test
Below the instructions to run the nexmark benchmark.

## General steps
- Run redpanda in docker through the docker compose file. 
- Reset or create the topic in redpanda ```./kafka_event_gen reset```
- Start the middleware (check next paragraph)
- Start the nexmark generator ```./kafka_event_gen generate -t <backend_name> -e <number_of_events```


### Renoir
```shell
 ./renoir_nexmark -q <query_number>
```

### Risingwave

```shell
docker compose up -d # to start the risingwave instance
python app.py # to setup risingwave [todo: query selector]
```

### Flink
Check the README.md in the project folder
