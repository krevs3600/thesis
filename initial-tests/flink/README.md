# Flink test

Below some instructions to run the flink test.

## Requirements for Apache Flink
- Download and install the [Apache Flink standalone version](https://flink.apache.org/downloads/)
- Add it to the system path to run ```start-cluster.sh```, ```flink run /path/to/your.jar```, and ```stop-cluster.sh``` from anywhere.

There is no way to process CSV with a header in Flink so I came up with a workaround... Flink can ignore comments so let's make the first line of the CSV (the header) a comment.

To edit the source run (assuming that you have exported the env vars)
```bash
sed -i '1s/^/#/' "$INPUT_PATH/ints_string.csv"
```
To restore the source run 
```bash
sed -i '1s/^#//' "$INPUT_PATH/ints_string.csv"
```

After this just run
```bash
start-cluster.sh 
flink run /path/to/your.jar
stop-cluster.sh
```