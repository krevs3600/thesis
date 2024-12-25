# Risingwave test

Below some notes for my future self and instruction to run the test.

## Notes
- In Risingwave a primary key has to be defined in the table creation.
- The ints_string.csv file data, as it was provided, has no primary key and it cannot be defined directly composing the key cause the int4 column has null values.
- I did not find a way to create new column with generated values directly in risingwave.
- I did not want to edit the source file directly.
- During the ingestion of the ints_string.csv file in Postgres I had to define a new columns idx generated incrementally with ```SERIAL PRIMARY KEY```


## How to run
Run postgres and risingwave through docker
```bash
docker compose up -d
```
After importing the env var, to execute the query via risingwave run
```bash
python app.py
```
When the execution complete stop docker with
After this just run
```bash
docker compose down
```