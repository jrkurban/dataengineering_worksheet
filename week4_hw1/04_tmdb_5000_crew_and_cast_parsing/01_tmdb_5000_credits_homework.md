Use `tmdb_5000_credits.csv` in https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip

Process this dataset with Apache Spark and create the following two dataframes:

## 1. credits_cast
```commandline
+--------+------+-------+-------------------+------------------------+------+-----+----------------+
|movie_id|title |cast_id|character          |credit_id               |gender|id   |name            |
+--------+------+-------+-------------------+------------------------+------+-----+----------------+
|19995   |Avatar|242    |Jake Sully         |5602a8a7c3a3685532001c9a|2     |65731|Sam Worthington |
|19995   |Avatar|3      |Neytiri            |52fe48009251416c750ac9cb|1     |8691 |Zoe Saldana     |
|19995   |Avatar|25     |Dr. Grace Augustine|52fe48009251416c750aca39|1     |10205|Sigourney Weaver|
|19995   |Avatar|4      |Col. Quaritch      |52fe48009251416c750ac9cf|2     |32747|Stephen Lang    |
+--------+------+-------+-------------------+------------------------+------+-----+----------------+
```
- One row represents one player.
- Distinct movie_id count must be 4803.

## 2. credits_crew
```commandline
+--------+------+------------------------+----------+------+----+------------------------+-----------------+
|movie_id|title |credit_id               |department|gender|id  |job                     |name             |
+--------+------+------------------------+----------+------+----+------------------------+-----------------+
|19995   |Avatar|52fe48009251416c750aca23|Editing   |0     |1721|Editor                  |Stephen E. Rivkin|
|19995   |Avatar|539c47ecc3a36810e3001f87|Art       |2     |496 |Production Design       |Rick Carter      |
|19995   |Avatar|54491c89c3a3680fb4001cf7|Sound     |0     |900 |Sound Designer          |Christopher Boyes|
|19995   |Avatar|54491cb70e0a267480001bd0|Sound     |0     |900 |Supervising Sound Editor|Christopher Boyes|
+--------+------+------------------------+----------+------+----+------------------------+-----------------+
```
- One row represents one crew.
- Distinct movie_id count must be 4803.

### Data source
https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata?select=tmdb_5000_movies.csv

