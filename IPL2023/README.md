# IPL 2023 ETL and data analysis 🏏

This project is a to dive deep into some facts and stats for the Indian premier leage 2023 season.

## Project Structure

This project is structured as follows:\
<pre>
.
├── README.md
├── analysis
│   ├── Match-analysis.ipynb
│   ├── analysis.ipynb
│   └── spark-warehouse
├── configs
│   └── etl_config.json
├── data
│   ├── input
│   │   ├── deliveries.csv
│   │   └── matches.csv
│   └── output
│       └── match_stats
│           ├── _SUCCESS
│           └── part-00000-961eae76-b32c-4365-85ff-07968774526f-c000.csv
├── db
│   ├── schema
│   │   └── table_definitions.sql
│   └── table_config.json
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── __pycache__
│   │   ├── database.cpython-311.pyc
│   │   ├── extractor.cpython-311.pyc
│   │   ├── loader.cpython-311.pyc
│   │   ├── main.cpython-311.pyc
│   │   └── transformer.cpython-311.pyc
│   ├── database.py
│   ├── extractor.py
│   ├── loader.py
│   ├── main.py
│   ├── spark.py
│   └── transformer.py
└── tests
</pre>

## Installation ⬇️

To set up this project, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/yourproject.git
   cd yourproject
    ```

## Running the ETL
We have divided each extract, transform and load into seperate classes and can be found in /src.

To run all of them you can run : 
```bash
python src/main.py
```

## Analysis 📈
Analysis of the project can be found in:

```bash
cd analysis
jupyter notebook Match-analysis.ipynb
```


## Analysis
### Example of an analysis:

##### Who had the best win rate in IPL 2023?

```python
result_df = spark.sql('''
                      with matches as 
                      (
                          select *, rank() over(partition by match_id order by match_id) as rnk
                          from match
                      ),
                       wins as
                      (
                          select match_id, team1 as team ,winner from matches where rnk = 1
                          union
                          (select match_id, team2 as team, winner from matches where rnk = 1)
                      )
                      select team, round(100 * sum(case when team = winner then 1 else 0 end)/count(team),2) as win_rate
                      from wins
                      group by 1
                      order by 2 desc
                      ''')
result_df.show()
```


> Result:
<html>
<img width="20%" height="20%" src ="https://upload.wikimedia.org/wikipedia/en/thumb/0/09/Gujarat_Titans_Logo.svg/1200px-Gujarat_Titans_Logo.svg.png"</img>
</html>
<pre>
+--------------------+--------+
|                team|win_rate|
+--------------------+--------+
|      Gujarat Titans|   64.71|
| Chennai Super Kings|    62.5|
|      Mumbai Indians|   56.25|
|Lucknow Super Giants|   53.33|
|    Rajasthan Royals|    50.0|
|Royal Challengers...|    50.0|
|Kolkata Knight Ri...|   42.86|
|        Punjab Kings|   42.86|
|      Delhi Capitals|   35.71|
| Sunrisers Hyderabad|   28.57|
+--------------------+--------+
</pre>








