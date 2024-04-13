from pyspark.sql.functions import udf, col, when, sum as _sum, count, to_date, month, dayofmonth, dayofweek, date_format


class Transformer:
    def __init__(self):
        self.home_team_mapping = \
        {
            'MA Chidambaram Stadium':'Chennai Super Kings',
            'Bharat Ratna Shri Atal Bihari Vajpayee Ekana Cricket Stadium':'Lucknow Super Giants',
            'Rajiv Gandhi International Stadium':'Sunrisers Hyderabad',
            'Narendra Modi Stadium':'Gujarat Titans',
            'Sawai Mansingh Stadium':'Rajasthan Royals',
            'Wankhede Stadium':'Mumbai Indians',
            'Arun Jaitley Stadium':'Delhi Capitals',
            'Punjab Cricket Association IS Bindra Stadium':'Punjab Kings',
            'M Chinnaswamy Stadium':'Royal Challengers Bangalore',
            'Eden Gardens':'Kolkata Knight Riders',
            'Himachal Pradesh Cricket Association Stadium': 'Punjab Kings',
            'Barsapara Cricket Stadium' : 'Rajasthan Royals'
        }
        self.mapping_func = udf(lambda v: self.home_team_mapping.get(v, None))

    def add_home_team(self, df):
        return df.withColumn('home_team', self.mapping_func(df['venue']))

    def convert_dates(self, df):
        df = df.withColumn('date', to_date(df['date'], 'yyyy/MM/dd'))
        df = df.withColumn('month', date_format('date', 'MMMM'))
        df = df.withColumn('day', dayofmonth('date'))
        return df.withColumn('day_name', date_format('date', 'EEEE'))

    def calculate_total_runs(self, df):
        return df.withColumn('total_runs', col('runs_off_bat') + col('extras') + col('wides') + col('noballs') + col('byes') + col('legbyes'))

    def aggregate_match_stats(self, df):
        df = df.withColumnRenamed("batting_team", "ms_batting_team")
        df = df.withColumnRenamed("bowling_team", "ms_bowling_team")
        return df.groupBy('match_id', 'ms_batting_team', 'ms_bowling_team').agg(
            _sum('total_runs').alias('ms_runs_scored'),
            count(when(col('wicket_type').isNotNull(), True)).alias('ms_wickets'),
            count(when(col('runs_off_bat') == 4, True)).alias('ms_fours'),
            count(when(col('runs_off_bat') == 6, True)).alias('ms_sixes'),
            count(when(col('total_runs') == 0, True)).alias('ms_dots'),
            _sum('noballs').alias('ms_no_balls'),
            _sum('wides').alias('ms_wides')
        )
    def aggregate_powerplay_stats(self, df):
        # Filter for Powerplay overs (first 6 overs)
        powerplay_df = df.filter(df.ball < 6.1)
        powerplay_df = powerplay_df.withColumnRenamed("batting_team", "pp_batting_team")
        powerplay_df = powerplay_df.withColumnRenamed("bowling_team", "pp_bowling_team")
        # Aggregate statistics for the Powerplay period
        return powerplay_df.groupBy('match_id', 'pp_batting_team', 'pp_bowling_team').agg(
            _sum('total_runs').alias('pp_runs_scored'),
            count(when(col('wicket_type').isNotNull(), True)).alias('pp_wickets'),
            count(when(col('runs_off_bat') == 4, True)).alias('pp_fours'),
            count(when(col('runs_off_bat') == 6, True)).alias('pp_sixes'),
            count(when(col('total_runs') == 0, True)).alias('pp_dots'),
            _sum('noballs').alias('pp_no_balls'),
            _sum('wides').alias('pp_wides')
        )
