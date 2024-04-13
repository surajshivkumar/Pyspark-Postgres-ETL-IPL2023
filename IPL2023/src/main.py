from pyspark.sql import SparkSession
from extractor import Extractor
from transformer import Transformer
from loader import Loader
import warnings
from database import DatabaseManager
warnings.filterwarnings("ignore")

import json

def load_config(config_path):
    """Load the ETL configuration file."""
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

base_dir = '../'
config = load_config(base_dir + 'configs/etl_config.json')
config_db = load_config(base_dir + 'db/table_config.json')

def main():
    spark = SparkSession.builder.appName("Cricket Match Analysis").getOrCreate()
    #initialize 
    extractor = Extractor(spark)
    transformer = Transformer()
    loader = Loader(spark)
    print("*"*60)
    print("Stage 1/4 : Running Extract")
    # Extract
    match_df = extractor.read_csv(base_dir + config["input"]["matches"])
    deliv_df = extractor.read_csv(base_dir + config["input"]["deliveries"])
    print("-"*30)
    print("Stage 1/4 : Extract Complete")
    print("*"*60)
    print("Stage 2/4 : Transform")

    # Transform

    match_df = transformer.add_home_team(match_df)
    match_df = transformer.convert_dates(match_df)
    deliv_df = transformer.calculate_total_runs(deliv_df)
    match_stats = transformer.aggregate_match_stats(deliv_df)
    powerplay_stats = transformer.aggregate_powerplay_stats(deliv_df)
    match_df2 = match_df.join(powerplay_stats, on="match_id", how="inner")
    match_df2 = match_df.join(match_stats, on="match_id",how="inner")
    print("-"*30)
    print("Stage 2/4 : Transform Complete")
    print("*"*60)
    print("Stage 3/4 : Loading")
    outs_dir = base_dir + config['output']['match_stats']
    try:
        loader.write_csv(match_df2, outs_dir)
    except:
        print("Table exists")
    print("*"*60)
    print("Stage 3/4 : Loading Complete")
    print("File output at:",outs_dir)

    print("*"*60)
    print("Stage 4/4 : Loader SQL")
    try:
        dBConfig = config_db["ipl_db"]
        loader_sql = DatabaseManager(dBConfig)
        loader_sql.insert_data(dBConfig["table_match_info"], match_df.toPandas().fillna(0).values)
        loader_sql.insert_data(dBConfig["table_match_stats"], match_stats.toPandas().fillna(0).values)
        loader_sql.insert_data(dBConfig["table_powerplay_stats"], powerplay_stats.toPandas().fillna(0).values)
        loader_sql.close()
        print("Stage 4/4 : Loader SQL Complete")
    except:
        print("Skipping db load, db connection fault")

    spark.stop()

if __name__ == "__main__":
    main()
