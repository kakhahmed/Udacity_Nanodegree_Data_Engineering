import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, dayofmonth, dayofweek, isnan,
                                   isnull, month, split, weekofyear, when,
                                   year)


def read_csv(csv_file, spark):
    """Read `.csv` file into a dataframe using SparkSession

    Args:
        csv_file (str): the target CSV file with the table of interest.
        spark (pyspark.sql.session.SparkSession): A spark session.

    Returns:
        pyspark.sql.dataframe.DataFrame: Spark SQL dataframe.
    """
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(csv_file)


def fix_names(df):
    """Fix first_name and last_name values,
    if they are missing and there is name value.

    Args:
        df (Spark dataframe): Dataframe with name, first_name,
        and last name columns
    """
    return df.withColumn(
        "first_name", split(col("name"), " ").getItem(0)
    ).withColumn("last_name", split(col("name"), " ").getItem(1))


def inspect_data(df, perc=10, preview=False):
    """Inspect spark dataframe for columns with nulls
    and plot null percentage by showing columns with
    more than perc% values

    Args:
        df (Spark dataframe): Spark dataframe to inspect.
        perc (Integer): The percentage of nulls in columns
        to include in the saved plot.
    """

    # create a list of column names
    columns = df.columns
    n_rows = df.count()
    # loop through the list of column names
    null_col_count = {}
    for c in columns:
        # count the number of nulls in the column
        null_count = df.select(
            count(when(isnan(c) | isnull(c), c))).collect()[0][0]
        if null_count:
            null_col_count[c] = null_count / n_rows * 100
    filtered_null_col_count = {
        c: v for c, v in null_col_count.items() if v > perc}
    plt.bar(filtered_null_col_count.keys(), filtered_null_col_count.values())
    plt.title(f"Percentage of nulls for columns with > {perc}% nulls.")
    plt.xticks(rotation=90)
    plt.savefig("Columns_null_fraction.png")

    if preview:
        plt.show()


def create_players_table(df, output_path):
    df = select_table(df, [
        "player_id", col("date_of_birth").alias("birth_date"),
        col("country_of_birth").alias("birth_country"),
        col("country_of_citizenship").alias("citizenship"),
        "first_name", "last_name",
        col("height_in_cm").alias("height"),
        "foot", "position", "sub_position"
        ])
    write_table_parquet(df, ["player_id"], output_path + "/players")
    return df


def create_time_table(df, output_path):
    # extract columns to create time table
    df = df.select('date').withColumn(
        'hour', dayofmonth('date')
    ).withColumn(
        'day', dayofmonth('date')
    ).withColumn(
        'week', weekofyear('date')
    ).withColumn(
        'month', month('date')
    ).withColumn(
        'year', year('date')
    ).withColumn(
        'weekday', dayofweek('date')
    ).dropDuplicates()
    df.write.partitionBy(
            "year").mode("overwrite").parquet(output_path + "/time")
    return df


def select_table(df, columns_names):
    """Select columns from spark dataframe to create a new dataframe.

    Args:
        df (Spark dataframe): Spark dataframe with needed columns.
        columns_names (List): Columns names of interest.

    Returns:
        Spark dataframe: Selected spark dataframe
    """
    return df.select(*columns_names).dropDuplicates()


def write_table_parquet(df, partition_by, table_path):
    """Write spark table in parquet files.

    Args:
        df (Spark dataframe): Table to store.
        partition_by (List): Column names to partition by
        table_path (_type_): The directory path to store the parquet files.
    """
    df.write.partitionBy(*partition_by).mode("overwrite").parquet(table_path)


def create_player_appearance_table(df, output_path):
    """Create player_appearance table.

    Args:
        df (Spark dataframe): Table to create and store in parquet format.
        output_path (Str): Directory to save the created table.

    Returns:
        Spark dataframe: Created table.
    """
    df = select_table(df, [
        "appearance_id", "player_id", "competition_id",
        "game_id", "club_id", "yellow_cards", "red_cards",
        "minute", "minutes_played", "goals", "assists", "date"
        ])
    write_table_parquet(df, ["date", "competition_id"], output_path + "/player_appearance")
    return df


def create_games_table(df, output_path):
    """Create games table.

    Args:
        df (Spark dataframe): Table to create and store in parquet format.
        output_path (Str): Directory to save the created table.

    Returns:
        Spark dataframe: Created table.
    """
    df = select_table(df, [
        "game_id", "home_club_id", "away_club_id",
        "home_club_goals", "away_club_goals",
        "home_club_position", "away_club_position",
        col("home_club_manager_name").alias("home_club_manager"),
        col("away_club_manager_name").alias("away_club_manager"),
        "stadium", "attendance", "referee",
        ])
    write_table_parquet(df, ["game_id"], output_path + "/games")
    return df


def create_clubs_table(df, output_path):
    """Create clubs table.

    Args:
        df (Spark dataframe): Table to create and store in parquet format.
        output_path (Str): Directory to save the created table.

    Returns:
        Spark dataframe: Created table.
    """
    df = select_table(df, [
        "club_id", "name", "squad_size",
        "average_age", "foreigners_percentage",
        "national_team_players", "stadium_name",
        "stadium_seats", "coach_name",
        ])
    write_table_parquet(df, ["club_id"], output_path + "/clubs")
    return df


def create_competitions_table(df, output_path):
    """Create competitions table.

    Args:
        df (Spark dataframe): Table to create and store in parquet format.
        output_path (Str): Directory to save the created table.

    Returns:
        Spark dataframe: Created table.
    """
    df = select_table(df, ["competition_id", "name", "type", "country_id"])
    write_table_parquet(df, ["competition_id"], output_path + "/competitions")
    return df


def create_countries_table(df, output_path):
    """Create countries table.

    Args:
        df (Spark dataframe): Table to create and store in parquet format.
        output_path (Str): Directory to save the created table.

    Returns:
        Spark dataframe: Created table.
    """
    df = select_table(
        df, [
            "country_id", "country_name",
            "country_latitude", "country_longitude"
        ])
    write_table_parquet(df, ["country_id"], output_path + "/countries")
    return df


def data_quality_check(tables):
    """ Perform two data quality checks on dict of tables.

    Args:
        tables (DICT): A dictionary of tables and there names.
        {table_name: spark dataframe}

    Raises:
        ValueError: Table doesn't exists.
        ValueError: No rows available for a table.

    Returns:
        STR: A successful message that quality check was passed.
    """
    for table_name, df in tables.items():
        if df is not None:
            print(f"Table {table_name} exists.")
        else:
            raise ValueError(f"Table {table_name} doesn't exist")

        total_count = df.count()

        if total_count == 0:
            raise ValueError(f"No rows for {table_name}!")
        else:
            print(f"Table {table_name} has {total_count:,} rows.")
    return f"Data quality check passed for {list(tables.keys())}"


def main():
    output_path = "output_data"
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Read CSV/JSON") \
        .master("local[*]") \
        .getOrCreate()

    # Save the data in JSON format
    players_df = spark.read.json("raw_data/players_appearances/*")
    games_df = read_csv("football_data_transfer_market/games.csv", spark)
    clubs_df = read_csv("football_data_transfer_market/clubs.csv", spark)
    competitions_df = read_csv(
        "football_data_transfer_market/competitions.csv", spark)

    # Fix and inspect player datasets.
    players_df = fix_names(players_df)
    inspect_data(players_df)

    # Create Dimensions table.
    players = create_time_table(players_df, output_path)
    games = create_games_table(games_df, output_path)
    clubs = create_clubs_table(clubs_df, output_path)
    competitions = create_competitions_table(competitions_df, output_path)
    countries = create_countries_table(competitions_df, output_path)
    time = create_time_table(players_df, output_path)
    dimension_tables = {
        "players": players,
        "time": time,
        "games": games,
        "clubs": clubs,
        "competitions": competitions,
        "countries": countries,
    }
    data_quality_check(dimension_tables)

    # Create Fact table.
    player_appearance = {
        "player_appearance": create_player_appearance_table(players_df)
        }
    data_quality_check(player_appearance)


if __name__ == "__main__":
    main()
