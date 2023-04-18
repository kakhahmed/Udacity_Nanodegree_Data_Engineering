import pandas as pd
from pyspark.sql import SparkSession
import pytest
import etl
import os


SPARK = SparkSession.builder \
    .appName("Testing") \
    .master("local[*]") \
    .getOrCreate()


def test_fix_names():
    """Test output for etl.fix_names."""
    # create a sample dataframe with a column containing NAN values
    data = [
        ("John Doe", "John", "Doe"),
        ("John Doe", None, "Doe"),
        ("John Doe", "John", None),
        (None, None, "Doe"),
        (None, "John", None)
    ]
    df = SPARK.createDataFrame(data, ["name", "first_name", "last_name"])
    assert etl.fix_names(df).toPandas() == pd.DataFrame(
        {
            'name': ['John Doe', 'John Doe', 'John Doe', None, None],
            'first_name': ['John', None, 'John', None, 'John'],
            'last_name': ['Doe', 'Doe', None, 'Doe', None]
        }
    )


def test_write_table_parquet(tmp_path):
    """Test etl.create_player_appearance_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = SPARK.createDataFrame(data, ["name", "age"])
    etl.write_table_parquet(df, tmp_path, "tmp")
    assert os.path.isfile(tmp_path + 'tmp.parquet')


def test_select_table():
    """Select columns to create a Spark dataframe.

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = SPARK.createDataFrame(data, ["name", "age"])
    df = etl.select_table(df, ["name"])
    assert df.count() == 3
    assert df.columns == ["name"]


def test_create_players_table(tmp_path):
    """Test etl.create_players_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = [
        "player_id", "birth_date", "birth_country",
        "citizenship", "first_name",
        "last_name", "height",
        "foot", "position", "sub_position"
        ]
    data = [(
        1, "dd-mm-yyyy", "country",
        "citizenship", "John", "Doe",
        176, "left", "left-wing", "striker"
        )]
    df = SPARK.createDataFrame(data, cols)
    etl.create_players_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'players.parquet')


def test_create_games_table(tmp_path):
    """Test etl.create_games_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = [
        "game_id", "home_club_id", "away_club_id",
        "home_club_goals", "away_club_goals",
        "home_club_position", "away_club_position",
        ]
    data = [(1, 1, 2, 1, 2, 2, 1)]
    df = SPARK.createDataFrame(data, cols)
    etl.create_games_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'games.parquet')


def test_create_clubs_table(tmp_path):
    """Test etl.create_clubs_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = [
        "club_id", "name", "squad_size",
        "average_age", "foreigners_percentage",
        "national_team_players", "stadium_name",
        "stadium_seats", "coach_name",
        ]
    data = [(
        1, "club_name", 26, 26, 50, 13,
        "stadium_arena", 6000, "coach"
        )]
    df = SPARK.createDataFrame(data, cols)
    df = etl.create_clubs_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'clubs.parquet')


def test_create_competitions_table(tmp_path):
    """Test etl.create_competitions_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = [
        "competition_id", "name", "type", "country_id"
        ]
    data = [(1, "competition", "domestic", 1)]
    df = SPARK.createDataFrame(data, cols)
    etl.create_competitions_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'competitions.parquet')


def test_create_countries_table(tmp_path):
    """Test etl.create_player_appearance_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = [
        "country_id", "country_name",
        "country_latitude", "country_longitude",
        ]
    data = [(1, "country", 111, 222)]
    df = SPARK.createDataFrame(data, cols)
    etl.create_countries_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'countries.parquet')


def test_create_time_table(tmp_path):
    """Test etl.create_player_appearance_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = ["date", "hour", "day", "week", "month", "weekday", "year"]
    data = [(1, 1, 2, 1, 2, 2, 1)]
    df = SPARK.createDataFrame(data, )
    etl.create_time_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'time.parquet')


def test_create_player_appearance_table(tmp_path):
    """Test etl.create_player_appearance_table

    Args:
        tmp_path (Str): Temporary path for storing parquet data.
    """
    cols = [
        "appearance_id", "player_id", "competition_id",
        "game_id", "club_id", "yellow_cards", "red_cards",
        "minute", "minutes_played", "goals", "assists", "date",
        ]
    data = [(1, 1, 1, 1, 1, 2, 0, 45, 90, 1, 1, "00:00:00")]
    df = SPARK.createDataFrame(data, cols)
    etl.create_player_appearance_table(df, tmp_path)
    assert df.columns == cols
    assert os.path.isfile(tmp_path + 'player_appearance.parquet')


def test_data_quality_check():
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = SPARK.createDataFrame(data, ["name", "age"])
    table = {"tested_table": df}
    etl.data_quality_check(table)


def test_raise_data_quality_check():
    data = []
    df = SPARK.createDataFrame(data, ["name", "age"])
    table = {"tested_table": data}
    with pytest.raises(ValueError):
        etl.data_quality_check(table)

    df = None
    table = {"tested_table": df}
    # Table doesn't exists.
    with pytest.raises(ValueError):
        etl.data_quality_check(df)
