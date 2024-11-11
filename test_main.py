import unittest
from pyspark.sql import SparkSession
from mylib.lib import (
    load_and_preprocess,
    calculate_basic_stats,
    get_top_countries_by_alcohol,
)
from main import main


class TestMain(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a Spark session once for all tests
        cls.spark = SparkSession.builder.appName("TestAlcoholConsumption").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests
        cls.spark.stop()

    def setUp(self):
        # Setup a sample DataFrame to use in tests
        self.csv_url = (
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/"
            "alcohol-consumption/drinks.csv"
        )
        self.df = load_and_preprocess(self.csv_url)

    def test_main_execution(self):
        # Check if the main function runs without errors
        try:
            main()
            main_executed = True
        except Exception as e:
            main_executed = False
            print("Error in main:", e)

        self.assertTrue(
            main_executed, "The main function did not execute successfully."
        )

    def test_load_and_preprocess(self):
        # Test if the data loads and renames columns correctly
        columns = self.df.columns
        self.assertIn("beer", columns, 
                      "Column renaming failed for 'beer'")
        self.assertIn("spirits", columns, 
                      "Column renaming failed for 'spirits'")
        self.assertIn("wine", columns,
                       "Column renaming failed for 'wine'")
        self.assertIn("total_alcohol", columns, 
                      "Column renaming failed for 'total_alcohol'")

    def test_calculate_basic_stats(self):
        stats = calculate_basic_stats(self.df)
        # Check if the output is a Spark DataFrame and includes statistics like 'mean'
        self.assertIsNotNone(stats, "Basic statistics should not be None")
        self.assertIn("summary", stats.columns, 
                      "Statistics should include a summary column")
        stats.show()  # Optional: print stats for debugging

    def test_get_top_countries_by_alcohol(self):
        # Test if the function retrieves the correct top N countries
        top_5 = get_top_countries_by_alcohol(self.df, 5)
        top_5_count = top_5.count()
        self.assertEqual(top_5_count, 5, "Top N countries should return 5 rows")
        self.assertIn(
            "country",
            top_5.columns,
            "Top countries DataFrame should include 'country' column",
        )


if __name__ == "__main__":
    unittest.main()
