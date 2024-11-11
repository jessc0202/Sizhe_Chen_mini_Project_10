from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import matplotlib.pyplot as plt
import seaborn as sns
from mylib.lib import (
    load_and_preprocess,
    calculate_basic_stats,
    get_top_countries_by_alcohol,
    compute_correlation_matrix,
    plot_average_servings,
    plot_top_countries,
    plot_servings_distributions,
    classify_and_count_categories,
)

# Initialize Spark session
spark = SparkSession.builder.appName("AlcoholConsumptionAnalysis").getOrCreate()


def main():
    # Load dataset
    drink_csv = (
        "https://raw.githubusercontent.com/fivethirtyeight/data/"
        "master/alcohol-consumption/drinks.csv"
    )
    drink_df = load_and_preprocess(drink_csv)

    # Basic Data Inspection
    drink_df.printSchema()  # Prints schema instead of .info()
    print("Number of rows:", drink_df.count())
    print("Number of missing values:")
    drink_df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in drink_df.columns]
    ).show()

    # Basic Statistics
    basic_stats = calculate_basic_stats(drink_df)
    basic_stats.show()

    # Top 5 Countries by Total Alcohol Consumption
    top_countries = get_top_countries_by_alcohol(drink_df, 5)
    top_countries.show()

    # Correlation Matrix
    correlation_matrix = compute_correlation_matrix(drink_df)
    print("Correlation Matrix:\n", correlation_matrix)

    # Visualizations
    plot_average_servings(drink_df)
    plot_top_countries(drink_df)
    plot_servings_distributions(drink_df)

    # Data Manipulation: Classification by Consumption Level
    drink_df = classify_and_count_categories(drink_df)

    # Identify and print countries with zero alcohol consumption
    zero_consumption_countries = drink_df.filter(drink_df["total_alcohol"] == 0).select(
        "country", "total_alcohol"
    )
    print("Countries with Zero Alcohol Consumption:\n")
    zero_consumption_countries.show()

    # Visualize Alcohol Consumption by Category
    plot_category_counts(drink_df)


def plot_category_counts(df):
    category_counts = df.groupBy("consumption_category").count().toPandas()
    plt.figure(figsize=(8, 6))
    sns.barplot(
        x=category_counts["consumption_category"],
        y=category_counts["count"],
        palette="viridis",
    )
    plt.title("Number of Countries by Alcohol Consumption Category")
    plt.xlabel("Consumption Category")
    plt.ylabel("Number of Countries")
    plt.savefig("consumption_category.png", bbox_inches="tight")
    plt.close()


def save_to_markdown(drink_csv):
    """Save summary report to markdown."""
    # Load and preprocess data
    df = load_and_preprocess(drink_csv)

    # Generate descriptive statistics
    basic_stats = calculate_basic_stats(df)
    markdown_basic_stats = basic_stats.toPandas().to_markdown()

    # Get top 5 countries by total alcohol consumption
    top_5_countries = get_top_countries_by_alcohol(df, 5)
    markdown_top_5 = top_5_countries.toPandas().to_markdown()

    # Compute correlation matrix
    correlation_matrix = compute_correlation_matrix(df)
    markdown_corr_matrix = correlation_matrix.to_markdown()

    # Classify countries based on alcohol consumption
    df = classify_and_count_categories(df)

    # Generate visualizations and save them as images
    plot_average_servings(df)
    plot_top_countries(df)
    plot_servings_distributions(df)
    plot_category_counts(df)

    # Write the analysis summary to markdown
    with open("alcohol_consumption_summary.md", "w", encoding="utf-8") as file:
        file.write("# Alcohol Consumption Data Analysis Summary\n\n")

        file.write("## Basic Statistics\n")
        file.write(markdown_basic_stats)
        file.write("\n\n")

        file.write("## Top 5 Countries by Total Alcohol Consumption\n")
        file.write(markdown_top_5)
        file.write("\n\n")

        file.write("## Correlation Matrix\n")
        file.write(markdown_corr_matrix)
        file.write("\n\n")

        file.write("## Visualizations\n")
        file.write("### Average Servings by Drink Type\n")
        file.write("![Average Servings](average_servings.png)\n\n")

        file.write("### Top 5 Countries by Total Alcohol Consumption\n")
        file.write("![Top Countries](top_countries.png)\n\n")

        file.write("### Distribution of Servings\n")
        file.write("![Beer Distribution](beer_distribution.png)\n")
        file.write("![Spirits Distribution](spirit_distribution.png)\n")
        file.write("![Wine Distribution](wine_distribution.png)\n")
        file.write("![Total Alcohol Distribution](total_alcohol_distribution.png)\n\n")

        file.write("### Alcohol Consumption by Category\n")
        file.write("![Consumption Category](consumption_category.png)\n\n")

    print("Summary report saved as 'alcohol_consumption_summary.md'")


if __name__ == "__main__":
    main()
    drink_csv = (
        "https://raw.githubusercontent.com/fivethirtyeight/"
        "data/master/alcohol-consumption/drinks.csv"
    )
    save_to_markdown(drink_csv)
