from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, when
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

LOG_FILE = "pyspark_output.md"

def log_output(operation, output_df, query=None):
    """Logs the operation, query, and output of 
    a Spark DataFrame to a markdown file."""
    # Ensure we're in the driver node environment for file writing
    output_str = output_df._jdf.showString(20, 20, False)  
    
    with open(LOG_FILE, "a") as file:
        file.write(f"## Operation: {operation}\n\n")
        if query:
            file.write(f"### Query: {query}\n\n")
        file.write("### The truncated output is:\n\n")
        file.write("```\n")  # Markdown code block start
        file.write(output_str)
        file.write("\n```\n\n")  # Markdown code block end

# Initialize Spark session
spark = SparkSession.builder.appName("AlcoholConsumptionAnalysis").getOrCreate()

def load_and_preprocess(csv_path):
    # Load CSV with Pandas
    pdf = pd.read_csv(csv_path)

    # Convert Pandas DataFrame to Spark DataFrame
    df = spark.createDataFrame(pdf)
    
    # Rename columns for easier access
    df = df.withColumnRenamed('beer_servings', 'beer') \
           .withColumnRenamed('spirit_servings', 'spirits') \
           .withColumnRenamed('wine_servings', 'wine') \
           .withColumnRenamed('total_litres_of_pure_alcohol',
                               'total_alcohol')
    
    return df


# Function to calculate basic statistics
def calculate_basic_stats(df):
    return df.select(['beer', 'spirits', 'wine', 'total_alcohol']).describe()

# Function to get top N countries by total alcohol consumption
def get_top_countries_by_alcohol(df, n=5):
    return df.select('country', 'total_alcohol').orderBy(
        desc('total_alcohol')).limit(n)


# Function to compute correlation matrix
def compute_correlation_matrix(df):
    # Converting Spark DataFrame to Pandas DataFrame to use correlation
    pd_df = df.select(['beer', 'spirits', 'wine', 'total_alcohol']).toPandas()
    return pd_df.corr()

# Function to plot average servings for each type of drink
def plot_average_servings(df):
    pd_df = df.select(['beer', 'spirits', 'wine']).toPandas()
    avg_servings = pd_df.mean()
    avg_servings.plot(kind='bar', color=['gold', 'lightblue', 'pink'])
    plt.title("Average Servings of Beer, Spirits, and Wine")
    plt.xlabel("Type of Drink")
    plt.ylabel("Average Servings")
    plt.savefig("average_servings.png", bbox_inches='tight')
    plt.close()

# Function to plot top countries by total alcohol consumption
def plot_top_countries(df):
    top_5 = df.select('country', 'total_alcohol').orderBy(
        desc('total_alcohol')).limit(5).toPandas()
    plt.figure(figsize=(5, 6))
    sns.barplot(data=top_5, x='total_alcohol', y='country', palette='viridis')
    plt.title("Top 5 Countries by Total Alcohol Consumption")
    plt.xlabel("Total Alcohol Consumption (Litres)")
    plt.ylabel("Country")
    plt.savefig("top_countries.png", bbox_inches='tight')
    plt.close()

# Function to plot servings distributions
def plot_servings_distributions(df):
    pd_df = df.select(['beer', 'spirits', 'wine', 'total_alcohol']).toPandas()

    plt.figure(figsize=(14, 10))
    sns.histplot(pd_df['beer'], kde=True, color='gold', bins=20)
    plt.title('Distribution of Beer Servings')
    plt.xlabel('Beer Servings')
    plt.ylabel('Frequency')
    plt.savefig("beer_distribution.png")
    plt.close()

    sns.histplot(pd_df['spirits'], kde=True, color='lightblue', bins=20)
    plt.title('Distribution of Spirit Servings')
    plt.xlabel('Spirit Servings')
    plt.ylabel('Frequency')
    plt.savefig("spirit_distribution.png")
    plt.close()

    sns.histplot(pd_df['wine'], kde=True, color='pink', bins=20)
    plt.title('Distribution of Wine Servings')
    plt.xlabel('Wine Servings')
    plt.ylabel('Frequency')
    plt.savefig("wine_distribution.png")
    plt.close()

    sns.histplot(pd_df['total_alcohol'], kde=True, color='purple', bins=20)
    plt.title('Distribution of Total Alcohol Consumption')
    plt.xlabel('Total Litres of Pure Alcohol')
    plt.ylabel('Frequency')
    plt.savefig("total_alcohol_distribution.png")
    plt.close()

# Function to classify countries based on alcohol consumption level
def classify_and_count_categories(df):
    df = df.withColumn(
        'consumption_category',
        when(col('total_alcohol') < 3, "Low")
        .when((col('total_alcohol') >= 3) & (
            col('total_alcohol') <= 6), "Moderate")
        .otherwise("High")
    )
    df.groupBy('consumption_category').count().show()
    return df
