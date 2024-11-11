[![CI](https://github.com/jessc0202/Sizhe_Chen_mini_Project_10/actions/workflows/cicd.yml/badge.svg)](https://github.com/jessc0202/Sizhe_Chen_mini_Project_10/actions/workflows/cicd.yml)
# Alcohol Consumption Analysis

This project analyzes alcohol consumption data by country, providing insights into the average consumption of different types of alcohol (beer, spirits, and wine) and the total consumption of pure alcohol. It uses PySpark for data processing and analysis, along with `matplotlib` and `seaborn` for visualizations.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Functions and Features](#functions-and-features)
- [Testing](#testing)
- [License](#license)

## Installation

### Prerequisites

- Python 3.8+
- [PySpark](https://spark.apache.org/docs/latest/api/python/)
- `matplotlib`, `seaborn`, and other dependencies specified in `requirements.txt`

### Project Structure
   ```bash
   .
├── Dockerfile
├── LICENSE
├── Makefile
├── README.md
├── __pycache__
│   ├── main.cpython-312.pyc
│   └── test_main.cpython-312-pytest-7.1.3.pyc
├── alcohol_consumption_summary.md
├── average_servings.png
├── beer_distribution.png
├── consumption_category.png
├── data
│   └── drinks.csv
├── main.py
├── mylib
│   ├── __init__.py
│   ├── __pycache__
│   │   ├── __init__.cpython-312.pyc
│   │   └── lib.cpython-312.pyc
│   └── lib.py
├── repeat.sh
├── requirements.txt
├── setup.sh
├── spirit_distribution.png
├── test_main.py
├── top_countries.png
├── total_alcohol_distribution.png
└── wine_distribution.png
   ```

## Functions and Features

### Main Functions

- **`load_and_preprocess`**: Loads and preprocesses the dataset by renaming columns for easier access.
- **`calculate_basic_stats`**: Calculates basic descriptive statistics (mean, min, max, etc.).
- **`get_top_countries_by_alcohol`**: Retrieves the top N countries by total alcohol consumption.
- **`compute_correlation_matrix`**: Computes a correlation matrix for different types of alcohol consumption.
- **`plot_average_servings`**: Plots the average servings of beer, spirits, and wine.
- **`plot_top_countries`**: Visualizes the top countries by total alcohol consumption.
- **`plot_servings_distributions`**: Plots the distribution of servings for each type of alcohol.
- **`classify_and_count_categories`**: Classifies countries based on alcohol consumption and counts them by category.

### Visualizations

- **Average servings** of different types of alcohol.
- **Top 5 countries** by total alcohol consumption.
- **Distribution of servings** for beer, spirits, wine, and total alcohol.
- **Number of countries** by alcohol consumption category.

## References
1. https://github.com/nogibjj/python-ruff-template
2. https://github.com/fivethirtyeight/data/tree/master/daily-show-guests