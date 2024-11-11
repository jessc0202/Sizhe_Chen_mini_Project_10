# Alcohol Consumption Data Analysis Summary

## Basic Statistics
|    | summary   |    beer |   spirits |     wine |   total_alcohol |
|---:|:----------|--------:|----------:|---------:|----------------:|
|  0 | count     | 193     |  193      | 193      |        193      |
|  1 | mean      | 106.161 |   80.9948 |  49.4508 |          4.7171 |
|  2 | stddev    | 101.143 |   88.2843 |  79.6976 |          3.7733 |
|  3 | min       |   0     |    0      |   0      |          0      |
|  4 | max       | 376     |  438      | 370      |         14.4    |

## Top 5 Countries by Total Alcohol Consumption
|    | country        |   total_alcohol |
|---:|:---------------|----------------:|
|  0 | Belarus        |            14.4 |
|  1 | Lithuania      |            12.9 |
|  2 | Andorra        |            12.4 |
|  3 | Grenada        |            11.9 |
|  4 | Czech Republic |            11.8 |

## Correlation Matrix
|               |     beer |   spirits |     wine |   total_alcohol |
|:--------------|---------:|----------:|---------:|----------------:|
| beer          | 1        |  0.458819 | 0.527172 |        0.835839 |
| spirits       | 0.458819 |  1        | 0.194797 |        0.654968 |
| wine          | 0.527172 |  0.194797 | 1        |        0.667598 |
| total_alcohol | 0.835839 |  0.654968 | 0.667598 |        1        |

## Visualizations
### Average Servings by Drink Type
![Average Servings](average_servings.png)

### Top 5 Countries by Total Alcohol Consumption
![Top Countries](top_countries.png)

### Distribution of Servings
![Beer Distribution](beer_distribution.png)
![Spirits Distribution](spirit_distribution.png)
![Wine Distribution](wine_distribution.png)
![Total Alcohol Distribution](total_alcohol_distribution.png)

### Alcohol Consumption by Category
![Consumption Category](consumption_category.png)

