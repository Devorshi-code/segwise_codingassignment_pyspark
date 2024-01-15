# Playstore Insights using PySpark

This repository contains a PySpark script to generate insights from Playstore data. The script analyzes various combinations of properties and provides counts based on specified criteria.

## Prerequisites

- Apache Spark: Ensure that Apache Spark is installed and configured on your machine.
- Python: The script is written in Python. Ensure you have Python installed.

## Getting Started

1. Clone this repository:

    ```bash
    git clone <repository-url>
    cd playstore-insights-pyspark
    ```

2. Download the Playstore data file (`playstore.csv`) and place it in the project root directory.

3. Set up a virtual environment (optional but recommended):

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: .\venv\Scripts\activate
    ```

4. Install the required Python libraries:

    ```bash
    pip install pyspark
    ```

## Running the Script

Run the PySpark script using the `spark-submit` command:

export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH


```bash
spark-submit playstore_insights.py

## Contact

For any questions or concerns, contact bdevorshi100@gmail.com
