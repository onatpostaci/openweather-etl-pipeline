# OpenWeather ETL Pipeline Project
This project creates a pipeline using Airflow and Python. The ETL pipeline first extracts data from the OpenWeatherMap API and transforms the JSON data into a CSV file (semi-structured data to structured data), and loads the CSV File into an AWS S3 Bucket. The architectural structure of the project can be demonstrated as follows:


![Architecture of the Complete Workflow](./images/ETLPipelineSample.drawio.png)


As can be seen from the above figure, the pipeline uses the Data Source as OpenWeather and uses it's open-source API to extract the data as JSON response. Then, using Python the data is transformed to first a DataFrame, and then a CSV file. This means that the data will be received as a semi-structured form and transformed into a tabular which is a structured form. Lastly, the structured data are saved into the an eventual data source which is Amazon S3 Bucket in this case; however, more generally, the final source of transformed data is data warehouse.