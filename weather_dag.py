import os
from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import pandas as pd



def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    
    # Check if the required keys exist in the data dictionary
    if all(key in data for key in ["name", "weather", "main", "wind", "dt", "sys"]):
        city = data["name"]
        weather_description = data["weather"][0]['description']
        temp_farenheit = kelvin_to_celsius(data["main"]["temp"])
        feels_like_farenheit = kelvin_to_celsius(data["main"]["feels_like"])
        min_temp_farenheit = kelvin_to_celsius(data["main"]["temp_min"])
        max_temp_farenheit = kelvin_to_celsius(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {
            "City": city,
            "Description": weather_description,
            "Temperature (F)": temp_farenheit,
            "Feels Like (F)": feels_like_farenheit,
            "Minimum Temp (F)": min_temp_farenheit,
            "Maximum Temp (F)": max_temp_farenheit,
            "Pressure": pressure,
            "Humidity": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": time_of_record,
            "Sunrise (Local Time)": sunrise_time,
            "Sunset (Local Time)": sunset_time
        }

        transformed_data_list = [transformed_data]
        df_data = pd.DataFrame(transformed_data_list)

        aws_credentials = {"key": "ASIAUQXJCTULIOHGS3AX", "secret": "hEpEmSA7JSKr/6qmvG0aY8WEuc9Ucza+KQLImLrn", "token": "FwoGZXIvYXdzEDYaDLzXW7acTPpdZEYT7CJq/XSfaI+q1rQ8UeLFvgASemUf6PZKCnTyxJX6DRqhEKL//DqlLMnE1sdSX6cgTTXlfMemgUXXoqFDKmnJgfKqY3YuuZ51uuwWUCHu4b5jaGyH0p20nQNjLA9LeJsn2POVPxm9xltyG+jFvCjc/JiqBjIoUaOpdJhCCQrHk83DRmLKvvJtPI9gzGwzZUURnLxg+8ey9iyuAf25ag=="}

        now = datetime.now()
        dt_string = now.strftime("%d%m%Y%H%M%S")
        dt_string = 'current_weather_data_ankara_' + dt_string
        df_data.to_csv(f"s3://weather-api-airflow-test-yml/{dt_string}.csv", index=False, storage_options=aws_credentials)


        # Define the output directory
        output_dir = '/home/ubuntu/airflow/'

        # Generate the file path
        file_path = os.path.join(output_dir, f"{dt_string}.csv")

        # Write the DataFrame to the file
        df_data.to_csv(file_path, index=False)
    else:
        # Handle the case where some required keys are missing in the data dictionary
        raise ValueError("Missing or invalid data keys in the API response")


#First, the default arguments of the DAG instance should be determined
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 30),
    "email": ['onatpostac@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

#Then, a unique DAG should be created with proper names and parameters
with DAG('weather_dag',
         default_args=default_args,
         schedule_interval= '@daily',
         catchup=False) as dag:

        is_weather_api_ready = HttpSensor(
            task_id= 'is_weather_api_reaDy',
            http_conn_id= 'weathermap_api',
            endpoint= '/data/2.5/weather?q=ankara&appid=b3dad8ee999affbda8d272cb4c0cb562'
        )

        extract_weather_data = SimpleHttpOperator(
                task_id='extract_weather_data',
                http_conn_id='weathermap_api',
                endpoint='/data/2.5/weather?q=ankara&appid=b3dad8ee999affbda8d272cb4c0cb562',
                method='GET',
                response_filter= lambda r: json.loads(r.text),
                log_response=True
        )

        transofrm_load_weather_data = PythonOperator(
                task_id='transform_load_weather_data',
                python_callable=transform_load_data,
        )

        is_weather_api_ready >> extract_weather_data >> transofrm_load_weather_data