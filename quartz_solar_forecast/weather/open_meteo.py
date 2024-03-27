from datetime import datetime

import pandas as pd
import requests
import json


class WeatherDataHandler:
    def __init__(self):
        pass

    def _build_url(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        variables: list
    ) -> str:
        """Builds the URL for the Open Meteo API.

        Args:
            latitude (float): The latitude of the location.
            longitude (float): The longitude of the location.
            start_date (str): The start date of the forecast.
            end_date (str): The end date of the forecast.
            variables (list): The list of variables to include in the forecast.

        Returns:
            str: The URL for the Open Meteo API.
        """
        url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={latitude}&longitude={longitude}"
            f"$minutely_15={','.join(variables)}"
            f"&start_date={start_date}&end_date={end_date}&timezone=GMT"
        )
        return url
    
    def _call_url(self, url: str) -> dict:
        """Calls the Open Meteo API and returns the response.

        Args:
            url (str): The URL for the Open Meteo API.

        Returns:
            dict: The response from the Open Meteo API.
        """
        response = requests.get(url)
        return json.loads(response.text)


    def get_15_minutely_weather_data_with_forecast(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Get 15-minutely weather data with forecast.

        Parameters
        ----------
        latitude : float
            Latitude of the location.
        longitude : float
            Longitude of the location.
        start_date : str
            Start date in format YYYY-MM-DD.
        end_date : str
            End date in format YYYY-MM-DD.

        Returns
        -------
        pd.DataFrame
            15-minutely weather data with forecast.
        """

        variables = [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "precipitation",
            "surface_pressure",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "wind_speed_10m",
            "wind_direction_10m",
            "is_day",
            "shortwave_radiation",
            "direct_radiation",
            "diffuse_radiation",
            "direct_normal_irradiance",
            "terrestrial_radiation",
        ]
        url = self._build_url(latitude, longitude, start_date, end_date,
                              variables)
        d = self._call_url(url)
        response = pd.DataFrame(d["minutely_15"])
        response["date"] = pd.to_datetime(response["time"])
        return response


class WeatherService:
    def __init__(self):
        """
        Initialize the WeatherService.

        This class provides high-level weather-related functionality using OpenMeteo API.
        """
        self.data_handler = WeatherDataHandler()

    def _validate_coordinates(
        self, latitude: float, longitude: float
    ) -> None:
        """
        Validate latitude and longitude coordinates.

        Parameters
        ----------
        latitude : float
            The latitude value to be checked.
        longitude : float
            The longitude value to be checked.

        Raises
        ------
        ValueError
            If coordinates are not within valid ranges.
        """
        # Valid range for latitude: -90 to 90
        # Valid range for longitude: -180 to 180
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError(
                "Invalid coordinates. Latitude must be between -90 and 90, and longitude must be"
                " between -180 and 180."
            )

    def _validate_date_format(self, start_date: str, end_date: str) -> None:
        """
        Validate date format and check if end_date is greater than start_date.

        Parameters
        ----------
        start_date : str
            Start date in format YYYY-MM-DD.
        end_date : str
            End date in format YYYY-MM-DD.

        Raises
        ------
        ValueError
            If date format is invalid or end_date is not greater than start_date.
        """
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

            if end_datetime < start_datetime:
                raise ValueError("End date must be greater than start date.")
        except ValueError:
            raise ValueError(
                f"Invalid date format. Please use YYYY-MM-DD. Got {start_date} and {end_date}."
            )

    def get_minutely_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Get 15 minutely weather data ranging from 3 months ago up to 15 days ahead (forecast).
        Parameters
        ----------
        latitude : float
            The latitude of the location for which to get weather data.
        longitude : float
            The longitude of the location for which to get weather data.
        start_date : str
            The start date for the weather data, in the format YYYY-MM-DD.
        end_date : str
            The end date for the weather data, in the format YYYY-MM-DD.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the hourly weather data for the specified location and date
            range. The data includes both historical and forecast data.

        Raises
        ------
        ValueError
            If the provided coordinates are not within valid ranges, or if the date format is
            invalid, or if the end_date is not greater than the start_date.
        """
        self._validate_coordinates(latitude, longitude)
        self._validate_date_format(start_date, end_date)
        return self.data_handler.get_15_minutely_weather_data_with_forecast(
            latitude, longitude, start_date, end_date
        )
