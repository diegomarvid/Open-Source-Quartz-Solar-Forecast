from datetime import datetime

import openmeteo_requests
import pandas as pd


class OpenMeteoAPIClient:

    def __init__(self):
        """
        Initialize the OpenMeteo API client.

        This class handles the interaction with the OpenMeteo API using a cached
        and retriable session.
        """
        self.openmeteo = openmeteo_requests.Client()

    def get_weather_data(self, url: str, params: dict):
        """
        Get weather data from the OpenMeteo API.

        Parameters
        ----------
        url : str
            The API endpoint URL.
        params : dict
            The parameters to be sent with the API request.

        Returns
        -------
        List[openmeteo_requests.Response]
            List of API responses.
        """
        return self.openmeteo.weather_api(url, params=params)


class WeatherDataFetcher:
    def __init__(self, api_client: OpenMeteoAPIClient):
        """
        Initialize the WeatherDataFetcher.

        Parameters
        ----------
        api_client : OpenMeteoAPIClient
            An instance of OpenMeteoAPIClient for API communication.
        """
        self.api_client = api_client

    def fetch_data(self, url: str, params: dict, max_retries: int = 5, base_delay: float = 1.0):
        """
        Fetch weather data from the OpenMeteo API.

        Parameters
        ----------
        url : str
            The API endpoint URL.
        params : dict
            The parameters to be sent with the API request.

        Returns
        -------
        openmeteo_requests.Response
            The API response.
        """
        responses = self.api_client.get_weather_data(url, params)
        return responses[0]


class WeatherDataProcessor:
    @staticmethod
    def process_minutely_15_data(response, params: dict) -> pd.DataFrame:
        """
        Process minutely 15 data from the API response.

        Parameters
        ----------
        response : openmeteo_requests.Response
            The API response containing minutely 15 data.
        params : dict
            The parameters used in the API request.

        Returns
        -------
        pd.DataFrame
            Processed minutely 15 data in DataFrame format.
        """
        minutely_15 = response.Minutely15()
        minutely_15_data = {
            "date": pd.date_range(
                start=pd.to_datetime(minutely_15.Time(), unit="s"),
                end=pd.to_datetime(minutely_15.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=minutely_15.Interval()),
                inclusive="left",
            )
        }
        for i, variable in enumerate(params["minutely_15"]):
            minutely_15_data[variable] = minutely_15.Variables(i).ValuesAsNumpy()

        minutely_15_dataframe = pd.DataFrame(data=minutely_15_data)
        return minutely_15_dataframe

    @staticmethod
    def process_hourly_data(response, params: dict) -> pd.DataFrame:
        """
        Process hourly data from the API response.

        Parameters
        ----------
        response : openmeteo_requests.Response
            The API response containing hourly data.
        params : dict
            The parameters used in the API request.

        Returns
        -------
        pd.DataFrame
            Processed hourly data in DataFrame format.
        """
        hourly = response.Hourly()
        hourly_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s"),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }
        for i, variable in enumerate(params["hourly"]):
            hourly_data[variable] = hourly.Variables(i).ValuesAsNumpy()

        hourly_dataframe = pd.DataFrame(data=hourly_data)
        return hourly_dataframe

    @staticmethod
    def process_historical_data(response, params: dict) -> pd.DataFrame:
        """
        Process historical data from the API response.

        Parameters
        ----------
        response : openmeteo_requests.Response
            The API response containing historical data.
        params : dict
            The parameters used in the API request.

        Returns
        -------
        pd.DataFrame
            Processed historical data in DataFrame format.
        """
        hourly = response.Hourly()
        historical_data = {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s"),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }
        for i, variable in enumerate(params["hourly"]):
            historical_data[variable] = hourly.Variables(i).ValuesAsNumpy()

        historical_dataframe = pd.DataFrame(data=historical_data)
        return historical_dataframe


class WeatherDataHandler:
    def __init__(
        self,
        api_client: OpenMeteoAPIClient,
        data_fetcher: WeatherDataFetcher,
        data_processor: WeatherDataProcessor,
    ):
        """
        Initialize the WeatherDataHandler.

        Parameters
        ----------
        api_client : OpenMeteoAPIClient
            An instance of OpenMeteoAPIClient for API communication.
        data_fetcher : WeatherDataFetcher
            An instance of WeatherDataFetcher for fetching data.
        data_processor : WeatherDataProcessor
            An instance of WeatherDataProcessor for processing data.
        """
        self.api_client = api_client
        self.data_fetcher = data_fetcher
        self.data_processor = data_processor

    def get_hourly_weather_data_with_forecast(
        self, latitude: float, longitude: float, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Get hourly weather data with forecast.

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
            Hourly weather data with forecast.
        """
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "dew_point_2m",
                "precipitation",
                "surface_pressure",
                "cloud_cover",
                "cloud_cover_low",
                "cloud_cover_mid",
                "cloud_cover_high",
                "visibility",
                "wind_speed_10m",
                "wind_speed_80m",
                "wind_speed_120m",
                "wind_speed_180m",
                "wind_direction_10m",
                "wind_direction_80m",
                "wind_direction_120m",
                "wind_direction_180m",
                "is_day",
                "sunshine_duration",
                "shortwave_radiation",
                "direct_radiation",
                "diffuse_radiation",
                "direct_normal_irradiance",
                "terrestrial_radiation",
            ],
            "timezone": "GMT",
            "start_date": start_date,
            "end_date": end_date,
        }
        key, response = self._get_from_cache_url_params(url, params)
        if response is None:
            response = self.data_fetcher.fetch_data(url, params)
            response = self.data_processor.process_hourly_data(response, params)
            self._save_to_cache(df=response, key=key)

        return response

    def get_15_minutely_weather_data_with_forecast(
        self, latitude: float, longitude: float, start_date: str, end_date: str
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
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "minutely_15": [
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
            ],
            "timezone": "GMT",
            "start_date": start_date,
            "end_date": end_date,
        }
       
        response = self.data_fetcher.fetch_data(url, params)
        response = self.data_processor.process_minutely_15_data(response, params)
        return response

    def get_weather_data_historical(
        self, latitude: float, longitude: float, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Get historical weather data.

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
            Historical weather data.
        """
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": [
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
            ],
            "start_date": start_date,
            "end_date": end_date,
        }

        response = self.data_fetcher.fetch_data(url, params)
        response = self.data_processor.process_historical_data(response, params)

        return response


class WeatherService:
    def __init__(self):
        """
        Initialize the WeatherService.

        This class provides high-level weather-related functionality using OpenMeteo API.
        """
        api_client = OpenMeteoAPIClient()
        data_fetcher = WeatherDataFetcher(api_client)
        data_processor = WeatherDataProcessor()
        self.data_handler = WeatherDataHandler(api_client, data_fetcher, data_processor)

    def _validate_coordinates(self, latitude: float, longitude: float) -> None:
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
            raise ValueError(f"Invalid date format. Please use YYYY-MM-DD. Got {start_date} and {end_date}.")

    def get_hourly_weather(
        self, latitude: float, longitude: float, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Get hourly weather data ranging from 3 months ago up to 15 days ahead (forecast).

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
        return self.data_handler.get_hourly_weather_data_with_forecast(
            latitude, longitude, start_date, end_date
        )

    def get_minutely_weather(
        self, latitude: float, longitude: float, start_date: str, end_date: str
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

    def get_historical_weather(
        self, latitude: float, longitude: float, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Get hourly weather data ranging from 1940 up to 5 days ago.
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
        return self.data_handler.get_weather_data_historical(
            latitude, longitude, start_date, end_date
        )