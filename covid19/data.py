import asyncio
import datetime
from os import environ
from pathlib import Path

import aiohttp
import pandas as pd

from covid19 import logger as log


class CovidData:
    def __init__(self, cache_path=Path(__file__).parent):
        self.api_key = environ.get("DARKSKY_KEY", None)
        log.debug(f"Darksky api key: {self.api_key}")
        self.cache_path = cache_path
        self.is_diff = True

    @staticmethod
    async def _get_forecast_response(url: str, session: aiohttp.ClientSession, row: pd.Series) -> pd.Series:
        async with session.get(url) as resp:
            json = await resp.json()
            log.debug(f"Processing json response {json}")
            json = json['currently']
            json['Lat'] = row['Lat']
            json['Long'] = row['Long']
            json['Date'] = row['Date']
            return pd.Series(json)

    async def _forecast_url(self, row: pd.Series) -> str:
        current_time = datetime.datetime.strptime(row['Date'] + 'T12:00:00',
                                                  '%Y-%m-%dT%H:%M:%S')
        return 'https://api.darksky.net/forecast/%s/%s,%s,%s' \
               '?units=%s&lang=%s' % (self.api_key, row['Lat'], row['Long'],
                                      current_time.replace(microsecond=0).isoformat(),
                                      "auto", "en")

    async def _get_weather_from_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        tasks = []
        async with aiohttp.ClientSession() as session:
            for index, row in df.iterrows():
                log.debug(f"Adding async fetch task for Date:{row['Date']} Lat:{row['Lat']} Long:{row['Long']}")
                task = asyncio.ensure_future(self._get_forecast_response(await self._forecast_url(row),
                                                                         session,
                                                                         row))
                tasks.append(task)
            log.debug(f"Executing {len(tasks)} async tasks to fetch weather data")
            weather = pd.DataFrame(await asyncio.gather(*tasks))
        return weather

    async def _get_weather(self, covid_data: pd.DataFrame) -> pd.DataFrame:
        if not self.api_key:
            return pd.DataFrame()
        pickle_file = self.cache_path / "weather.pkl"
        if pickle_file.exists():
            log.debug("Reading data from weather cache file")
            weather_data = pd.read_pickle(pickle_file)
            diff = covid_data[['Date', 'Lat', 'Long']].merge(
                weather_data[['Date', 'Lat', 'Long']],
                how='outer', indicator=True
            ).loc[lambda x: x['_merge'] == 'left_only'].drop('_merge', axis=1)
            if not diff.empty:
                weather_data = weather_data.append(await self._get_weather_from_forecast(diff))
            else:
                log.debug("Nothing new to fetch")
                self.is_diff = False
        else:
            log.debug("Creating new pickle file")
            weather_data = await self._get_weather_from_forecast(covid_data)

        if self.is_diff:
            log.debug(f"Writing {len(weather_data)} records to pickle file")
            weather_data.to_pickle(pickle_file)
        return weather_data

    @staticmethod
    async def _fetch_covid19_data() -> pd.DataFrame:
        log.debug("Fetching data from github covid dataset")
        return pd.read_csv(
            "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv")

    async def get_data(self) -> pd.DataFrame:
        covid_data = await self._fetch_covid19_data()
        weather_data = await self._get_weather(covid_data)
        if not weather_data.empty:
            covid_weather_data = pd.merge(covid_data, weather_data, on=['Date', 'Lat', 'Long'])
            if self.is_diff:
                covid_weather_data.to_pickle(self.cache_path / "covid_weather_data.pkl")
            return covid_weather_data
        else:
            return covid_data
