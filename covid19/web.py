import asyncio
import json
from pathlib import Path
from os.path import getmtime
from time import ctime
from flask import Flask

from covid19.data import CovidData

app = Flask(__name__)

with open(Path(__file__).parent / 'static' / 'pivotui_template.html', 'r') as html_file:
    TEMPLATE = html_file.read()


async def pivot_ui(**kwargs) -> bytes:
    df = await CovidData().get_data()
    csv = df.to_csv(encoding='utf8')
    modified_time = ctime(getmtime(Path(__file__).parent / 'covid_weather_data.pkl'))
    if hasattr(csv, 'decode'):
        csv = csv.decode('utf8')
    return (TEMPLATE % dict(csv=csv, modified_time=modified_time, kwargs=json.dumps(kwargs))).encode()


@app.route('/')
def home():
    loop = asyncio.new_event_loop()
    html = loop.run_until_complete(pivot_ui())
    return html.decode()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000)
