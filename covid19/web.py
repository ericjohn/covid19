import asyncio
import json
from os.path import getmtime
from pathlib import Path
from time import ctime, time

from dotenv import load_dotenv
from flask import Flask

from covid19 import logger
from covid19.data import CovidData


def covid19_webapp(instance_path=Path(__file__).parent, **kwargs):
    logger.debug(f"Setting instance path to {instance_path}")

    app = Flask(__name__, instance_path=instance_path)

    with open(Path(__file__).parent / 'static' / 'pivotui_template.html', 'r') as html_file:
        TEMPLATE = html_file.read()

    async def pivot_ui(path, **kwargs) -> bytes:
        df = await CovidData(cache_path=path).get_data()
        csv = df.to_csv(encoding='utf8')
        pickle_file = Path(path) / 'covid_weather_data.pkl'
        modified_time = ctime(getmtime(pickle_file)) if pickle_file.exists() else ctime(time())

        if hasattr(csv, 'decode'):
            csv = csv.decode('utf8')
        return (TEMPLATE % dict(csv=csv, modified_time=modified_time, kwargs=json.dumps(kwargs))).encode()

    @app.route('/')
    def home():
        loop = asyncio.new_event_loop()
        html = loop.run_until_complete(pivot_ui(path=app.instance_path, **kwargs))
        return html.decode()

    return app


if __name__ == '__main__':
    instance_path = Path(__file__).parent
    if (instance_path / '.env').exists():
        logger.debug(f"Loading {instance_path / '.env'}")
        load_dotenv(instance_path / '.env')
    app = covid19_webapp(instance_path=instance_path, rows=["Country/Region"], cols=["Date"],
                         aggregatorName="Maximum",
                         vals=["Deaths"],
                         rendererName="Line Chart")
    app.run(host="0.0.0.0", port=8000, load_dotenv=True)
