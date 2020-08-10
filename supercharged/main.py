import fire
import os
import asyncio
from arsenic import get_session, keys, browsers, services
import pandas as pd
from requests_html import HTML
import itertools
import re
import time
import pathlib
from urllib.parse import urlparse

import logging
import structlog # pip install structlog


from .projects.spoonflower import run_spoonflower

class Pipeline():
    def __init__(self):
        self.spoonflower = run_spoonflower

if __name__ == "__main__":
    fire.Fire(Pipeline)
