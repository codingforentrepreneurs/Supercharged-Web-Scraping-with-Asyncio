import asyncio
from arsenic import get_session, keys, browsers, services

async def scraper(url, i=-1, timeout=60, start=None, body_delay=10):
    service = services.Chromedriver()
    browser = browsers.Chrome(chromeOptions={
        'args': ['--headless', '--disable-gpu']
    })
    async with get_session(service, browser) as session:
        try:
            await asyncio.wait_for(session.get(url), timeout=timeout)
        except asyncio.TimeoutError:
            return []
        if body_delay > 0:
            await asyncio.sleep(body_delay)
        body = await session.get_page_source()
        return body