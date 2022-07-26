#!/usr/bin/env python3.9

import asyncio
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

import aiohttp


NAMESPACES = {
    "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
}
RSS_URLS = [
    "http://feeds.feedburner.com/dancarlin/history?format=xml",
]
TIMEZONES = {
    "UTC": timezone.utc,
    "CET": timezone(timedelta(hours=1), "CET"), "CEST": timezone(timedelta(hours=2), "CEST"),
    "GMT": timezone(timedelta(hours=0), "GMT"),
    "EST": timezone(timedelta(hours=-5), "EST"), "EDT": timezone(timedelta(hours=-4), "EST"),
    "PST": timezone(timedelta(hours=-8), "PST"), "PDT": timezone(timedelta(hours=-7), "PDT"),
}


def parsed_datetime(timestamp: str) -> datetime:
    format = "%a, %d %b %Y %H:%M:%S"
    timestamp, tz_name = timestamp.rsplit(" ", maxsplit=1)
    return datetime.strptime(timestamp, format).replace(tzinfo=TIMEZONES[tz_name])


def datetime_in_utc(timestamp: datetime) -> datetime:
    """Return naive datetime in UTC given a tz-aware datetime."""

    return timestamp.replace(tzinfo=None) - timestamp.utcoffset()


def safe_filename(filename: str) -> str:
    # TODO: Probably should include some more characters
    return "".join([c for c in filename if c.isalnum()])


async def get(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as response:
        return await response.text()


async def download(session: aiohttp.ClientSession, url: str, target: str) -> None:
    """Download url using session to target location.
    """
    # Issue regarding timeout https://github.com/aio-libs/aiohttp/issues/2249
    # Writing to an async file https://www.twilio.com/blog/working-with-files-asynchronously-in-python-using-aiofiles-and-asyncio

    BUF_SIZE = 1024 * 16

    # TODO: Use suitable timeout
    async with session.get(url, timeout=None) as response:
        # TODO: Maybe use NamedTemporaryFile to be able to move it to target location
        # or use .progress-file and move to location when complete
        async with aiofiles.tempfile.TemporaryFile('wb') as output_file:
            async for chunk in response.content.iter_chunked(BUF_SIZE)
                await output_file.write(chunk)


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        for url in RSS_URLS:
            content = await get(session, url)
            tree = ElementTree.fromstring(content)
            for channel in tree.iterfind("channel"):
                pod_name = channel.find("title").text
                for item in channel.iterfind("item"):
                    title = item.find("title").text
                    published_at = parsed_datetime(item.find("pubDate").text)

                    print(" - ".join((pod_name, datetime_in_utc(published_at).isoformat(sep="T") + "Z", title)))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

