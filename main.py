#!/usr/bin/env python3.9

import asyncio
import hashlib
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

import aiohttp
import aiofiles
import progressbar


BUF_SIZE = 1024**2 * 16

NAMESPACES = {
    "itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
}

RSS_URLS = [
    # "http://feeds.feedburner.com/dancarlin/history?format=xml",
]

TIMEZONES = {
    "UTC": timezone.utc,
    "CET": timezone(timedelta(hours=1), "CET"), "CEST": timezone(timedelta(hours=2), "CEST"),
    "GMT": timezone(timedelta(hours=0), "GMT"),
    "EST": timezone(timedelta(hours=-5), "EST"), "EDT": timezone(timedelta(hours=-4), "EST"),
    "PST": timezone(timedelta(hours=-8), "PST"), "PDT": timezone(timedelta(hours=-7), "PDT"),
}


def parsed_datetime(timestamp: str) -> datetime:
    try:
        format = "%a, %d %b %Y %H:%M:%S %z"

        return datetime.strptime(timestamp, format)

    except ValueError:
        format = "%a, %d %b %Y %H:%M:%S"
        timestamp, tz_name = timestamp.rsplit(" ", maxsplit=1)

        return datetime.strptime(timestamp, format).replace(tzinfo=TIMEZONES[tz_name])


def datetime_in_utc(timestamp: datetime) -> datetime:
    """Return naive datetime in UTC given a tz-aware datetime."""

    return timestamp.replace(tzinfo=None) - timestamp.utcoffset()


def md5sum(input: str) -> str:
    return hashlib.md5(input.encode()).hexdigest()


def safe_filename(filename: str) -> str:
    # TODO: Probably should include some more characters
    additional_chars = """ -_'":()[]{}"""
    return "".join([c for c in filename if c.isalnum() or c in additional_chars])


async def get(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as response:
        return await response.text()


async def download(session: aiohttp.ClientSession, url: str, target: str, target_size = None) -> None:
    """Download url using session to target location.
    """
    # Issue regarding timeout https://github.com/aio-libs/aiohttp/issues/2249
    # Writing to an async file https://www.twilio.com/blog/working-with-files-asynchronously-in-python-using-aiofiles-and-asyncio

    target_size = target_size or progressbar.UnknownLength
    downloaded_bytes = 0

    # TODO: Use suitable timeout
    print("downloading", url)
    with progressbar.ProgressBar(max_value=target_size) as bar:
        bar.start()

        async with session.get(url, timeout=None) as response:
            # TODO: Maybe use NamedTemporaryFile to be able to move it to target location
            # or use .progress-file and move to location when complete
            #async with aiofiles.tempfile.TemporaryFile('wb') as output_file:
            async with aiofiles.open(target, "wb") as output_file:
                async for chunk in response.content.iter_chunked(BUF_SIZE):

                    await output_file.write(chunk)

                    downloaded_bytes += len(chunk)

                    bar.update(downloaded_bytes)


async def main() -> None:
    RSS_URLS.extend(sys.argv[1:])
    async with aiohttp.ClientSession() as session:
        for rss_url in RSS_URLS:

            content = await get(session, rss_url)
            tree = ElementTree.fromstring(content)

            for channel in tree.iterfind("channel"):

                pod_name = channel.find("title").text[:65]

                for item in channel.iterfind("item"):

                    title = item.find("title").text[:100]
                    guid = item.find("guid").text
                    published_at = datetime_in_utc(parsed_datetime(item.find("pubDate").text))

                    enclosure = item.find("enclosure")
                    if enclosure is None:
                        print("no enclosure")

                        continue

                    enclosure_type = enclosure.get("type")
                    enclosure_length = int(enclosure.get("length"))
                    enclosure_url = enclosure.get("url")
                    file_suffix = urllib.parse.urlparse(enclosure_url).path.rsplit(".", maxsplit=1)[-1]

                    elements = (published_at.isoformat(sep=" ") + "Z", md5sum(guid), pod_name, title)
                    basename = safe_filename(" - ".join(elements))

                    await download(session, url=enclosure_url, target=basename + "." + file_suffix, target_size=enclosure_length or 1)

                    print(basename + "." + file_suffix)

                    return


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

