#!/usr/bin/env python3.9

import asyncio
import hashlib
import os.path
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree
from typing import Optional

import aiohttp
import aiofiles
import aiofiles.os
#import progressbar
from tqdm.asyncio import tqdm


BUF_SIZE = 1024**1 * 256

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
    additional_chars = """ -_'":()[]{}"""
    safe_name = "".join([c for c in filename if c.isalnum() or c in additional_chars])

    return safe_name


async def get(session: aiohttp.ClientSession, url: str) -> str:
    async with session.get(url) as response:
        return await response.text()


async def download(session: aiohttp.ClientSession, url: str, target: str,
                   target_size: Optional[int] = None, progress_suffix: str = ".progress",
                   ) -> None:
    """Download url using session to target location."""
    # Issue regarding timeout https://github.com/aio-libs/aiohttp/issues/2249
    # Writing to an async file https://www.twilio.com/blog/working-with-files-asynchronously-in-python-using-aiofiles-and-asyncio

    #target_size = target_size or progressbar.UnknownLength
    downloaded_bytes = 0

    # TODO: Use suitable timeout
    #with progressbar.ProgressBar(max_value=target_size) as bar:
        #bar.start()
    try:
        async with session.get(url, timeout=None, read_bufsize=BUF_SIZE) as response:
            with tqdm(total=target_size, unit="b", unit_scale=True, desc=url) as bar:

                # TODO: Maybe use NamedTemporaryFile to be able to move it to target location
                # or use .progress-file and move to location when complete
                #async with aiofiles.tempfile.TemporaryFile('wb') as output_file:
                async with aiofiles.open(target + progress_suffix, "wb") as output_file:
                    async for chunk in response.content.iter_chunked(BUF_SIZE):

                        await output_file.write(chunk)

                        downloaded_bytes += len(chunk)

                        bar.update(len(chunk))

                await aiofiles.os.rename(target + progress_suffix, target)

                bar.set_description(target)

    except aiohttp.ClientResponseError:
        pass


async def download_feed(rss_url: str, session: aiohttp.ClientSession, loop):

    content = await get(session, rss_url)
    tree = ElementTree.fromstring(content)

    episode_tasks = []

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

            target_filename = basename + "." + file_suffix
            if not await aiofiles.os.path.exists(target_filename):
                episode_tasks.append(loop.create_task(download(session, url=enclosure_url, target=target_filename, target_size=enclosure_length)))

    await asyncio.wait(episode_tasks)


async def main(loop) -> None:
    RSS_URLS.extend(sys.argv[1:])
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=1, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector, raise_for_status=True) as session:

        await asyncio.gather(*[download_feed(rss_url, session, loop) for rss_url in RSS_URLS])



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))

