from functools import wraps
import aiohttp.client_exceptions
import aiohttp.http_exceptions
import requests
from urllib.parse import unquote
import os
import aiohttp
import asyncio
import certifi
import ssl
from bs4 import BeautifulSoup
import logging


HOME_DIR = os.path.dirname(__file__)
sslcontext = ssl.create_default_context(cafile=certifi.where())


URL = "https://orthodoxbiblestudy.info"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
}


def configure_logging():
    logging.basicConfig(
        filename="podcast.log",
        filemode="a",
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )

    return logging.getLogger(__name__)


LOGGER = configure_logging()
LOGGER.addHandler(logging.StreamHandler())


def retry(max_attempts=5, delay=5):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = 0
            while attempts <= max_attempts:
                try:
                    return await func(*args, **kwargs)
                except aiohttp.client_exceptions.ServerDisconnectedError:
                    attempts += 1
                    if attempts == max_attempts:
                        raise
                    LOGGER.error(
                        f"Server disconnected. retrying after {delay} seconds. Attempt {attempts}/{max_attempts}."
                    )
                    await asyncio.sleep(delay)
                except Exception as e:
                    LOGGER.error(f"An unexpected error occured: {str(e)}")

        return wrapper

    return decorator


def save_to_file(content, header, name):
    with open(f"{HOME_DIR}/podcasts/{header}/{name}.mp3", "xb") as f:
        f.write(content)
        LOGGER.info("Finished!")


@retry()
async def download_video(s, link, name, header):
    if not os.path.isfile(f"{HOME_DIR}/podcasts/{header}/{name}.mp3"):
        try:
            async with s.get(link, headers=headers, ssl=sslcontext) as r:
                content = await r.content.read()
                soup = BeautifulSoup(content, features="html.parser")
                story_content = soup.find("div", class_="storycontent")
                # link = story_content.find("ul").find_next_sibling().find("a").get("href")
                link = unquote(
                    story_content.find("audio", class_="wp-audio-player")
                    .find_previous("p")
                    .find("a")
                    .get("href")
                ).replace(" ", "")

                async with s.get(
                    link, headers=headers, allow_redirects=True, ssl=sslcontext
                ) as res:
                    content = await res.read()
                    if res.headers["Content-Type"] == "audio/mpeg":
                        LOGGER.info(f"Downloading {name}... ")
                        save_to_file(content, header, name)
                    else:
                        LOGGER.info(
                            f"link for {name} doesn't seem to be an audio file, See content type: "
                            f"{res.headers['Content-Type']}"
                            f"Link: {link} ",
                        )
        except AttributeError as e:
            LOGGER.error(f"Error parsing html for {name} under {header}:" f"{e}")
        except aiohttp.client_exceptions.InvalidUrlClientError as e:
            LOGGER.error(
                f"Error fetching download {name} link:{link} " f"See error: {e}"
            )
        except aiohttp.client_exceptions.ClientPayloadError as e:
            LOGGER.error(
                f"Error fetching download {name} link:{link} " f"See error: {e}"
            )


def extract_links(sections):
    podcasts = []

    for section in sections:
        try:
            rows = section.find("table").find_all("tr")
            header = section.find("h3").text
            os.makedirs(f"{HOME_DIR}/podcasts/{header}", exist_ok=True)
            for row in rows:
                cell = row.select("td")[-1]
                anchor = cell.find("a")
                name = anchor.text.replace("\n", "")
                link = anchor.get("href")
                podcasts.append((name, header, link))
        except Exception as e:
            print(e)
            # logging.info("Unable to parse: ", e.__str__())
    return podcasts


async def main():
    tasks = []
    os.makedirs(f"{HOME_DIR}/podcasts", exist_ok=True)

    r = requests.get(URL, headers=headers)
    home_soup = BeautifulSoup(r.content, features="html5lib")
    sections = home_soup.find("div", id="sidebar").find_all("div", recursive=False)
    podcasts = extract_links(sections)

    async with aiohttp.ClientSession(trust_env=True) as s:
        for name, header, link in podcasts:
            # await download_video(s, link, name, header)
            tasks.append(asyncio.ensure_future(download_video(s, link, name, header)))

        await asyncio.gather(*tasks)

    LOGGER.info(
        "Finished downloading! Successful downloads: ",
        # len(successful_downloads),
        "Unsuccessful Downloads: ",
        # len(unsuccessful_downloads),
    )


asyncio.run(main())
# THREAD_POOL.shutdown(wait=True)
# count = 0
# for section in sections:
#     for link in links:
#         if section == link["header"]:
#             count += 1
# print(count)
