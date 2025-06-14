
import asyncio
from astro_data.fetcher import fetch_and_store_astro_data

if __name__ == "__main__":
    asyncio.run(fetch_and_store_astro_data())
