
import os
import aiohttp
from datetime import date
from db import SessionLocal
from models.astro_data import AstroData
from models.user_location import UserLocation

API_KEY = os.getenv("WEATHER_API_KEY")

async def fetch_and_store_astro_data():
    async with SessionLocal() as session:
        result = await session.execute(
            "SELECT DISTINCT resolved_location FROM user_locations"
        )
        locations = [row[0] for row in result.fetchall()]

        for location in locations:
            async with aiohttp.ClientSession() as client:
                url = f"https://api.weatherapi.com/v1/astronomy.json?key={API_KEY}&q={location}&dt={date.today()}"
                async with client.get(url) as resp:
                    if resp.status != 200:
                        print(f"Failed for {location}: {resp.status}")
                        continue
                    data = await resp.json()
                    try:
                        astro = data["astronomy"]["astro"]
                        moon_phase = astro.get("moon_phase")
                        moon_illumination = int(float(astro.get("moon_illumination", 0)))
                        sunrise = astro.get("sunrise")
                        sunset = astro.get("sunset")
                        moonrise = astro.get("moonrise")
                        moonset = astro.get("moonset")

                        # Проверка на дубликат
                        existing = await session.execute(
                            "SELECT id FROM astro_data WHERE location = :loc AND date = :dt",
                            {"loc": location, "dt": date.today()}
                        )
                        if existing.fetchone():
                            print(f"Already exists for {location}")
                            continue

                        entry = AstroData(
                            location=location,
                            date=date.today(),
                            moon_phase=moon_phase,
                            moon_illumination=moon_illumination,
                            sunrise=sunrise,
                            sunset=sunset,
                            moonrise=moonrise,
                            moonset=moonset
                        )
                        session.add(entry)
                        await session.commit()
                        print(f"Saved data for {location}")
                    except Exception as e:
                        print(f"Parse error for {location}: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(fetch_and_store_astro_data())
