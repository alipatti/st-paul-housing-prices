from typing import Annotated, Optional
from pathlib import Path
from functools import wraps

from playwright.async_api import Page, async_playwright
import asyncio
import typer
from rich.progress import track
from rich import print

DATA_PATH = Path("data/raw")

SEARCH_PAGE_URL = "https://beacon.schneidercorp.com/Application.aspx?AppID=959&LayerID=18852&PageTypeID=2&PageID=12460"

START_DATE_SELECTOR = "#ctlBodyPane_ctl00_ctl01_txtStartDate"
END_DATE_SELECTOR = "#ctlBodyPane_ctl00_ctl01_txtEndDate"
SUBMIT_BUTTON_SELECTOR = "#ctlBodyPane_ctl00_ctl01_btnSearch"
DOWNLOAD_BUTTON_SELECTOR = "#ctlBodyPane_ctl00_ctl01_btnDownload"


async def get_data_by_year(year: int, page: Page):
    await page.goto(SEARCH_PAGE_URL)

    # search for all sales within the year
    start_input = await page.query_selector(START_DATE_SELECTOR)
    end_input = await page.query_selector(END_DATE_SELECTOR)
    submit_button = await page.query_selector(SUBMIT_BUTTON_SELECTOR)

    await start_input.type(f"01-01-{year}", delay=154)  # type: ignore
    await end_input.type(f"12-31-{year}", delay=97)  # type: ignore
    await submit_button.click(delay=143)  # type: ignore

    # wait for page to load
    await page.wait_for_selector(DOWNLOAD_BUTTON_SELECTOR)

    # download file
    async with page.expect_download() as download_info:
        download_button = await page.query_selector(DOWNLOAD_BUTTON_SELECTOR)
        await download_button.click()  # type: ignore

    download = await download_info.value
    path = DATA_PATH / f"{year}.xlsx"
    await download.save_as(path)

    return path


async def get_data(
    start_year: int,
    end_year: Annotated[Optional[int], typer.Argument()] = None,
):
    end_year = end_year or start_year
    years = list(reversed(range(start_year, end_year + 1)))

    print(
        f"[b][yellow]Scraping property sales from Jan 1 {start_year} to Dec 31 {end_year}"
    )

    DATA_PATH.mkdir(exist_ok=True, parents=True)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()

        # agree to terms of use
        print("Agreeing to terms of service...")
        page = await context.new_page()
        await page.goto(SEARCH_PAGE_URL)
        await page.get_by_role("button", name="Agree", exact=True).click()
        await page.wait_for_selector(START_DATE_SELECTOR)  # wait to process

        for y in track(years, description="Scraping..."):
            await get_data_by_year(y, page)

        await page.close()


@wraps(get_data)
def main(*args, **kwargs):
    asyncio.run(get_data(*args, **kwargs))


if __name__ == "__main__":
    typer.run(main)
