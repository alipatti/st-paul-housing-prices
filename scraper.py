from typing import List, Optional
from playwright.async_api import async_playwright
from playwright.async_api import BrowserContext
from pathlib import Path
import asyncio_pool
import asyncio
import typer
from functools import wraps

DATA_PATH = Path("data")

SEARCH_PAGE_URL = "https://beacon.schneidercorp.com/Application.aspx?AppID=959&LayerID=18852&PageTypeID=2&PageID=12460"

START_DATE_SELECTOR = "#ctlBodyPane_ctl00_ctl01_txtStartDate"
END_DATE_SELECTOR = "#ctlBodyPane_ctl00_ctl01_txtEndDate"
SUBMIT_BUTTON_SELECTOR = "#ctlBodyPane_ctl00_ctl01_btnSearch"
DOWNLOAD_BUTTON_SELECTOR = "#ctlBodyPane_ctl00_ctl01_btnDownload"


async def get_data_by_year(year: int, context: BrowserContext):
    print(f"Fetching {year}")

    page = await context.new_page()
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
    print(f"Saving to {path}")
    await download.save_as(path)

    await page.close()

    return path


async def get_data(
    start_year: int,
    end_year: int | None = None,
    concurrency=4,
    headless=False,
):
    if start_year and end_year:
        years = list(reversed(range(start_year, end_year + 1)))
    else:
        years = [start_year]

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context()

        # agree to terms of use
        print("Agreeing to terms of service...")
        page = await context.new_page()
        await page.goto(SEARCH_PAGE_URL)
        await page.get_by_role("button", name="Agree", exact=True).click()
        await page.wait_for_selector(START_DATE_SELECTOR)  # wait to process
        await page.close()

        # scrape actual data while limiting number of simultaneous connections
        _results = await asyncio_pool.AioPool(concurrency).map(
            lambda y: get_data_by_year(y, context),
            years,
        )


def main(
    start_year: int,
    end_year: Optional[int] = None,
    concurrency: int = 4,
):
    asyncio.run(get_data(start_year, end_year, concurrency, headless))


if __name__ == "__main__":
    typer.run(main)
