# pipelines/princpal_orchestrator/orchestrator_match.py
import time

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import text

from extractors.extractor_match import MatchdayExtractor, MatchExtractor


# UPDATED: Function to extract matchday data using your extractors
def extract_matchday_data_with_soup(url, matchday_number, season):
    """Extract matchday data using your extractors"""
    print(f"🌐 Fetching HTML from: {url}")
    soup = get_selenium_soup(url)

    print("📊 Extracting matchday data...")
    extractor = MatchdayExtractor()
    return extractor.extract_matchday(soup, matchday_number, season)


def extract_match_detail_with_soup(
    url, matchday_id=None, season=None, competition=None, day_of_week=None
):
    print(f"🌐 Fetching match HTML from: {url}")
    soup = get_selenium_soup(url)

    print("⚽ Extracting match details...")
    extractor = MatchExtractor()
    detail = extractor.extract_from_url(soup, url)

    # inject only the extra context into match_info
    info = detail.match_info
    if matchday_id is not None:
        info.matchday_id = matchday_id
    if season is not None:
        info.season = season
    if competition is not None:
        info.competition = competition
    if day_of_week is not None:
        info.day_of_week = day_of_week
    if not getattr(info, "match_report_url", None):
        info.match_report_url = url

    return detail


def get_competition_and_season_data(session, competition_id=None):
    """
    Get competition code and season data from database tables,
    ensuring season_year is not beyond 2024.
    """
    comp_query = """
    SELECT c.competition_code,
           c.competition_id,
           t.season_year
    FROM competitions c
    JOIN teams t ON c.competition_id = t.competition_id
    WHERE t.season_year IS NOT NULL
      AND CAST(t.season_year AS INTEGER) <= 2024
    """

    if competition_id:
        comp_query += f" AND c.competition_id = '{competition_id}'"

    comp_query += " ORDER BY CAST(t.season_year AS INTEGER) DESC LIMIT 1"

    result = session.execute(text(comp_query)).first()

    if not result:
        raise ValueError(
            "No competition/season data found in database for season_year up to 2026"
        )

    return {
        "competition_code": result.competition_code,
        "competition_id": result.competition_id,
        "season_year": result.season_year,
    }


def get_next_matchday_to_scrape(session, competition_id, season_year):
    """
    Determine next matchday to scrape from existing matches table
    """
    # Check what matchdays we already have scraped in the matches table
    existing_matchdays = session.execute(
        text(
            """
        SELECT DISTINCT matchday 
        FROM matches 
        WHERE competition_id = :comp_id AND season = :season
        ORDER BY matchday DESC
    """
        ),
        {"comp_id": competition_id, "season": season_year},
    ).fetchall()

    if not existing_matchdays:
        return 1

    latest_scraped = existing_matchdays[0][0]
    next_matchday = latest_scraped + 1

    # Safety check - most leagues have max 38 matchdays
    if next_matchday > 38:
        print(
            f"⚠️  Next matchday would be {next_matchday}, which seems too high. Using matchday 1."
        )
        return 1

    return next_matchday


def build_transfermarkt_url(session, competition_code, season_year, matchday):
    """
    Build Transfermarkt URL from database components
    URL structure: https://www.transfermarkt.com/{competition_code}/spieltag/wettbewerb/{wettbewerb_code}/saison_id/{season_id}/spieltag/{matchday}
    """

    # Get wettbewerb_code from your competitions table
    wettbewerb_result = session.execute(
        text(
            """
        SELECT competition_id 
        FROM competitions 
        WHERE competition_code = :comp_code
    """
        ),
        {"comp_code": competition_code},
    ).first()

    if not wettbewerb_result:
        raise ValueError(f"Competition code '{competition_code}' not found in database")

    wettbewerb_code = wettbewerb_result[0]
    season_id = season_year

    return f"https://www.transfermarkt.com/{competition_code}/spieltag/wettbewerb/{wettbewerb_code}/saison_id/{season_id}/spieltag/{matchday}"


def get_selenium_soup(url: str, timeout: float = 20.0) -> BeautifulSoup:
    """Fetch a page via Selenium and return its BeautifulSoup."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get(url)

        # 1) Wait for full document.readyState
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except TimeoutException:
            # if still loading, give it a couple more seconds
            time.sleep(5)

        # 2) Then ensure at least one stable container is present
        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.box-content"))
            )
        except TimeoutException:
            # fallback: one more pause
            time.sleep(3)

        return BeautifulSoup(driver.page_source, "html.parser")

    finally:
        driver.quit()


def extract_matchday_data(url: str, matchday: int, season: str):
    """Extract matchday data from URL"""
    # Get soup using Selenium
    soup = get_selenium_soup(url)

    # Create extractor and extract data
    extractor = MatchdayExtractor()
    return extractor.extract_matchday(soup, matchday, season)


def extract_match_data(url: str):
    """Extract match data from URL"""
    # Get soup using Selenium
    soup = get_selenium_soup(url)

    # Create extractor and extract data
    extractor = MatchExtractor()
    return extractor.extract_from_url(soup, url)
