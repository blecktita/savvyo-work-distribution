from pipelines.run_competition_club_collection import scrape_club_data
from selenium import webdriver


if __name__ == "__main__":
    driver = webdriver.Chrome()
    scrape_club_data(driver=driver, environment="production", use_vpn=True)