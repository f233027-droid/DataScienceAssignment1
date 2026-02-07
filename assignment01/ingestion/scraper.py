
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict

try:
    from assignment01.config import TLC_BASE_URL
except ImportError:
    # Fallback for standalone testing
    TLC_BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

def get_tlc_links(years: List[int], taxi_types: List[str]) -> Dict[str, str]:
    """
    Scrapes the TLC website for parquet file URLs matching the specified years and taxi types.
    Returns a dictionary mapping 'filename' -> 'url'.
    """
    print(f"Scraping {TLC_BASE_URL} for {years} - {taxi_types}...")
    try:
        response = requests.get(TLC_BASE_URL)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching TLC page: {e}")
        return {}

    soup = BeautifulSoup(response.content, 'html.parser')
    links = {}

    # Regex patterns
    # Matches URLs ending in .parquet and containing specific year/type
    # Example: yellow_tripdata_2025-01.parquet
    
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href'].strip()
        if not href.endswith('.parquet'):
            continue
            
        filename = href.split('/')[-1]
        
        # Check against filters
        match_year = False
        for year in years:
            if str(year) in filename:
                match_year = True
                break
        
        match_type = False
        for t_type in taxi_types:
            if t_type in filename:
                match_type = True
                break
                
        if match_year and match_type:
            links[filename] = href

    print(f"Found {len(links)} matching files.")
    return links

if __name__ == "__main__":
    # Test run
    found = get_tlc_links([2024, 2025], ["yellow", "green"])
    for name, url in found.items():
        print(f"{name}: {url}")
