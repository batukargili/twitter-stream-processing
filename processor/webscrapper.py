import requests
from bs4 import BeautifulSoup
from config import covid_source_url


def get_covid_count():
    """
    Web scrapper with BeautifulSoup
    Gets total covid cases value from worldometers.info
    Reaching value with nested divs.

                div id = maincounter-wrap
                    div class = maincounter-number
                        span <covid_value>
    Returns
    -------
    int covid_cases_value
    """
    page = requests.get(covid_source_url)
    soup = BeautifulSoup(page.text, 'html.parser')

    covid_cases_value = soup \
        .find("div", attrs={"id": "maincounter-wrap"}) \
        .find("div", attrs={"class": "maincounter-number"}) \
        .find("span").text

    return covid_cases_value
