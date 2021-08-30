#%%
import requests
import pandas as pd
from bs4 import BeautifulSoup


def group_id():
    r = requests.get(
        "http://www.tsetmc.com/Loader.aspx?ParTree=111C1213"
    )  # This URL contains all sector groups.
    soup = BeautifulSoup(r.text, "html.parser")
    header = soup.find_all("table")[0].find("tr")
    list_header = []
    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue

    # for getting the data
    HTML_data = soup.find_all("table")[0].find_all("tr")[1:]
    data = []
    for element in HTML_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text())
            except:
                continue
        data.append(sub_data)
    df = pd.DataFrame(data=data, columns=list_header).rename(
        columns={"گروه های صنعت": "group_name", "کد گروه های صنعت": "group_id"}
    )
    return df
