import pandas as pd
import requests
from time import sleep

WIKIPEDIA_URL = 'http://en.wikipedia.org/w/api.php?action=query&format=json&redirects=1&prop=pageprops&titles='
WIKIDATA_URL = 'http://www.wikidata.org/entity/'


def fetch_wikidata_iri(page_title):
    url = WIKIPEDIA_URL + page_title
    try:
        response = requests.get(url, timeout=5)
        response_json = response.json()
        for page_number, page_data in response_json['query']['pages'].items():
            return WIKIDATA_URL + page_data['pageprops']['wikibase_item']
    except KeyError:
        print(f'Item {page_title} not found.')
        return None
    except Exception:
        print(f'Item {page_title} not retrieved due to a network error.')
        return None


df = pd.read_csv('WikiSRS_similarity.csv', sep='\t')
df['wikidata_iri_1'] = None
df['wikidata_iri_2'] = None
columns = df.columns.tolist()
columns = columns[:4] + columns[-2:] + columns[4:-2]
df = df[columns]

for index, row in df.iterrows():
    print(f'~ {index} ~')
    title_1 = row['String1'].replace(' ', '_')
    df.at[index, 'wikidata_iri_1'] = fetch_wikidata_iri(title_1)
    title_2 = row['String2'].replace(' ', '_')
    df.at[index, 'wikidata_iri_2'] = fetch_wikidata_iri(title_2)
    sleep(0.5)

df.to_csv('wikidata_similarity.csv', sep='\t')
