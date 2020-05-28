import pandas as pd
import requests
import os
from time import sleep

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
WIKIDATA_URL = 'https://query.wikidata.org/sparql'


def get_common_types(iri_1, iri_2):
    with open(os.path.join(THIS_FOLDER, 'get_entity_types.sparql')) as file:
        query = file.read().replace('{iri_1}', iri_1).replace('{iri_2}', iri_2)
    try:
        response = requests.get(WIKIDATA_URL, params={'format': 'json', 'query': query},
                                headers={'user-agent': 'pathsim'}, timeout=5)
        if response.status_code == requests.codes.ok:
            return [binding['type']['value'] for binding in response.json()['results']['bindings']]
        else:
            return None
    except Exception:
        print(f'Network error, restarting...')
        sleep(1)
        return get_common_types(iri_1, iri_2)


df = pd.read_csv('wikidata_similarity.csv', sep='\t', index_col=0)
df = df.dropna()

sorted_dfs = {}
for index, row in df.iterrows():
    print(f'~ {index}/{len(df.index)} ~')
    iri_1 = row['wikidata_iri_1']
    iri_2 = row['wikidata_iri_2']
    common_types = get_common_types(iri_1, iri_2)
    for common_type in common_types:
        type_id = common_type.split('/')[-1]
        if type_id not in sorted_dfs:
            sorted_dfs[type_id] = pd.DataFrame(data=None, columns=df.columns)
        sorted_dfs[type_id] = sorted_dfs[type_id].append(row, ignore_index=True)
    sleep(1)

for type_id, type_df in sorted_dfs.items():
    type_df.to_csv(f'sorted_pairs/{type_id}.csv', sep='\t', index=False)
