import pandas as pd
import requests
import os
from time import sleep

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
WIKIDATA_URL = 'https://query.wikidata.org/sparql'


def get_relations(iri_1, iri_2):
    with open(os.path.join(THIS_FOLDER, 'get_relations.sparql')) as file:
        query = file.read().replace('{iri_1}', iri_1).replace('{iri_2}', iri_2)
    try:
        response = requests.get(WIKIDATA_URL, params={'format': 'json', 'query': query},
                                headers={'user-agent': 'pathsim'}, timeout=5)
        if response.status_code == requests.codes.ok:
            return [(binding['rel']['value'], binding['type']['value'])
                    for binding in response.json()['results']['bindings']]
        else:
            return None
    except Exception:
        print(f'Network error, restarting...')
        sleep(1)
        return get_relations(iri_1, iri_2)


df = pd.read_csv(os.path.join(THIS_FOLDER, 'sorted_pairs', 'Q6256.csv'), sep='\t')

counts = {}
for index, row in df.iterrows():
    print(f'~ {index}/{len(df.index)} ~')
    iri_1 = row['wikidata_iri_1']
    iri_2 = row['wikidata_iri_2']
    relations = get_relations(iri_1, iri_2)
    for relation in relations:
        if relation not in counts:
            counts[relation] = 0
        else:
            counts[relation] += 1
    sleep(1)

rows = [(key[0], key[1], value) for key, value in counts.items()]
result = pd.DataFrame(data=rows, columns=['Predicate', 'ObjectType', 'Count'])
result.sort_values(by='Count', ascending=False, inplace=True)
result.to_csv('counted_relations/Q6256.csv', sep='\t', index=False)
