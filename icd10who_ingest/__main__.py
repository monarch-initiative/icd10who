""""ICD10WHO Ingest

Creates an RDF file of ICD10WHO based on WHO's ICD API.

# More info
InsecureRequestWarning: This has been disabled. Read more here:
https://urllib3.readthedocs.io/en/1.26.x/advanced-usage.html#ssl-warnings

# TODO: RecursionError: (a) probably best: fix by doing a counter, then stepping out when reaches 999||1000, and set an
   iterator/recursor around /that/. (b) use multithreading so that when one thread dies, next can start. (c) any way to
   recurse over API without using pure recursion? (d) set recursion limit, but there are ~70k codes, so some systems
   could run out of memory (https://stackoverflow.com/questions/8177073/python-maximum-recursion-depth-exceeded).

# TODO: Use later when getting codes if needed
header_rdf = {
    'Authorization': 'Bearer ' + token,
    'Accept': 'application/rdf+xml',
    'Accept-Language': language,
    'API-Version': api_version}
current_xml = requests.get(current_uri, headers=header_rdf).text
g = rdflib.Graph()
g.parse(data=current_xml, format='xml')
xxx = [(s, p, o) for s, p, o in g]
# todo: state: Better design might be to not use state when using API, since caching anyway?
# todo: state: pickle file redundant with file system cache?
# todo: state: i have use_cache as a param, but it is always needed currently, as recursion error will always happen and
   ...pickle will need to be used.
# todo: exception handling: authentication seems to wear off when making too many requests:
   'Authentication failed. The request must include a valid and non-expired bearer token in the Authorization header.'
   ...need to parse JSONDecodeError for this
# todo: performance: when not reading from cache, its pretty slow. maybe takes like 20-45 minutes. Should profile.
   ...Perhaps instead of saving one at a time, save in bulk. Or set alternative to pickling only and set that as default
# TODO: namespace URLs: am I using correct? especially for ICD10WHO. what if not listed?
# TODO (finally): clean up commented out code
"""
import json
import os
import pickle
import yaml
from typing import Dict, List, Union

import requests
from dotenv import load_dotenv
from rdflib import Graph, Namespace, RDF, OWL, RDFS, URIRef, SKOS, Literal
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

from icd10who_ingest.utils import del_nested, get_nested, kv_recursive_generator, set_nested


# Vars
# # Semantic variables
LABEL_KEY = 'skos:prefLabel'
SUBCLASS_OF_KEY = 'rdfs:subClassOf'
# # RDF Namespaces
# This gave instances like: "OBO:ICD10WHO_A00-A09 a owl:Class ;"
# ICD10WHO = Namespace('http://purl.obolibrary.org/obo/ICD10WHO_')
ICD10WHO = Namespace('https://icd.who.int/browse10/2019/en#/')
# # Helper variables
TRAVERSING_KEY = 'traversing'
TRAVERSED_KEY = 'traversed'
# # Path variables
PKG_ROOT = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.join(PKG_ROOT, '..')
ENV_DIR = os.path.join(PROJECT_ROOT, 'env')
CACHE_DIR = os.path.join(PROJECT_ROOT, 'cache')
ENV_FILE = os.path.join(ENV_DIR, '.env')
PICKLED_STATE_PATH = os.path.join(CACHE_DIR, 'traversal_state.pickle')
SAVE_PATH = os.path.join(PROJECT_ROOT, 'icd10who.ttl')
# # Parameters
DEFAULT_PARAMS = {
    'api_version': 'v2',
    'language': 'en',
    'release': 'latest',
    'use_cache': True,
    # 'data_format': [
    #     'application/json', 'application/ld+json', 'application/xml', 'application/rdf+xml'][3],  # 3='rdf/xml'
    'client_id': os.getenv('CLIENT_ID'),
    'client_secret': os.getenv('CLIENT_SECRET'),
}


# Functions
def get_curie_map():
    """Rectrieve CURIE to URL info"""
    map_path = os.path.join(PKG_ROOT, 'data/curie_map.yaml')
    with open(map_path, "r") as f:
        maps = yaml.safe_load(f)
    return maps


def _get_auth_token(
    client_id: str, client_secret: str, scope='icdapi_access', grant_type='client_credentials',
    token_endpoint='https://icdaccessmanagement.who.int/connect/token'
) -> str:
    """Get authorization token"""
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': scope,
        'grant_type': grant_type}
    r = requests.post(token_endpoint, data=payload, verify=False).json()

    return r['access_token']


def _get_release_uri(release: str, headers: Dict, releases_uri='https://id.who.int/icd/release/10') -> str:
    """Get release URI"""
    releases_json = requests.get(releases_uri, headers=headers).json()

    if release == 'latest':
        release_uri = releases_json['latestRelease']
    else:
        year_uri_map = {
            url.rsplit('/', 1)[-1]: url
            for url in releases_json['release']
        }
        release_uri = year_uri_map[release]

    return release_uri


def _initialize_state(release, headers) -> Dict:
    """Initialize traversal state

    Previously, the state was being pickled and picked back up as a way around the RecursionError limit. It's no longer
    necessary to do that. However it has been left here as a placeholder should pickling be useful again in the future.
    """
    if os.path.exists(PICKLED_STATE_PATH):
        return pickle.load(open(PICKLED_STATE_PATH, 'rb'))

    traversal_state = {
        TRAVERSED_KEY: {},
        TRAVERSING_KEY: {}
    }
    release_uri: str = _get_release_uri(release, headers)
    chapters_json: Dict = requests.get(release_uri, headers=headers).json()
    traversal_state[TRAVERSING_KEY] = {k: {} for k in chapters_json['child']}

    return traversal_state


def _get_next_path(state, path) -> Union[List[str], None]:
    """Get next URI

    This function, or at least the logic surrounding how it is used, looks like it could be a lot cleaner.
    Particularly, the try/except at the bottom. Only encountered that IndexError when it finished the final node.
    """
    if not path:  # done with everything
        return None

    try:
        node_uris: Dict = get_nested(state, [TRAVERSING_KEY] + path)
    except KeyError:  # node was done, so was deleted. go to parent
        path = path[0:-1]
        node_uris: Dict = get_nested(state, [TRAVERSING_KEY] + path)

    if not node_uris:  # parent done, so delete that too, and go up again
        del_nested(state, [TRAVERSING_KEY] + path)
        path = path[:-1]
        if path:
            return _get_next_path(state, path)
        else:  # done w/ current top node. get next one
            node_uris: Dict = get_nested(state, [TRAVERSING_KEY])

    try:
        next_uri: str = list(node_uris.items())[0][0]
    except IndexError as err:
        if not state[TRAVERSING_KEY]:
            return None
        raise err
    next_path = path + [next_uri]

    return next_path


def _retrieve_node_json(path: List[str], headers, read_cache=False, cache_dir=CACHE_DIR) -> Dict:
    """Retrieve from cache if there, else from API.

    Side effects: Caches
    """
    node_path_list = [cache_dir] + [url.rsplit('/', 1)[-1] for url in path]
    node_dir_path = os.path.join(*node_path_list)
    node_response_path = os.path.join(node_dir_path, 'response.json')
    if os.path.exists(node_dir_path) and read_cache:
        with open(node_response_path, 'r') as file:
            response: Dict = json.load(file)
    else:
        current_uri = path[-1]
        response: Dict = requests.get(current_uri, headers=headers).json()
        if os.path.exists(node_response_path):
            os.remove(node_response_path)  # overwrite if exists
        else:
            os.makedirs(node_dir_path)
        with open(node_response_path, 'w') as file:
            file.write(json.dumps(response))

    return response


def recurse(
    state: Dict, path: List[str], headers: Dict, use_cache=False, i=0, recursion_limit=800, print_interval=0
):
    """Recurse through all terms in ICD, caching as we go. If interrupted and continue later and use_cache is True,
    will use cached data where it exists.

    :param path: State iterator path head
    :param recursion_limit: Because Python has a 1k recursion limit, we have to have this function call itself less than
    that. 800 is semi-arbitrary, as it is <1000, but I liked `print_interval` at 200, and it went evenly w/ that.
    :param print_interval: If > 0, prints the current node being processed every nth iteration.

    Side effects: This funciton is not pure. It updates the `state` variable in the calling frame (`traversal_state`).
    Hence why it does not need to return anything.

    :return: Returns state anyway, as that feels more pythonic and easier to read, even though the state in higher scope
    will already have been updated.
    """
    if print_interval and i % print_interval == 0:
        print(path[-1])

    current_json: Dict = _retrieve_node_json(path, headers, use_cache)
    update_to_traversed: Dict[str, {}] = {
        'semantic_data': {
            LABEL_KEY: current_json['title']['@value'],
            SUBCLASS_OF_KEY: current_json['parent'][-1]
        }
    }

    if 'child' in current_json:
        child_uris = {uri: {} for uri in current_json['child']}
        update_to_traversed = {**update_to_traversed, **child_uris}
        set_nested(state, [TRAVERSING_KEY] + path, child_uris)
    else:
        del_nested(state, [TRAVERSING_KEY] + path)

    set_nested(state, [TRAVERSED_KEY] + path, update_to_traversed)
    next_path: List[str] = _get_next_path(state, path)

    if not next_path or i == recursion_limit:
        return state, next_path
    return recurse(state, next_path, headers, use_cache, i+1)


def download_everything(traversal_state: Dict, headers: Dict, use_cache=False):
    """Downloads everything in the given code system ICD10WHO.

    This is a wrapper around recurse(). Ideally, recurse() would have been all that was needed. However, Python has
    a 1k recursion limit by default, and there are a lot more than 1k codes in the system. The solution implemented
    here will call recurse() the maximum amount of times allowed, return the state at that time, and then continue
    again.
    """
    traversal_head_path: List[str] = [list(traversal_state[TRAVERSING_KEY].items())[0][0]]
    while True:
        traversal_state, traversal_head_path = recurse(
            state=traversal_state,
            path=traversal_head_path,
            headers=headers,
            use_cache=use_cache)
        if not traversal_state[TRAVERSING_KEY]:
            break

    # os.remove(PICKLED_STATE_PATH)  # Add back if pickling added
    return traversal_state[TRAVERSED_KEY]  # the complete, traversed and filled out code system


def save_rdf(d):
    """From code system dict structure, generate and save RDF.
    todo: Optimally, URIs would be replaced with purls or codes before this step."""
    about = 'Created from the 2019 release of the ICD10WHO API: https://icd.who.int/icdapi'
    ignore_keys = ['semantic_data', 'rdfs:subClassOf', 'skos:prefLabel']
    icd10who_node = URIRef(ICD10WHO)

    graph = Graph()
    for prefix, uri in get_curie_map().items():
        graph.namespace_manager.bind(prefix, URIRef(uri))
    graph.add((icd10who_node, RDF.type, OWL.Ontology))
    graph.add((icd10who_node, SKOS.prefLabel, Literal('ICD10WHO')))
    graph.add((icd10who_node, RDFS.comment, Literal(about)))

    # Not sure why PyCharm typechecker is wrong so many times here.
    for k, v in kv_recursive_generator(d):
        if k in ignore_keys:
            continue
        code: str = k.split('/')[-1]
        # noinspection PyTypeChecker
        uri: URIRef = ICD10WHO[code]
        parent_code: str = v['semantic_data']['rdfs:subClassOf'].split('/')[-1]
        # noinspection PyTypeChecker
        parent_uri: URIRef = ICD10WHO[parent_code]
        label: str = v['semantic_data']['skos:prefLabel']
        # For top-most categories, parent is different:
        parent: URIRef = OWL.Thing if str(parent_uri) == 'https://icd.who.int/browse10/2019/en#/2019' else parent_uri

        graph.add((uri, RDF.type, OWL.Class))
        graph.add((uri, RDFS.subClassOf, parent))
        graph.add((uri, SKOS.prefLabel, Literal(label)))
        graph.add((uri, SKOS.notation, Literal(code)))

    with open(SAVE_PATH, 'w') as f:
        f.write(graph.serialize(format='turtle'))


def setup(parameters: Dict, env_path=ENV_FILE):
    """Setup steps"""
    # Disable HTTPS request warnings from known API
    disable_warnings(InsecureRequestWarning)

    # Set env variables in parameters
    load_dotenv(env_path)
    parameters['client_id'] = os.getenv('CLIENT_ID')
    parameters['client_secret'] = os.getenv('CLIENT_SECRET')

    return parameters


def run(
    api_version: str, language: str, release: str, client_id: str, client_secret: str, use_cache: bool
) -> Dict:
    """Run the ingest.

    :param language: e.g. en
    :param release: Put the year of the release, e.g. `2019`, or just put `latest`, and this tool will look for and
      select the latest release.
    :param client_id: Get your own from WHO API.
    :param client_secret: Get your own from WHO API.

    Side effects: Caches
    """
    # Setup
    token = _get_auth_token(client_id, client_secret)
    headers_json = {
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/json',
        'Accept-Language': language,
        'API-Version': api_version}
    traversal_state = _initialize_state(release, headers_json)

    # Get code system structure from API or cache
    code_system: Dict = download_everything(traversal_state, headers_json, use_cache)

    # Convert to RDF and save
    save_rdf(code_system)

    return code_system


# Execution
params = setup(DEFAULT_PARAMS)
run(**params)
