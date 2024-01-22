import config
from elasticsearch import Elasticsearch, helpers
from typing import Dict
import warnings
warnings.filterwarnings(action='ignore')
from elasticsearch_dsl import Search


INDEX_NAME = config.INDEX_NAME#default index to connect to, unless specified differently per API request. eg 'preprints-biorxiv'
ESKNN_HOST = config.ESKNN_HOST#where elastic lives, eg 'http://localhost:9200'
RET_FIELD = config.RET_FIELD#main field to use as unique ID when query and actual document retrieval are split (like PubMed API), eg.
RETURN_AS = config.RETURN_AS#Output format, eg 'dict', 'ris' or whatever is implemented (see utils function 'format_output' for current options.

####################Default setup to conect to main index
es = Elasticsearch(
    hosts=[ESKNN_HOST]
)

if es.ping():
    print('elastic_functions.py: Connected to elastic host <{}>'.format(config.ESKNN_HOST))
else:
    print('elastic_functions.py: Unable to connect to elastic host <{}>'.format(config.ESKNN_HOST))


class ESKNN():
    ''' This class creates an instance of Elasticsearch
    '''

    def __init__(self) -> None:
        self.current_index_name=INDEX_NAME
        self.search_context = Search(using=es, index=self.current_index_name)

        pass

    def get_index_name(self)-> str:
        ''' Get index name currently specified\n
                    ----------------
                    Takes -> None\n
                    Returns -> str
                '''
        print('elastic_functions.py: Using elastic index {}'.format(self.current_index_name))
        return self.current_index_name

    def set_index_name(self, new_name)-> str:
        ''' Set a new index to be searched. This can include wildcards,
        eg to search indices preprints-medrxiv AND preprints-biorxiv
        set index name to preprints-* to select all matching indices\n
                    ----------------
                    Takes -> str\n
                    Returns -> None
                '''
        self.current_index_name = new_name
        if es.indices.exists(index=new_name):
            print('elastic_functions.py: New index {} existed and was selected'.format(new_name))
        else:
            print('elastic_functions.py: WARNING:New index {} did not exist, '
                  'ignore this if you are using a wildcard to search multiple indices'.format(new_name))
        self.search_context = Search(using=es, index=self.current_index_name)#chnge the search context for search queries


        return self.current_index_name

    def create_index(self) -> None:
        ''' Create new index\n
            ----------------
            Takes -> None\n
            Returns -> None
        '''
        body = {}

        if es.indices.exists(index=INDEX_NAME):
            print("elastic_functions.py: Index {} exists".format(INDEX_NAME))
            return 0

        try:
            result = es.indices.create(
                index=INDEX_NAME,
                body=body,
                ignore=400
            )
            if 'error' in result:
                print("elastic_functions.py: Index {} creation error".format(INDEX_NAME))
                return 2
            else:
                print("elastic_functions.py: Index {} created".format(INDEX_NAME))
                return 1
        except:
            return 0

    def search_query(self, query,ret_field=RET_FIELD, return_docs=False) -> Dict:
        ''' Search a index using a query_string and return only one field, most likely id field\n
            --------------
        '''

        search = self.search_context.query('query_string',query=query)
        response = search.execute()

        if response.success():  # this just returns a true/false value. So if the success is true, we show the results :)
            if return_docs:
                dat = [d.to_dict() for d in search.scan()]
            else:
                dat = [d.to_dict().get(ret_field,'error:field does not exist?!') for d in search.scan()]

        print('Found {} search results!'.format(len(dat)))

        return dat

    def retrieve_documents(self, id_list, ret_field, return_docs=False) -> list:
        ''' Get a list of values and also potentially a field to search on. Then retrieve all these values. \n
        id_list: list of anything, eg [234,456,459]
        ret_field: string specifying which field to be filtered
            --------------
        '''



        id_list= list(set(id_list))#make sure IDs are unique
        id_list=["{}:\"{}\"".format(ret_field, str(i).strip()) for i in id_list]#for each ID add field to search in (ie OpenAlex ID field) and chaiin up the query
        query=" OR ".join(id_list)#finish query

        print(query)

        search = self.search_context.query('query_string', query=query)
        response = search.execute()

        if response.success():  # this just returns a true/false value. So if the success is true, we show the results :)
            dat = [d.to_dict() for d in search.scan()]

        return dat



    # def search_document(self, query, field_name) -> Dict:
    #     ''' Search a index\n
    #         --------------
    #         Takes -> fvecs\n
    #         Returns -> dict from es
    #     '''
    #     result = es.search(
    #         request_timeout=30,
    #         index=INDEX_NAME,
    #         body={
    #             'query': {
    #                 'match': {
    #                     field_name: {
    #                         'query': query
    #                     }
    #                 }
    #             }
    #         }
    #     )
    #
    #     return result

    def insert_document(self, document) -> None:
        ''' Insert new fvecs to the index\n
            -----------------------------
            Takes -> document\n
            Returns -> None
        '''
        rows = [
            {
                '_index': INDEX_NAME,
                '_source': document
            }
        ]

        result = helpers.bulk(
            es,
            rows,
            request_timeout=30
        )

        return result