from flask import Flask, jsonify
import flask

import elastic_functions
from elastic_functions import ESKNN

app = Flask(__name__)

# Check the index
esknn = ESKNN()


@app.route('/', methods=['GET'])
def home():
    return 'Elastic search API is online :)'

@app.route('/api/get_current_index', methods=['GET'])
def get_names():
    '''
    Returns a string that tells us which index will be searched.
    Usage:
        print(requests.get('http://localhost:9090/api/get_current_index').text)
    Result:
        {
          "current_name": "preprints-biorxiv",
          "status": 200
        }
    '''
    return jsonify(
        {
            "status": 200,
            "current_name": esknn.get_index_name()
        }
    )


@app.route('/api/set_current_index', methods=['GET','POST'])
def set_names():
    '''
    Change the elasticsearch index to search. In the example below, the json body has a key called 'input',
    this is used to set the new database.
    Usage example on python console:

    print(requests.post('http://localhost:9090/api/set_current_index', json={"input":"preprints-medrxiv"}).text)

    Returns:
    {
    "current_name": "preprints-medrxiv",
    "old_name": "preprints-biorxiv",
    "status": 200
    }

    :return: json string confirming the index set after this function call, as well as the old name to see if anything changed.


    '''
    data = flask.request.json
    new_name=data.get('input', False)
    if new_name:
        old_name=esknn.get_index_name()
        esknn.set_index_name(new_name)

        return jsonify(
            {
                "status": 200,
                "old_name": old_name,
                "current_name": esknn.get_index_name()
            }
        )
    else:
        return jsonify(
            {
                "error": "Error, make sure to send a json request body that contains data in this format: {\"input\":\"medrxiv-preprints\"}",
            }
        )


# # Insert a new document route
# @app.route('/api/insert_document')
# def insert_document():
#     data = flask.request.json
#
#     esknn.insert_document(data)
#
#     return jsonify(
#         {
#             "status": 200
#         }
#     )

@app.route('/api/direct_retrieval', methods=['GET','POST'])
def direct_retrieval():

    """
    Combining search_query and get_document calls. When used in this function, the elastic database is searched only one.

    input: A string that is an elastic-style search query, for example "Abstract:schizo* AND Authors:*dams"
    index: The index name to search, for example 'tblreport'


    usage: print(requests.post('http://localhost:9090/api/direct_retrieval', json={"input":"Abstract:schizo* AND Authors:*dams", "index":"tblreport"}).text)
    :return:
    """
    data = flask.request.json

    query = data.get('input', False)
    indexname= data.get('index', False)
    ret_field=""
    esknn.set_index_name(indexname)

    if query:
        result = esknn.search_query(query,ret_field=ret_field,return_docs=True)



    else:
        return {
                "status": 400,
                "response": "Your request did not include a search query. Try including a key-value pair in this format: {\"input\":\"title:\"genome dried\"~15\"} "
            }

    return {
        "status": 200,
        "response":result
    }

@app.route('/api/studyfromreportid', methods=['GET','POST'])
def study_from_reportid():

    """
    Combining api calls to retrieve the studies that are associated with report hits.

    JSON param 'return_as':
        'dict': simply returns a list of dictionaries.
        'ris': TODO we can return a RIS fiel as single string for direct reference download, if needed?
        'pubmed': TODO


    usage: print(requests.post('http://localhost:9090/api/studyfromreportid', json={"input":[149,218]}).text)
    :return:
    """
    data = flask.request.json

    ids = data.get('input', False)
    print(ids)

    esknn.set_index_name("tblstudyreport")

    ret_field="CRGReportID"#the field to search

    if ids:
        result = esknn.retrieve_documents(ids,ret_field=ret_field)#get study ID data from report ids
        ids=[d['CRGStudyID'] for d in result]


        esknn.set_index_name("tblstudy")#get study metadata
        ret_field = "CRGStudyID"  # the field to search
        #
        result = esknn.retrieve_documents(ids, ret_field=ret_field)



    else:
        return {
                "status": 400,
                "response": "Your request did not include a search query. Try including a key-value pair in this format: {\"input\":\"title:\"genome dried\"~15\"} "
            }

    return {
        "status": 200,
        "response":result
    }


# Search documents route
@app.route('/api/search_query', methods=['GET','POST'])
def search_query():

    """
    Make a search via query string but return only one field specified by ret_field. Per default this should be OpenAlex ID.

    Usage:
        print(requests.post('http://localhost:9090/api/search_query', json={"input":"title:\"genome dried\"~15", "ret_field":"title"}).text)

    returns:
        {
          "response": [
            "Whole genome sequencing of Plasmodium falciparum from dried blood spots using selective whole genome amplification",
            "Optimization of whole-genome sequencing of Plasmodium falciparum from low-density dried blood spot samples",
            "Low-Pass Whole Genome Bisulfite Sequencing of Neonatal Dried Blood Spots Identifies a Role for RUNX1 in Down Syndrome DNA Methylation Profiles"
          ],
      "status": 200
}

    :return:
    """
    data = flask.request.json

    query = data.get('input', False)

    ret_field=data.get("ret_field", False)
    if not ret_field:
        ret_field=elastic_functions.RET_FIELD

    if query:
        result = esknn.search_query(query,ret_field)
    else:
        return {
                "status": 400,
                "response": "Your request did not include a search query. Try including a key-value pair in thiss format: {\"input\":\"title:\"genome dried\"~15\"} "
            }




    return {
        "status": 200,
        "response":result
    }

    # field_name = data['field_name']
    # query = data['query']
    #
    # result = esknn.search_document(query, field_name)
    #
    # documents = []
    #
    # hits = result['hits']['hits']
    #
    # for hit in hits:
    #     documents.append(hit['_source'])
    #
    # return {
    #     "status": 200,
    #     "documents": documents
    # }


# Search documents route
@app.route('/api/get_documents', methods=['GET','POST'])
def get_documents():

    """
    JSON param 'input': Submit a list of OpenAlex IDs or any other list of items for any specific field for which all matching documents are retrieved.
    Default: give OA IDs and retrieve whole documents.

    JSON param 'ret_field': For non-default requests, give 'ret_field' optional parameter, eg to get references
    by 'title', 'DOI', 'PMCID' or anything indexed in this index. Those can also be searched by /api/search_query when DOI or else is given as 'ret_field' parameter there.
    Caution: If anything oter than OA ID is used for ID-selection-retrieval then there may be missing records due to incomplete fields.

    JSON param 'return_as':
        'dict': simply returns a list of dictionaries.
        'ris': TODO we can return a RIS fiel as single string for direct reference download, if needed?


    Usage:
        print(requests.post('http://localhost:9090/api/get_documents',
                json={"input": [
                    "Whole genome sequencing of Plasmodium falciparum from dried blood spots using selective whole genome amplification",
                    "Optimization of whole-genome sequencing of Plasmodium falciparum from low-density dried blood spot samples",
                    "Low-Pass Whole Genome Bisulfite Sequencing of Neonatal Dried Blood Spots Identifies a Role for RUNX1 in Down Syndrome DNA Methylation Profiles"
                  ],
                  "ret_field": "title"}).text)

    returns:
        {
              "response": [
                {
                  "abstract": "Translating genomic technologies into healthcare applications for the malaria parasite Plasmodium falciparum has been limited by the technical and logistical difficulties of obtaining high quality clinical samples from the field. Sampling by dried blood spot (DBS) finger-pricks can be performed safely and efficiently with minimal resource and storage requirements compared with venous blood (VB). Here, we evaluate the use of selective whole genome amplification (sWGA) to sequence the P. falciparum genome from clinical DBS samples, and compare the results to current methods using leucodepleted VB. Parasite DNA with high (> 95%) human DNA contamination was selectively amplified by Phi29 polymerase using short oligonucleotide probes of 8-12 mers as primers. These primers were selected on the basis of their differential frequency of binding the desired (P. falciparum DNA) and contaminating (human) genomes. Using sWGA method, we sequenced clinical samples from 156 malaria patients, including 120 paired samples for head-to-head comparison of DBS and leucodepleted VB. Greater than 18-fold enrichment of P. falciparum DNA was achieved from DBS extracts. The parasitaemia threshold to achieve >5x coverage for 50% of the genome was 0.03% (40 parasites per 200 white blood cells). Over 99% SNP concordance between VB and DBS samples was achieved after excluding missing calls. The sWGA methods described here provide a reliable and scalable way of generating P. falciparum genome sequence data from DBS samples. Our data indicate that it will be possible to get good quality sequence data on most if not all drug resistance loci from the majority of symptomatic malaria patients. This technique overcomes a major limiting factor in P. falciparum genome sequencing from field samples, and paves the way for large-scale epidemiological applications.",
                  "authors": "Samuel O. Oyola;Cristina Valente Ariani;William Hamilton;Mihir Kekre;Lucas Amenga-Etego;Anita Ghansah;Gavin R. Rutledge;Seth Redmond;Magnus Manske;Dushyanth Jyothi;Chris G. Jacob;Thomas Otto;Kirk Rockett;Chris I. Newbold;Matthew Berriman;Dominic P. Kwiatkowski;",
                  "doi": "10.1101/067546",
                  "published": "10.1186/s12936-016-1641-7",
                  "title": "Whole genome sequencing of Plasmodium falciparum from dried blood spots using selective whole genome amplification"
                },
                {
                  "abstract": "BackgroundWhole-genome sequencing (WGS) is becoming increasingly useful to study the biology, epidemiology, and ecology of malaria parasites. Despite ease of sampling, DNA extracted from dried blood spots (DBS) has a high ratio of human DNA compared to parasite DNA, which poses a challenge for downstream genetic analyses. We evaluated the effects of multiple methods for DNA extraction, digestion of methylated DNA, and amplification on the quality and fidelity of WGS data recovered from DBS.\r\n\r\nResultsAt 100 parasites/L, Chelex-Tween-McrBC samples had higher coverage (5X depth = 93% genome) than QIAamp extracted samples (5X depth = 76% genome). The two evaluated sWGA primer sets showed minor differences in overall genome coverage and SNP concordance, with a newly proposed combination of 20 primers showing a modest improvement in coverage over those previously published.\r\n\r\nConclusionsOverall, Tween-Chelex extracted samples that were treated with McrBC digestion and are amplified using 6A10AD sWGA conditions had minimal dropout rate, higher percentages of coverage at higher depth, and more accurate SNP concordance than QiaAMP extracted samples. These findings extend the results of previously reported methods, making whole genome sequencing accessible to a larger number of low density samples that are commonly encountered in cross-sectional surveys.",
                  "authors": "Teyssier, N. B.; Chen, A.; Duarte, E. M.; Sit, R.; Greenhouse, B.; Tessema, S. K.",
                  "doi": "10.1101/835389",
                  "published": "10.1186/s12936-021-03630-4",
                  "title": "Optimization of whole-genome sequencing of Plasmodium falciparum from low-density dried blood spot samples"
                },
                {
                  "abstract": "Neonatal dried blood spots (NDBS) are a widely banked sample source that enable retrospective investigation into early-life molecular events. Here, we performed low-pass whole genome bisulfite sequencing (WGBS) of 86 NDBS DNA to examine early-life Down syndrome (DS) DNA methylation profiles. DS represents an example of genetics shaping epigenetics, as multiple array-based studies have demonstrated that trisomy 21 is characterized by genome-wide alterations to DNA methylation. By assaying over 24 million CpG sites, thousands of genome-wide significant (q < 0.05) DMRs that distinguished DS from typical development (TD) and idiopathic developmental delay (DD) were identified. Machine learning feature selection refined these DMRs to 22 loci. The DS DMRs mapped to genes involved in neurodevelopment, metabolism, and transcriptional regulation. Based on comparisons to previous DS methylation studies and reference epigenomes, the hypermethylated DS DMRs were significantly (q < 0.05) enriched across tissues while the hypomethylated DS DMRs were significantly (q < 0.05) enriched for blood-specific chromatin states. A [~]28 kb block of hypermethylation was observed on chromosome 21 in the RUNX1 locus, which encodes a hematopoietic transcription factor whose binding motif was the most significantly enriched (q < 0.05) overall and specifically within the hypomethylated DMRs. Finally, we also identified DMRs that distinguished DS NDBS based on the presence or absence of congenital heart disease (CHD). Together, these results not only demonstrate the utility of low-pass WGBS on NDBS samples for epigenome-wide association studies, but also provide new insights into the early-life mechanisms of epigenomic dysregulation resulting from trisomy 21.",
                  "authors": "Laufer, B. I.; Hwang, H.; Jianu, J. M.; Mordaunt, C. E.; Korf, I. F.; Hertz-Picciotto, I.; Lasalle, J. M.",
                  "doi": "10.1101/2020.06.18.157693",
                  "published": "10.1093/hmg/ddaa218",
                  "title": "Low-Pass Whole Genome Bisulfite Sequencing of Neonatal Dried Blood Spots Identifies a Role for RUNX1 in Down Syndrome DNA Methylation Profiles"
                }
              ],
              "status": 200
            }

    :return:
    """
    data = flask.request.json

    ids = data.get('input', False)
    if not ids:
        return {
            "status": 400,
            "response": "Your query did not contain the JSON entry for 'input' key. You need to give a list of values, eg {'input'=['W1234', 'W5678']}"
        }

    ret_field=data.get("ret_field", False)
    if not ret_field:
        ret_field=elastic_functions.RET_FIELD

    if ids:
        result = esknn.retrieve_documents(ids,ret_field=ret_field)

    return {
        "status": 200,
        "response":result
    }


# Field-based search route
# @app.route('/api/search_field')
# def search_document():
#     """
#     Untested function, search a specific field.
#     :return:
#     """
#     data = flask.request.json
#
#     field_name = data['field_name']
#     query = data['query']
#
#     result = esknn.search_document(query, field_name)
#
#     documents = []
#
#     hits = result['hits']['hits']
#
#     for hit in hits:
#         documents.append(hit['_source'])
#
#     return {
#         "status": 200,
#         "documents": documents
#     }