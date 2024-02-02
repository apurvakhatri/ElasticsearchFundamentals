from elasticsearch7 import Elasticsearch

elasticsearch_host = "http://localhost:9200"
es = Elasticsearch(hosts=[elasticsearch_host])
output = open("./AP_DATA/docstats.txt", "a")
es_index = "ap89_collection"

with open("./AP_DATA/doclengths.txt", "r") as doc:
    data = doc.read()
    doclength_data = data.split("\n")[:-1]  # Read till the second last line ie avoid last line
    doc_stats_dict = dict()

    for l in doclength_data:
        doc_id, doc_length = l.split(" ")
        doc_stats_dict.update({doc_id: int(doc_length)})  # Generate dictionary of doc_id as key and doc_length as value

    total_docs = len(doc_stats_dict.keys())
    avg_doc_length = round(sum(doc_stats_dict.values())/total_docs)

    output.write("doc_count " + str(total_docs) + "\n")
    output.write("avg_doc_length " + str(avg_doc_length) + "\n")
    print("Written: " + "doc_count = " + str(total_docs) + " avg_doc_length = " + str(avg_doc_length))
# Get vocabulary size
#result = es.search(index=es_index,
#                   doc_type="document",
#                   body={
#                       "aggs": {
#                           "unique_terms": {
#                               "cardinality": {
#                                   "field": "text"
#                               }
#                           }
#                       }
#                   })

#vocab_size = result["aggregations"]["unique_terms"]["value"]

#output.write("vocab_size " + str(vocab_size) + "\n")
output.close()
