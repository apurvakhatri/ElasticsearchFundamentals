from elasticsearch7 import Elasticsearch

elasticsearch_host = "http://localhost:9200"
es = Elasticsearch(hosts=[elasticsearch_host])
es_index = "ap89_collection"

output = open("./AP_DATA/doclengths.txt", "a")

with open("../AP_DATA/doclist.txt", "r") as doc:
    for line in doc:
        if line != "":
            if line.split(" ")[0] != "0":
                doc_id = line.split(" ")[1].replace("\n", "")
                result = es.termvectors(index=es_index,
                                        doc_type="document",
                                        id=doc_id,
                                        body={                # Body of the request for getting term vectors
                                            "fields": ["text"],
                                            "term_statistics": True,
                                            "field_statistics": True
                                        })
                #print("result: ", result)

                doc_length = 0
                if len(result) > 0:
                    #text_check = result.get('_source', {}).get('text', [])
                    #print("len(text_check)", text_check)
                    #if len(text_check)!=0:
                        if 'term_vectors' in result and 'text' in result['term_vectors']:
                            for term in result['term_vectors']['text']['terms']:
                                #print("term freq: ", result['term_vectors']['text']['terms'][term]['term_freq'])
                                doc_length += result['term_vectors']['text']['terms'][term]['term_freq']
                            output.write(doc_id+" "+str(doc_length)+"\n")
                else:
                    output.write(doc_id + " " + str(0) + "\n")
                print("Finished " + doc_id + " len: " + str(doc_length))
output.close()
print("Doc length written in file")
