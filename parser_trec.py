import os
from elasticsearch7 import Elasticsearch, helpers
from elasticsearch7.helpers import bulk


directory_path = "/home/ec2-user/AP_DATA/ap89_collection"
elasticsearch_host = "http://localhost:9200"
es = Elasticsearch(hosts=[elasticsearch_host])

def parse_file(file_path):
    documents = []
    with open(file_path, 'r', encoding='ISO-8859-1') as file:
        in_document = False
        capturing_content = False
        doc_id = None
        doc_content = []

        for line in file:
            line = line.strip()

            if line.startswith('<DOC>'):
                in_document = True
                doc_content = []

            elif line.startswith('</DOC>'):
                in_document = False
                if doc_id:
#                    process_document(doc_id, doc_content)
                    documents.append((doc_id, doc_content))
                doc_id = None

            elif in_document:
                if line.startswith('<DOCNO>'):
                    doc_id = line.split('<DOCNO>')[1].split('</DOCNO>')[0].strip()

                elif line.startswith('<TEXT>'):
                    capturing_content = True

                elif line.startswith('</TEXT>'):
                    capturing_content = False

                elif capturing_content:
                    doc_content.append(line)
    return documents

# def process_document(doc_id, doc_content):
#     print(f"Document ID: {doc_id}")
#     print("Document Content:")
#     for line in doc_content:
#         print(line)


def process_documents(filename, documents):
    # Print the filename along with document information
    # print(f"File: {filename}")
    # print(f"Document ID: {doc_id}")
    # print("Document Content:")
    # for line in doc_content:
    #     print(line)
    # print()
    #index_name = filename

    #actions = [
    #    {
    #        "_op_type": "index",
    #        "_index": index_name,
    #        "_source": document
    #    }
    #    for document in documents
    #]
    #success, failed = bulk(es, actions)
    for doc in documents:
        #print(doc["text"])
        docno = doc["id"]
        docText = doc["text"]
        try:
            es.index(index="ap89_collection",
                         doc_type="document",
                         id = docno,
                         body={
                             "docno": docno,
                             "text": docText
                         })
        except:
            print(f"Indexed {success} documents successfully. Failed to index {failed} documents.")





def process_directory(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        documents_pair = parse_file(file_path)
        documents = []

        # Print the collected documents for each file
        for doc_id, doc_content in documents_pair:
            document = {
                "id": doc_id,
                "text": doc_content
            }
            documents.append(document)

        process_documents(filename, documents)

if __name__ == "__main__":
    process_directory(directory_path)
