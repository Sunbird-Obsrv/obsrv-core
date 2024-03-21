from elasticsearch import Elasticsearch

# Create an Elasticsearch client
es = Elasticsearch([{'host': 'elastic', 'port': 9200}])  

# Define the index name and document data
index_name = 'my_index'  # Change to your desired index name
document_data = {
    "firstname": "Negrah",
    "lastname": "s",
    "dob": "23/03/2003",
    "age": "19"
}

# Have used a sample data as document_data
# Ingest data into Elasticsearch using the Index API
# not using request.get to receive as I didnt found need to do so 
try:
    response = es.index(index=index_name, body=document_data)
    print("Data ingested successfully. Response:", response)
except Exception as e:
    print("Error ingesting data:", str(e))


