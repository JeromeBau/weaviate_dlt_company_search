# DLT Pipeline from Weaviate to DuckDB
A DltHub Pipeline that uses a weaviate database as source. 

Source:
A Weaviate cluster with company meta data.

Target:
In a streamlit app, we want to see startups listed by sector. 


## Prerequisits: Weaviate Cluster
1. Add a Weaviate Cluster by following the instructions in the [Weaviate Documentation](https://weaviate.io/developers/weaviate) and update the url in the Weaviate client code.
2. Add company data to the Weaviate Cluster, using the script `scripts/add_vectors_to_weaviate.py`. A few examples are provided but to make it work sensibly, more data should be used, e.g. by scraping company catalogues or LinkedIn.

## Build the Pipeline
Build the pipeline
```
cd weaviate_company_seach
python weaviate_company_search.py
```

Run the streamlit app
```
streamlit run app.py
```
