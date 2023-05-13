import dlt
import os
import weaviate
from typing import Dict, List

fields: List[str] = ["description", "name", "location_country", "location_city", "website", "number_of_employees_on_linkedin", "number_of_employees"]

search_term_to_category_mapping = {
    "sales": "Sales tech startups",
    "machine learning": "AI startups",
    "edtech": "EdTech startups",
}

client = weaviate.Client(
    url="https://abcd.weaviate.network",  # Replace w/ your endpoint
    # auth_client_secret=weaviate.auth.AuthApiKey(api_key="<YOUR-WEAVIATE-API-KEY>"),  # Replace w/ your API Key for the Weaviate instance
    additional_headers={
        "X-HuggingFace-Api-Key": os.environ["HUGGING_FACE_ACCESS_TOKEN"],
        "X-OpenAI-Api-Key": os.environ["OPENAI_API_KEY_PROTOTYPING"],
    },
)


def search_on_weaviate(search_query: str) -> List[Dict]:
    near_text_filter = {"concepts": [search_query]}
    result = client.query.get("LinkedInCompany", fields).with_additional(["distance"]).with_near_text(near_text_filter).do()
    return result["data"]["Get"]["LinkedInCompany"]


def transform_one_company_object(company: Dict, search_term: str):
    return {
        "name": company["name"],
        "description": company["description"],
        "location_country": company["location_country"],
        "location_city": company["location_city"],
        "website": company["website"],
        "number_of_employees_on_linkedin": company["number_of_employees_on_linkedin"],
        "number_of_employees": company["number_of_employees"],
        "distance": company["_additional"]["distance"],
        "search_term": search_term,
        "category": search_term_to_category_mapping.get(search_term, search_term),
    }


@dlt.source
def weaviate_company_search_source(api_secret_key=dlt.secrets.value):
    return weaviate_company_search_resources(api_secret_key)


@dlt.resource(write_disposition="append")
def weaviate_company_search_resources(api_secret_key=dlt.secrets.value):
    for search_query in search_term_to_category_mapping:
        yield list(map(lambda company: transform_one_company_object(company, search_query), search_on_weaviate(search_query)))


if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="weaviate_company_search", destination="duckdb", dataset_name="weaviate_company_search_data")
    data = list(weaviate_company_search_resources())
    load_info = pipeline.run(weaviate_company_search_source())
