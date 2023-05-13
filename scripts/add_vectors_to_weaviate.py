import os
import weaviate
from weaviate.util import generate_uuid5

# Instantiate the client with the auth config
client = weaviate.Client(
    url="https://abcd.weaviate.network",  # Replace w/ your endpoint
    # auth_client_secret=weaviate.auth.AuthApiKey(api_key="<YOUR-WEAVIATE-API-KEY>"),  # Replace w/ your API Key for the Weaviate instance
    additional_headers={
        "X-HuggingFace-Api-Key": os.environ["HUGGING_FACE_ACCESS_TOKEN"],
        "X-OpenAI-Api-Key": os.environ["OPENAI_API_KEY_PROTOTYPING"],
    },
)


all_fields: list[str] = [
    "description",
    "name",
    "location_country",
    "location_city",
    "website",
    "number_of_employees_on_linkedin",
    "number_of_employees",
]

properties: list[dict] = [
    {"name": "description", "dataType": ["text"]},
    {"name": "name", "dataType": ["text"]},
    {"name": "location_country", "dataType": ["text"]},
    {"name": "location_city", "dataType": ["text"]},
    {"name": "website", "dataType": ["text"]},
    {"name": "number_of_employees_on_linkedin", "dataType": ["int"]},
    {"name": "number_of_employees", "dataType": ["text"]},
]

companies: list[dict] = [
    {
        "description": "Since 2017, the number of Python users has been increasing by millions annually. The vast majority of these people leverage Python as a tool to solve problems at work. Our mission is to make them autonomous when they create and use data in their organizations. For this end, we are building an open source Python library called data load tool (dlt). Our users use dlt in their Python scripts to turn messy, unstructured data into regularly updated datasets. It empowers them to create highly scalable, easy to maintain, straightforward to deploy data pipelines without having to wait for help from a data engineer. We are dedicated to keeping dlt an open source project surrounded by a vibrant, engaged community. To make this sustainable, dltHub stewards dlt while also offering additional software and services that generate revenue (similar to what GitHub does with Git). dltHub is based in Berlin and New York City. It was founded by data and machine learning veterans. We are backed by Dig Ventures and many technical founders from companies such as Hugging Face, Instana, Matillion, Miro, and Rasa.",
        "name": "dltHub",
        "location_country": "DE",
        "location_city": "Berlin",
        "website": "https://dlthub.com",
        "number_of_employees_on_linkedin": 8,
        "number_of_employees": "2-10",
    },
    {
        "description": "Weaviate is a cloud-native, real-time vector database that allows you to bring your machine-learning models to scale. There are extensions for specific use cases, such as semantic search, plugins to integrate Weaviate in any application of your choice, and a console to visualize your data.",
        "name": "Weaviate",
        "location_country": "Amsterdam",
        "location_city": "Netherlands",
        "website": "https://weaviate.io",
        "number_of_employees_on_linkedin": 41,
        "number_of_employees": "11-50",
    },
    {
        "description": "Creatext is an AI sales platform that automatically conducts research on your prospects and generates hyper-personalized sales messages that are so good that your prospects will think you spent 30 minutes researching them.",
        "name": "Creatext",
        "location_country": "DE",
        "location_city": "Berlin",
        "website": "https://creatext.ai",
        "number_of_employees_on_linkedin": 10,
        "number_of_employees": "2-10",
    },
    # Add more companies, ideally 200+
]


# create schema
schema = {
    "classes": [
        {
            "class": "LinkedInCompany",
            "properties": properties,
            "vectorIndexType": "hnsw",
            "vectorIndexConfig": {"distance": "cosine"},
            "vectorizer": "text2vec-openai",
            "moduleConfig": {"text2vec-openai": {"model": "ada", "modelVersion": "002", "type": "text"}},
        }
    ],
}

# client.schema.delete_all()
client.schema.create(schema)


client.batch.configure(
    batch_size=5,
)

with client.batch as batch:
    for company in companies:
        uuid_company = generate_uuid5(company, "LinkedInCompany")
        batch.add_data_object(
            data_object=company,
            class_name="LinkedInCompany",
            uuid=uuid_company,
        )


companies_in_db = client.query.aggregate("LinkedInCompany").with_meta_count().do()
print(f"Number of companies in the DB: {len(companies_in_db)}")
