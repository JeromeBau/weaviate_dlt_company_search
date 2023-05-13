import streamlit as st
import duckdb
from weaviate_company_search import search_term_to_category_mapping

def connect_to_db():
    return duckdb.connect('weaviate_company_search.duckdb')

conn = connect_to_db()

def run_query(query):
    result = conn.execute(query)
    return result.fetchdf()

categories = search_term_to_category_mapping.values()

def get_data_for_category(category):
    query = f"""
    WITH ranked_data AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY name ORDER BY name) AS row_num
        FROM weaviate_company_search_data.weaviate_company_search_resources
        WHERE category='{category}'
    )
    SELECT name, description, website, number_of_employees_on_linkedin, location_country, location_city
    FROM ranked_data
    WHERE row_num = 1
    ;"""
    return run_query(query)

def main():
    st.title('New and upcoming startups across different categories')
    for category in categories:
        data = get_data_for_category(category)
        st.write(f"## {category}")
        st.write(data)

if __name__ == '__main__':
    main()


    