import openai
import pyodbc
import os

# Set OpenAI API paraemters
openai.api_type = "azure"
openai.api_base = os.getenv("OPENAI_API_BASE")
openai.api_version = "2023-09-15-preview"
openai.api_key = os.getenv("OPENAI_API_KEY")

# English to T-SQL translation
def translate_to_sql(english_text, tables_metadata):
    prompt = "### Synapse SQL tables, with their properties:\n#\n"
    for table_name, table_properties in tables_metadata.items():
        prompt += f"#{table_name}{table_properties}\n"
    prompt += f"#\n"
    prompt += f"### Using T-SQL syntax only, {english_text}. End the SQL statement with ;.\n\n"
    prompt += "SELECT"
    response = openai.Completion.create(
        engine="gpt-35-turbo-instruct",
        prompt=prompt,
        temperature=0,
        max_tokens=100,
        top_p=0.5,
        frequency_penalty=0,
        presence_penalty=0,
        best_of=1,
        stop=[";"]
    )
    return f"SELECT {response.choices[0].text.strip()}"


# Connect to Synapse SQL pool
def connect_to_synapse():
    server = os.getenv("SYNAPSE_SERVER")
    database = os.getenv("SYNAPSE_DATABASE")
    username = os.getenv("SYNAPSE_USERNAME")
    password = os.getenv("SYNAPSE_PASSWORD")
    driver = '{ODBC Driver 17 for SQL Server}'
    connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    # connection = pyodbc.connect(f'DRIVER={driver};SERVER={server};DATABASE={database};Authentication=ActiveDirectoryInteractive')
    return connection

# Execute T-SQL query
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except pyodbc.Error as e:
        print(f"An error occurred: {e}")

# Get schema's tables metadata
def get_tables_metadata(connection):
    cursor = connection.cursor()
    query = """
    SELECT concat(t.TABLE_SCHEMA, '.', t.TABLE_NAME) as TABLE_NAME, c.COLUMN_NAME
    FROM INFORMATION_SCHEMA.TABLES t, INFORMATION_SCHEMA.COLUMNS c
    WHERE t.TABLE_SCHEMA = 'pbi'
    AND t.TABLE_NAME NOT IN ('sysdiagrams', 'AdventureWorksDWBuildVersion', 'DatabaseLog')
    AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
    AND c.TABLE_NAME = t.TABLE_NAME;
    """
    cursor.execute(query)
    tables = cursor.fetchall()
    tables_dict = {}
    for table in tables:
        table_name = table[0]
        if table_name not in tables_dict:
            tables_dict[table_name] = []
        tables_dict[table_name].append(table[1])
    return tables_dict

# Main function
def main():
    english_text = "What are the top 10 products (using english name) per sales?"
    connection = connect_to_synapse()
    tables_metadata = get_tables_metadata(connection)
    sql_query = translate_to_sql(english_text, tables_metadata)
    results = execute_query(connection, sql_query)
    for row in results:
        print(row)

# Run main function
if __name__ == "__main__":
    main()