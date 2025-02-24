import json
import os
import glob
from google.cloud import bigquery

def get_table_list(client, project, dataset):
    """Get list of tables in a GCP dataset."""
    query = f"""SELECT table_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`"""
    query_job = client.query(query)  # API request
    rows = query_job.result()        # Waits for query to finish
    return [row.table_name for row in rows]

def get_table_schema(client, project, dataset, table):
    """
    Fetches the schema for a given BigQuery table.
    Returns a sorted list of dictionaries with schema details (name and type).
    """
    table_ref = client.get_table(f"{project}.{dataset}.{table}")
    # Only include 'name' and 'type' since we are ignoring 'mode'
    schema = [{
        'name': field.name,
        'type': field.field_type,
    } for field in table_ref.schema]
    return sorted(schema, key=lambda x: x['name'])

def load_local_schema(directory, base_table):
    """
    Loads the local schema JSON file for the given base table.
    Assumes files follow the pattern: <base_table>_<date>.json.
    """
    pattern = os.path.join(directory, f"{base_table}*.json")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No schema file found for table '{base_table}' in directory '{directory}'")
    # Load the first matching file.
    with open(files[0], "r") as f:
        schema = json.load(f)
    # Assume local schema already only contains 'name' and 'type'
    return sorted(schema, key=lambda x: x['name'])

def compare_schemas(schema_old, schema_new):
    """
    Compares two table schemas based solely on the 'type' field.
    Returns a dictionary with differences:
      - 'missing_in_new': fields in the old schema but missing in the new.
      - 'missing_in_old': fields in the new schema but missing in the old.
      - 'field_differences': differences in the common fields (comparing 'type').
    """
    dict_old = {field['name']: field for field in schema_old}
    dict_new = {field['name']: field for field in schema_new}
    
    differences = {}
    missing_in_new = set(dict_old.keys()) - set(dict_new.keys())
    missing_in_old = set(dict_new.keys()) - set(dict_old.keys())
    
    if missing_in_new:
        differences['missing_in_new'] = list(missing_in_new)
    if missing_in_old:
        differences['missing_in_old'] = list(missing_in_old)
    
    field_diffs = {}
    for col in set(dict_old.keys()).intersection(dict_new.keys()):
        if dict_old[col].get('type') != dict_new[col].get('type'):
            field_diffs[col] = {'old': dict_old[col].get('type'),
                                'new': dict_new[col].get('type')}
    if field_diffs:
        differences['field_differences'] = field_diffs
        
    return differences

def compare_table_pair(client, old_project, old_dataset, old_table, new_schema_dir, new_table):
    """
    Compares the schema for a table from BigQuery (old) to a schema loaded from a local JSON file (new).
    """
    schema_old = get_table_schema(client, old_project, old_dataset, old_table)
    schema_new = load_local_schema(new_schema_dir, new_table)
    return compare_schemas(schema_old, schema_new)

if __name__ == "__main__":
    # --- Configuration ---
    old_project = "nih-nci-dceg-connect-prod-6d04"  # Old project ID (BigQuery)
    old_dataset = "FlatConnect"                     # Old dataset in BigQuery
    new_schema_dir = "warren_schemas"               # Directory with new schema JSON files

    # Initialize the BigQuery client.
    client = bigquery.Client(project=old_project)
    
    # Get list of tables in the dataset.
    tables = get_table_list(client, old_project, old_dataset)
    
    # Dictionary to collect schema differences.
    all_differences = {}
    
    # Loop through each table in the dataset.
    for table in tables:
        # Derive the base table name.
        # If the table name ends with "_JP", remove it.
        if table.endswith("_JP"):
            base_table = table[:-3]
        else:
            base_table = table
        
        try:
            differences = compare_table_pair(client, old_project, old_dataset, table, new_schema_dir, base_table)
            all_differences[base_table] = {
                "old_table": f"{old_project}.{old_dataset}.{table}",
                "new_schema": f"{new_schema_dir}/{base_table}*.json",
                "differences": differences
            }
            print(f"Differences for table '{table}' (base: '{base_table}'):")
            print(json.dumps(differences, indent=2))
            print("\n" + "="*80 + "\n")
        except Exception as e:
            print(f"Error comparing table '{table}' (base: '{base_table}'): {e}")
    
    # Write all differences to a JSON file.
    output_filename = "schema_comparison_results.json"
    with open(output_filename, "w") as f:
        json.dump(all_differences, f, indent=2)
    
    print(f"Comparison results written to {output_filename}")
