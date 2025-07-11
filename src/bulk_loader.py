from elasticsearch import Elasticsearch
import json
from tqdm import tqdm
import requests


class BulkLoader:
  def __init__(self, input_file, index_name):
    self.input_file = input_file
    self.index_name = index_name

# Initialize Elasticsearch client for stats only
    self.es = Elasticsearch(
        "http://localhost:9200",
        verify_certs=False,
        ssl_show_warn=False
    )

    # Define the mapping for the index
    self.index_config = {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index.mapping.ignore_malformed": True
      },
      "mappings": {
        "properties": {
          "context_domain": {"type": "keyword"},
          "context_bap_id": {"type": "keyword"},
          "context_bap_uri": {"type": "keyword"},
          "context_bpp_id": {"type": "keyword"},
          "context_location_country_name": {"type": "text"},
          "context_location_country_code": {"type": "keyword"},
          "context_location_city_name": {"type": "text"},
          "context_location_city_code": {"type": "keyword"},
          "context_location_gps": {"type": "geo_point"},
          "provider_id": {"type": "keyword"},
          "provider_descriptor_name": {
              "type": "text",
              "fields": {
                  "keyword": {"type": "keyword"}
              }
          },
          "providers_locations_gps": {"type": "geo_point"},
          "providers_locations_address": {"type": "text"},
          "providers_locations_city_name": {"type": "text"},
          "providers_locations_state_name": {"type": "text"},
          "providers_locations_country_name": {"type": "text"},
          "providers_locations_area_code": {"type": "keyword"},
          "providers_locations_gps": {"type": "geo_point"},
          "providers_fulfillments_id": {"type": "keyword"},
          "providers_fulfillments_type": {"type": "text"},
          "providers_fulfillments_rateable": {"type": "boolean"},
          "items_id": {"type": "keyword"},
          "items_descriptor_name": {
              "type": "text",
              "fields": {
                  "keyword": {"type": "keyword"}
              }
          },
          "items_descriptor_short_desc": {"type": "text"},
          "items_price_value": {"type": "float"},
          "items_price_currency": {"type": "keyword"},
          "items_quantity_available_count": {"type": "float"},
          "items_category_ids": {"type": "keyword"},
          "items_fulfillment_ids": {"type": "keyword"},
          "items_rating": {"type": "float"},
          "items_tags_list_value": {"type": "keyword"},
          "items_tags_list_descriptor_code": {"type": "keyword"},
          "items_tags_list_descriptor_name": {"type": "keyword"},
          "raw_catalog": {"type": "keyword"}
        }
      }
    }

# Index name
# INDEX_NAME = "ev_catalog"
# INDEX_NAME = "deg-ev"
# INDEX_NAME = "diagnostics"

  def create_index(self):
    """Create the index with mapping if it doesn't exist"""
    try:
        # Check if index exists
        response = requests.head(f"http://localhost:9200/{self.index_name}")
        if response.status_code == 404:
            # Create index with mapping
            response = requests.put(
                f"http://localhost:9200/{self.index_name}",
                json=index_config,
                headers={"Content-Type": "application/json"}
            )
            if response.status_code == 200:
                print(f"Created index {self.index_name} with mapping")
            else:
                print(f"Error creating index: {response.text}")
                raise Exception(response.text)
        else:
            print(f"Index {self.index_name} already exists")
    except Exception as e:
        print(f"Error creating index: {str(e)}")
        raise

  def bulk_load_data(self):
    """Bulk load data from JSON file into Elasticsearch"""
    try:
      # Read the JSON file
      with open(self.input_file, 'r') as f:
        # Count number of lines for progress bar
        total_lines = sum(1 for _ in f)
        f.seek(0)  # Reset file pointer to beginning
        
        bulk_data = []
        for line_num, line in enumerate(tqdm(f, total=total_lines, desc="Processing records"), 1):
          # Skip empty lines
          if not line.strip():
              continue
              
          try:
            # Parse JSON record
            record = json.loads(line)
            
            # Get the document ID from providers.items.id
            doc_id = record.get("items_id")
            if not doc_id:
                print(f"Warning: Missing providers_items_id at line {line_num}, skipping record")
                continue
            
            # Convert numeric fields
            if "providers_items_price_value" in record:
                record["providers_items_price_value"] = float(record["providers_items_price_value"])
            if "providers_items_quantity_available_count" in record:
                record["providers_items_quantity_available_count"] = int(record["providers_items_quantity_available_count"])
            if "providers_items_rating" in record:
                record["providers_items_rating"] = float(record["providers_items_rating"])
            
            # Prepare the bulk action with document ID
            action = {
                "index": {
                    "_index": self.index_name,
                    "_id": doc_id
                }
            }
            bulk_data.append(json.dumps(action))
            bulk_data.append(json.dumps(record))
            
            # Process in batches of 500
            if len(bulk_data) >= 1000:  # 500 documents = 1000 lines (action + source)
                bulk_body = "\n".join(bulk_data) + "\n"
                response = requests.post(
                    "http://localhost:9200/_bulk",
                    data=bulk_body,
                    headers={"Content-Type": "application/x-ndjson"}
                )
                if response.status_code != 200:
                    print(f"Error in bulk operation: {response.text}")
                elif response.json().get("errors"):
                    print(f"Errors in bulk operation: {response.json()}")
                bulk_data = []
          
          except json.JSONDecodeError as e:
              print(f"Error parsing JSON at line {line_num}: {str(e)}")
              continue
          except Exception as e:
              print(f"Error processing record at line {line_num}: {str(e)}")
              continue
        
        # Process any remaining records
        if bulk_data:
            bulk_body = "\n".join(bulk_data) + "\n"
            response = requests.post(
                "http://localhost:9200/_bulk",
                data=bulk_body,
                headers={"Content-Type": "application/x-ndjson"}
            )
            if response.status_code != 200:
                print(f"Error in bulk operation: {response.text}")
            elif response.json().get("errors"):
                print(f"Errors in bulk operation: {response.json()}")
              
      print("Bulk loading completed successfully")
        
    except Exception as e:
        print(f"Error during bulk loading: {str(e)}")
        raise
