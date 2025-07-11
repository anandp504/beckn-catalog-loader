from bulk_loader import BulkLoader
from parse_catalog import ParseCatalog
from flatten_catalog import FlattenCatalog

if __name__ == "__main__":
  index_name = "diagnostics"
  base_dir = f"/home/ubuntu/beckn/catalog_data/{index_name}"
  # Source Catalog File in the format of on_search Beckn object
  beckn_catalog_file = f"{base_dir}/{index_name}_catalog_on_search.json"

  # These are auto generated and need not be created manually
  beckn_catalog_exploded_file = f"{base_dir}/{index_name}_catalog_items.json"
  beckn_catalog_flattened_file = f"{base_dir}/{index_name}_catalog_items_flattened.json"
  

  try:

    parse_catalog = ParseCatalog(beckn_catalog_file, beckn_catalog_exploded_file)
    parse_catalog.parse_catalog()

    flatten_catalog = FlattenCatalog(beckn_catalog_exploded_file, beckn_catalog_flattened_file)
    flatten_catalog.flatten_catalog()

    bulk_loader = BulkLoader(beckn_catalog_flattened_file, index_name)
    bulk_loader.bulk_load_data()
  except Exception as e:
    print(f"Error: {str(e)}")