from bulk_loader import BulkLoader
from parse_catalog import ParseCatalog
from flatten_catalog import FlattenCatalog

if __name__ == "__main__":
  beckn_catalog_file = "data/diagnostics/diagnostics_catalog_on_search.json"
  beckn_catalog_exploded_file = "data/diagnostics/diagnostics_catalog_items.json"
  beckn_catalog_flattened_file = "data/diagnostics/diagnostics_catalog_items_flattened.json"
  index_name = "diagnostics"

  try:

    parse_catalog = ParseCatalog(beckn_catalog_file, beckn_catalog_exploded_file)
    parse_catalog.parse_catalog()

    flatten_catalog = FlattenCatalog(beckn_catalog_exploded_file, beckn_catalog_flattened_file)
    flatten_catalog.flatten_catalog()

    bulk_loader = BulkLoader(beckn_catalog_flattened_file, index_name)
    bulk_loader.bulk_load_data()
  except Exception as e:
    print(f"Error: {str(e)}")