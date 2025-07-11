import json

class FlattenCatalog:
  def __init__(self, input_file, output_file):
    self.input_file = input_file
    self.output_file = output_file

    self.required_keys = {
      "context_domain",
      "context_bpp_id",
      "context_location_country_name",
      "context_location_country_code",
      "context_location_city_name",
      "context_location_city_code",
      "context_bap_id",
      "context_bap_uri",
      "message_catalog_providers_id",
      "message_catalog_providers_descriptor_name",
      "message_catalog_providers_categories_id",
      "message_catalog_providers_categories_descriptor_code",
      "message_catalog_providers_categories_descriptor_name",
      "message_catalog_providers_locations_gps",
      "message_catalog_providers_locations_address",
      "message_catalog_providers_locations_city_name",
      "message_catalog_providers_locations_state_name",
      "message_catalog_providers_locations_country_name",
      "message_catalog_providers_locations_area_code",
      "message_catalog_providers_locations_gps",
      "message_catalog_providers_fulfillments_ids",
      "message_catalog_providers_fulfillments_type",
      "message_catalog_providers_fulfillments_rateable",
      "message_catalog_providers_items_id",
      "message_catalog_providers_items_descriptor_name",
      "message_catalog_providers_items_descriptor_short_desc",
      "message_catalog_providers_items_category_ids",
      "message_catalog_providers_items_fulfillment_ids",
      "message_catalog_providers_items_price_listed_value",
      "message_catalog_providers_items_price_currency",
      "message_catalog_providers_items_price_value",
      "message_catalog_providers_items_quantity_available_count",
      "message_catalog_providers_items_rating",
      # "message_catalog_providers_items_tags_display",
      # "message_catalog_providers_items_tags_descriptor_name",
      # "message_catalog_providers_items_tags_descriptor_code",
      # "message_catalog_providers_items_tags_descriptor_short_desc",
      "message_catalog_providers_items_tags_list_display",
      "message_catalog_providers_items_tags_list_descriptor_name",
      "message_catalog_providers_items_tags_list_descriptor_code",
      "message_catalog_providers_items_tags_list_value",
    }

  def flatten_json(self, y, prefix=''):
    out = {}

    if isinstance(y, dict):
        for key, value in y.items():
            full_key = f"{prefix}_{key}" if prefix else key
            out.update(self.flatten_json(value, full_key))

    elif isinstance(y, list):
        if all(isinstance(i, dict) for i in y):
            temp = {}
            for item in y:
                for k, v in self.flatten_json(item).items():
                    if k not in temp:
                        temp[k] = []
                    # If v is already a list, extend it, otherwise append it
                    if isinstance(v, list):
                        temp[k].extend(v)
                    else:
                        temp[k].append(v)
            for k, v in temp.items():
                full_key = f"{prefix}_{k}" if prefix else k
                out[full_key] = v
        else:
            out[prefix] = y

    else:
        out[prefix] = y

    return out

  def transform_catalog_keys(self, data):
      """
      Transform keys by removing 'message.catalog.' prefix from keys that start with it.
      """
      transformed = {}
      for key, value in data.items():
          if key.startswith("message_catalog_providers_items"):
              new_key = key.replace("message_catalog_providers_items", "items")
              transformed[new_key] = value
          elif key.startswith("message_catalog_providers"):
              new_key = key.replace("message_catalog_providers", "providers")
              transformed[new_key] = value
          elif key.startswith("message_catalog_"):
              new_key = key.replace("message_catalog_", "")
              transformed[new_key] = value
          else:
              transformed[key] = value
      return transformed

  # output_file = "../diagnostics/diagnostics_catalog_items_flattened.json"

  def flatten_catalog(self):
    with open(self.input_file, 'r') as f_in, open(self.output_file, "w", encoding="utf-8") as f_out:
      for line in f_in:
        data = json.loads(line.strip())
        # catalog_data = data['message']['catalog']
        catalog_data = data
        catalog_data.pop("context")
        # First flatten the JSON
        flattened = self.flatten_json(data)
        # Then filter for required keys
        filtered_flattened = {k: v for k, v in flattened.items() if k in self.required_keys}
        # Finally transform the keys to remove message.catalog prefix
        transformed = self.transform_catalog_keys(filtered_flattened)
        transformed['raw_catalog'] = json.dumps(catalog_data)
        f_out.write(json.dumps(transformed, ensure_ascii=False) + "\n")

    print(f"Filtered flattened JSON written line by line to {self.output_file}")