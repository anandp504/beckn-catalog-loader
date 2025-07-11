
class ParseCatalog:
  def __init__(self, input_file, output_file):
    self.input_file = input_file
    self.output_file = output_file

  def parse_catalog(self):
    import json

    # Replace 'file.json' with your actual JSON file path
    # with open('retail/unified_retail_catalog_25k.json', 'r') as f:
    # with open('retail/unified_retail_catalog_1lac_correct.json', 'r') as f:
    # with open('../energy/on_search_ev_catalog_100k_items_retry.json', 'r') as f:
    with open(self.input_file, 'r') as f:
      data = json.load(f)

    # Print top-level keys
    # print("Top-level keys:")
    # for key in data.keys():
    #     print(key)

    context = data['context']

    providers = data['message']['catalog']['providers']

    with open(self.output_file, 'w') as f:
      for provider in providers:
        items = provider['items']
        catalog_data = {}
        catalog_data['context'] = context
        provider_name = provider['descriptor']['name']
        catalog_name = f"{provider_name}-Retail-Catalog"
        catalog_data.setdefault('message', {}).setdefault('catalog', {}).setdefault('descriptor', {})['name'] = catalog_name
        catalog_data['message']['catalog']['providers'] = provider
        for item in items:
            catalog_data['message']['catalog']['providers']['items'] = item
            f.write(json.dumps(catalog_data))
            f.write("\n")

      