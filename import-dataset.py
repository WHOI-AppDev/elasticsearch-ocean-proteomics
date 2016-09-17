import requests
import urllib2
import csv
import json
import getopt
import sys

def main(argv):

  rebuild_index = False
  server = 'http://localhost:9200'
  index = 'ec-ocean-protein-portal'
  es_type = 'dataset_646115_v1'

  help = 'import-dataset.py -r <rebuild ES index eg. true|false> -s <ES server eg. http://localhost:9200> -i <ES index name> -t <ES document type>'
  try:
    opts, args = getopt.getopt(argv,"hr:s:i:t:",["rebuild=","server=","index=","type="])
  except getopt.GetoptError:
    print help
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
       print help
       sys.exit()
    elif opt in ("-r", "--rebuild"):
       rebuild_index = arg.lower() == "true"
    elif opt in ("-s", "--server"):
       server = arg
    elif opt in ("-i", "--index"):
       index = arg
    elif opt in ("-t", "--type"):
       es_type =arg

  print 'ES Server: "', server
  print 'ES Index "', index
  print 'ES Doc Type "', es_type
  print 'Rebuild? "', rebuild_index

  index_settings = {'settings': {'index':{'number_of_shards': 1, 'number_of_replicas': 0}}}

  url = 'http://www.bco-dmo.org/dataset/646115/data/download'
  schema_url = 'https://raw.githubusercontent.com/ashepherd/elasticsearch-ocean-proteomics/master/dataset_646115_schema.json'

  delim = '\t'

  dataset = requests.get(url)
  tsv = dataset.text.split('\n')
  columns = []
  length = 0
  line_idx = 0

  for line in tsv:
    if not columns:
      columns = line.split(delim)
      length = len(columns)

      if rebuild_index:
        index_url = server + '/' + index
        mapping_url = index_url + '/_mapping/' + es_type
        r = requests.delete(mapping_url)
        print "DELETE INDEX: " + r.text

        r = requests.put(index_url, data = json.dumps(index_settings))

        schema = requests.get(schema_url)
        r = requests.put(mapping_url, data = schema.text)
        print r.status_code
        print "PUT SCHEMA: " + r.text

    else:
      line_idx += 1
      values = line.split(delim)
      values_len = len(values)
      if values_len != length:
        print 'Missing data on line ' + `(line_idx + 1)` + '. Found only ' + `values_len` + ' values, but expected ' + `length`
        print 'Line: "' + line + '"'
        exit()

      data = {}
      lat = False
      lon = False

      data['_data_table_row'] = {'value': line_idx}

      for idx, column_label in enumerate(columns):
        data[column_label] = {'value': values[idx]}
        if 'lon' == column_label:
          lon = values[idx]
        elif 'lat' == column_label:
          lat = values[idx]

      if (lat and lon):
        data['_coordinate'] = {'lat': lat, 'lon': lon}

      es_url = server + '/' + index + '/' + es_type + '/' + `line_idx`
      r = requests.head(es_url)
      if r.status_code == 200:
        # Update the row
        r = requests.post(es_url + '/_update', data = json.dumps({'doc': data}))
        if r.status_code == 200:
          print 'Updated row ' + `line_idx` + ': ' + r.text
        else:
          print 'ERROR updating row ' + `line_idx` + '[' + `r.status_code` + ']: ' + r.text

      else:
        r = requests.put(es_url + '/_create', data = json.dumps(data))
        if r.status_code == 201:
          print 'Created row ' + `line_idx` + ': ' + r.text
        else:
          print 'ERROR creating row ' + `line_idx` + '[' + `r.status_code` + ']: ' + r.text

if __name__ == "__main__":
   main(sys.argv[1:])
