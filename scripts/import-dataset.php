<?php

/**
 * @file import-dataset.php
 *
 */

$restart = FALSE;
$server = 'localhost:9200';
$url = 'http://www.bco-dmo.org/dataset/646115/data/download';
$index = 'ec-ocean-protein-portal';
$type = 'dataset_646115_v1';
$schema_url = 'https://raw.githubusercontent.com/ashepherd/elasticsearch-ocean-proteomics/master/dataset_646115_schema.json';
$newline = "\n";
$delim = "\t";
$base_http_opts = array(
  CURLOPT_RETURNTRANSFER => FALSE,
  CURLOPT_TIMEOUT => 10,
  CURLOPT_VERBOSE => TRUE,
);

$tsv = fopen($url, "r");
if ($tsv == FALSE) {
  echo "Could not open $url" . $newline;
  exit -1;
}

$columns = FALSE;
$length = 0;
$line_idx = 0;
while (!feof($tsv)) {
  $line = fgets($tsv);
  if (!$columns) {
    $columns = explode($delim, trim($line));
    $length = count($columns);

    if ($restart) {
      // Delete ElasticSearch Index.
      $delete_options = array(
        CURLOPT_CUSTOMREQUEST => 'DELETE',
      );
      $delete = _curl_request('http://' . $server . '/' . $index . '/_mapping/' . $type, $delete_options + $base_http_opts);
      echo "DELETE INDEX: " . $delete['response'] . $newline;
      // Create ElasticSearch Index.
      $schema = file_get_contents($schema_url);
      $insert_options = array(
        CURLOPT_CUSTOMREQUEST => 'PUT',
        CURLOPT_POSTFIELDS => $schema,
      );
      $insert = _curl_request('http://' . $server . '/' . $index . '/_mapping/' . $type, $insert_options + $base_http_opts);
      echo "PUT INDEX: " . $insert['response'] . $newline;
    }
  }
  else {
    $line_idx++;
    $values = explode($delim, trim($line));
    if ($length !== ($value_len = count($values))) {
      // Skip this row.
      echo $value_len . " >> " . $line . $newline;
    }
    else {
      // Add to ElasticSearch
      $doc = array('_data_table_row' => array('value' => $line_idx));
      $lat = FALSE;
      $lon = FALSE;
      foreach ($columns as $idx => $name) {
        $doc[$name]['value'] = $values[$idx];
        if ('lon' == $name) {
          $lon = $values[$idx];
        }
        elseif ('lat' == $name) {
          $lat = $values[$idx];
        }
      }
      if ($lon && $lat) {
        $doc['_coordinate'] = array('lat' => $lat, 'lon' => $lon);
      }
      $es_url = 'http://' . $server . '/' . $index . '/' . $type . '/' . $line_idx;
      $head_options = array(
        CURLOPT_HEADER => TRUE,
        CURLOPT_NOBODY => TRUE,
      );
      $head = _curl_request($es_url, $head_options + $base_http_opts);
      echo "CHECK " . $line_idx . ": " . $head['code'] . $newline;
      if (200 == $head['code']) {
        $update_options = array(
          CURLOPT_CUSTOMREQUEST => 'POST',
          CURLOPT_POSTFIELDS =>  '{"doc" : ' . json_encode($doc) . '}',
        );
        $update = _curl_request($es_url . '/_update', $update_options + $base_http_opts);
        if (200 == $update['code']) {
          echo "Updated row: $line_idx - " . $update['response'] . $newline;
        }
        else {
          echo "Error updating row: $line_idx - [" . $update['code'] . ']: '. $update['response'] . $newline;
          echo "  " . $update['log'] . $newline;
        }
      }
      else {
        $create_options = array(
          CURLOPT_CUSTOMREQUEST => 'PUT',
          CURLOPT_POSTFIELDS =>  json_encode($doc),
        );
        $create = _curl_request($es_url . '/_create', $create_options + $base_http_opts);

        if (201 == $create['code']) {
          echo "Created row: $line_idx - " . $create['response'] . $newline;
        }
        else {
          echo "Error creating row: $line_idx - [" . $create['code'] . ']: ' . $create['response'] . $newline;
          echo "  " . $create['log'] . $newline;
        }
      }
    }
  }
}

/**
 * Make an cURL HTTP Request.
 *
 * @param string $url
 *   The URL to request
 *
 * @param array $options
 *   THe PHP cURL options
 *
 * @return array
 *    'code' => the HTTP status code
 *    'response' => the response data
 */
function _curl_request($url, $opts) {

  ob_start();
  $out = fopen('php://output', 'w');

  $ch = curl_init($url);
  curl_setopt_array($ch, $opts);
  curl_setopt($ch, CURLOPT_STDERR, $out);
  $response = curl_exec($ch);
  $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
  curl_close($ch);

  fclose($out);
  $log = ob_get_clean();

  return array(
    'code' => $status,
    'response' => $response,
    'log' => $log,
  );
}
