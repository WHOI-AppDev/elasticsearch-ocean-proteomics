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
      $ch = curl_init('http://' . $server . '/' . $index . '/_mapping/' . $type);
      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
      curl_setopt($ch,CURLOPT_TIMEOUT,1000);
      curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "DELETE");
      $delete = curl_exec($ch);
      curl_close($ch);
      echo "DELETE INDEX: $delete" . $newline;
      // Create ElasticSearch Index.
      $schema = file_get_contents($schema_url);
      $ch = curl_init('http://' . $server . '/' . $index . '/_mapping/' . $type);
      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
      curl_setopt($ch,CURLOPT_TIMEOUT,1000);
      curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT");
      curl_setopt($ch, CURLOPT_POSTFIELDS, $schema);
      $put = curl_exec($ch);
      curl_close($ch);
      echo "PUT INDEX $put" . $newline;
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
      echo $es_url . $newline;
      $ch = curl_init($es_url);
      curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
      curl_setopt($ch, CURLOPT_HEADER, true);
      curl_setopt($ch, CURLOPT_NOBODY, true);
      curl_setopt($ch,CURLOPT_TIMEOUT,1000);
      $exec = curl_exec($ch);
      $check_exists = curl_getinfo($ch, CURLINFO_HTTP_CODE);
      curl_close($ch);
      if (200 == $check_exists) {
        $ch = curl_init($es_url . '/_update');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch,CURLOPT_TIMEOUT,1000);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "POST");
        curl_setopt($ch, CURLOPT_POSTFIELDS, '{"doc" : ' . json_encode($doc) . '}');
        $exec = curl_exec($ch);
        $update = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        if (200 == $update) {
          echo "Updated row: $line_idx - $exec" . $newline;
        }
        else {
          echo "Error updating row[$update]: $line_idx -  - $exec" . $newline;
        }
      }
      else {
        $ch = curl_init($es_url . '/_create');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch,CURLOPT_TIMEOUT,1000);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($doc));
        $exec = curl_exec($ch);
        $create = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        if (201 == $create) {
          echo "Created row: $line_idx - $exec" . $newline;
        }
        else {
          echo "Error creating row: $line_idx - $exec" . $newline;
        }
      }
    }
  }
}
