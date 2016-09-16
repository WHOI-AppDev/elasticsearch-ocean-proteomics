<?php

/**
 * @file import-dataset.php
 *
 */

$restart = TRUE;
$server = 'localhost:9200';
$url = 'http://www.bco-dmo.org/dataset/646115/data/download';
$index = '/ec-ocean-protein-portal/_mapping/dataset_646115_v1';
$schema_url = 'https://raw.githubusercontent.com/ashepherd/elasticsearch-ocean-proteomics/master/dataset_646115_schema.json';
$newline = "\n";

$tsv = fopen($url, "r");
if ($tsv !== FALSE) {
  $columns = FALSE;
  $length = 0;
  $delim = "\t";
  $line_idx = 0;
  while (!feof($tsv)) {
    $line = fgets($tsv);
    if (!$columns) {
      $columns = explode($delim, trim($line));
      $length = count($columns);

      if ($restart) {
        // Delete ElasticSearch Index.
        $ch = curl_init('http://' . $server . $index);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "DELETE");
        $delete = curl_exec($ch);
        curl_close($ch);
        echo $delete . $newline;
        // Create ElasticSearch Index.
        $schema = file_get_contents($schema_url);
        $ch = curl_init('http://' . $server . $index);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_setopt($ch, CURLOPT_POSTFIELDS,http_build_query(array('data' => json_encode($schema))));
        $put = curl_exec($ch);
        curl_close($ch);
        echo $put . $newline;
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
          elseif ('lat' == $name) {
            $lat = $values[$idx];
          }
        }
        if ($lon && $lat) {
          $doc['_coordinate'] = array('lat' => $lat, 'lon' => $lon);
        }

        $es_url = 'http://' . $server . $index . '/' . $line_idx;
        $ch = curl_init($es_url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_HEADER, true);
        curl_setopt($ch, CURLOPT_NOBODY, true); 
        $exec = curl_exec($ch);
        $check_exists = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);
        if (200 == $check_exists->code) {
          $doc = '{"doc" : ' . json_encode($doc) . '}';
          $ch = curl_init($es_url. '/_update');
          curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
          curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "POST");
          curl_setopt($ch, CURLOPT_POSTFIELDS,http_build_query(array('data' => $doc)));
          $exec = curl_exec($ch);
          curl_close($ch);
          $update = curl_getinfo($ch, CURLINFO_HTTP_CODE);
          if (200 == $update) {
            echo "Updated row: $line_idx - $exec" .$newline;
          }
          else {
            echo "Error updating row: $line_idx -  - $exec" .$newline;
          }
        }
        else {
          $ch = curl_init($es_url. '/_create');
          curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
          curl_setopt($ch, CURLOPT_CUSTOMREQUEST, "PUT");
          curl_setopt($ch, CURLOPT_POSTFIELDS,http_build_query(array('data' => $doc)));
          $exec = curl_exec($ch);
          curl_close($ch);
          $create = curl_getinfo($ch, CURLINFO_HTTP_CODE);
          if (201 == $create) {
            echo "Created row: $line_idx - $exec";
          }
          else {
            echo "Error creating row: $line_idx - $exec";
          }
        }
      }
    }
  }
}
