<<<<<<< HEAD
import apache_beam as beam


def print_element(element):
	print(element)


# Define el pipeline
pipeline = beam.Pipeline()

# Crea un PCollection
numbers = pipeline | 'Create numbers' >> beam.Create([1, 2, 3, 4, 5])

# Aplica una transformación para imprimir cada elemento
numbers | 'Print numbers' >> beam.Map(print_element)

# Ejecuta el pipeline
pipeline.run()
=======
import pandas as pd
import json
import os
import re
import datetime
import pyarrow as pa
import apache_beam as beam
from apache_beam.io.parquetio import WriteToParquet

def main():
  #---PATHS OF JSONS---
  path_json = './data/input/csgo-export-data.json'

  newFile_path = './data/input/cleaned_data.json'


  #---FORMATING DF IN PANDAS---
  pd.set_option('display.max_columns', None)
  pd.set_option('display.max_rows', 10)
  pd.set_option('display.width', None)
  pd.set_option('display.max_colwidth', None)

  #---FUNCTIONS---
  def clean(text):
    replacements = [('string_field_0,string_field_1', ''), ('""', '"'), ('"{', '{'), ('}"', '}'), ('á', 'a'), ('é', 'e'), ('í', 'i'),('ó', 'o'),('ú', 'u'),
                    ('[]', '[{"-" : "-"}]'), ('{}', '{"-" : "-"}'), ('1. ', '')]

    text = text.lower()

    for old, new in replacements:
      text = text.replace(old, new)

    text = re.sub(r'\b\d{1,2}\. ', '', text)
    text = re.sub(r',matches_\d{4}-\d{2}-\d{2}\.json', ',', text)
    text = text[:text.rfind(',')] if ',' in text else text

    return f"[{text}]"

  def maps_json(result):
    maps = {}

    try:
      if 'map_results' in result and isinstance(result['map_results'], list):
        map_results = result['map_results']
        for i in range(0,3):
          if len(map_results) > i:
            maps[f'map_{i + 1}'] = map_results[i].get('map_name', 'desconocido')
          else:
            maps[f'map_{i + 1}'] = 'desconocido'
      return maps
    except KeyError as e:
      print(f"Error: {e}")

  ###
  def halfs_json(result):
    halfs_score = {}
    previous_half = True #controla las anidaciones/dependencias del if

    try:
      if 'map_results' in result and isinstance(result['map_results'], list) and result['map_results']:
        first_map = result['map_results'][0]
        if 'score_per_half' in first_map:
          for i in range(1, 4):
            half_key = f'half_{i}'
            if previous_half and half_key in first_map['score_per_half']:
              score = first_map['score_per_half'][half_key]
              halfs_score[f'half{i}_left'] = int(score.get('left_team', 0))
              halfs_score[f'half{i}_right'] = int(score.get('right_team', 0))
            else:
              previous_half = False
              halfs_score[f'half{i}_left'] = 0
              halfs_score[f'half{i}_right'] = 0
      return halfs_score
    except KeyError as e:
      print(f"Error: {e}")
      return {f'half{i}_{side}': 0 for i in range(1, 4) for side in ['left', 'right']}


  #---CONTROL OF EXECUTION---
  if not os.path.exists(newFile_path):
      with open(path_json, 'r') as original_data:
        data = original_data.read()

      cleaned_data = clean(data)
      cleaned_data = f"[{cleaned_data}]"

      with open(newFile_path, 'w') as clean_data:
        clean_data.write(clean(data))


  with open('./data/input/cleaned_data.json', 'r') as f:
    json_data = json.load(f)


  df = pd.DataFrame(json_data)

  general_results = []
  match_id = 0

  for index, row in df.iterrows():
    date = pd.to_datetime(row['date'], format='%Y-%m-$d', errors='coerce')
    for result in row['results']:
      match_id += 1
      general_result = result['general_result']
      event = general_result['event']
      event_type = general_result['event_type']
      team_left, team_right = general_result['left_team']['name'], general_result['right_team']['name']
      team_left_score, team_right_score = int(general_result['left_team']['score']), int(general_result['right_team']['score'])
      team_left_country = result.get('countries', {}).get('team_left_country', 'desconocido')
      team_right_country = result.get('countries', {}).get('team_right_country', 'desconocido')
      list_veto = ', '.join(result.get('veto_maps', []))


      maps = maps_json(result)
      for i in range(1, 4):
        maps.setdefault(f'map_{i}', 'desconocido')

      halfs_score = halfs_json(result)
      for i in range(1, 4):
        halfs_score.setdefault(f'half{i}_left', 0)
        halfs_score.setdefault(f'half{i}_right', 0)

      match_data = {
        'match_id': match_id,
        'date': date,
        'event': event,
        'event_type': event_type,
        'team_left': team_left,
        'team_right': team_right,
        'score_left': team_left_score,
        'score_right': team_right_score,
        'map_1': maps.get(f'map_1', 'desconocido'),
        'half1_left': halfs_score.get(f'half1_left', '0'),
        'half1_right': halfs_score.get(f'half1_right', '0'),
        'map_2': maps.get(f'map_2', 'desconocido'),
        'half2_left': halfs_score.get(f'half2_left', '0'),
        'half2_right': halfs_score.get(f'half2_right', '0'),
        'map_3': maps.get(f'map_3', 'desconocido'),
        'half3_left': halfs_score.get(f'half3_left', '0'),
        'half3_right': halfs_score.get(f'half3_right', '0'),
        'country_team_left': team_left_country,
        'country_team_right': team_right_country,
        'veto': list_veto
      }

      general_results.append(match_data)

  #---CONVERT TO DF AND PRINT WITH PRETTYTABLE---
  df_general_results = pd.DataFrame(general_results)

  schema = pa.schema([
    pa.field('match_id', pa.int64()),
    pa.field('date', pa.date32()),
    pa.field('event', pa.string()),
    pa.field('event_type', pa.string()),
    pa.field('team_left', pa.string()),
    pa.field('team_right', pa.string()),
    pa.field('score_left', pa.int32()),
    pa.field('score_right', pa.int32()),
    pa.field('map_1', pa.string()),
    pa.field('half1_left', pa.int32()),
    pa.field('half1_right', pa.int32()),
    pa.field('map_2', pa.string()),
    pa.field('half2_left', pa.int32()),
    pa.field('half2_right', pa.int32()),
    pa.field('map_3', pa.string()),
    pa.field('half3_left', pa.int32()),
    pa.field('half3_right', pa.int32()),
    pa.field('country_team_left', pa.string()),
    pa.field('country_team_right', pa.string()),
    pa.field('veto', pa.string())
  ])

  timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
  output_file = f'./data/output/results_{timestamp}.parquet'

  with beam.Pipeline() as p:
    data = (
            p
            | 'CreateCollection' >> beam.Create(df_general_results.to_dict(orient="records"))
            | 'WriteParket' >> WriteToParquet(file_path_prefix = output_file, file_name_suffix = "", schema = schema)
          )
  result = p.run()
  result.wait_until_finish()


if __name__ == "__main__":
    main()
>>>>>>> 38dc7e8 (Create solution with Python and Apache Beam for data management)
