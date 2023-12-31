{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_stations"."id_osm" ||
      'bicycle_station' ||
      'bicycle_station_fr'
    )  as id,
    'gouv' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_stations._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_stations"."id_osm" as external_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    code_com as zipcode,
    acces as access,
    lumiere as lit,
    protection as protection,
    type_accroche as bicycle_parking,
    proprietaire as owner,
    capacite as capacity,
    gestionnaire as operator,
    d_service as start_date,
    gratuit as fee,
    coordonneesxy as geolocation,
    url_info as website,
    mobilier as furniture,
    commentaires as note,
    surveillance,
    couverture as covered,
    capacite_cargo as cargo_capacity
FROM "{{ var("table_prefix") }}_stations"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_stations
ON _airbyte_raw_{{ var("table_prefix") }}_stations._airbyte_ab_id = "{{ var("table_prefix") }}_stations"._airbyte_ab_id

