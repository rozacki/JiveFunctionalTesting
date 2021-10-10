!echo ------------------------;
!echo ------------------------ arrays_of_arrays;
!echo ------------------------;
DROP TABLE IF EXISTS src_test_data_arrays_of_arrays_arrays_of_arrays
;

CREATE EXTERNAL TABLE src_test_data_arrays_of_arrays_arrays_of_arrays(
`array1` ARRAY<STRUCT<`array4`:ARRAY<STRUCT<`array5`:ARRAY<STRING>
>>
,`array3`:ARRAY<STRUCT<`a`:STRING
,`b`:STRING
>>
,`array2`:ARRAY<STRING>
,`a`:STRING
>>
 )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive/test_data/arrays_of_arrays';

DROP TABLE IF EXISTS arrays_of_arrays;

CREATE TABLE arrays_of_arrays  AS SELECT 
 
`exploded_array1_exploded_array2` as array1_array2, 
`exploded_array1`.`a` as array1_a, 
`exploded_array1_exploded_array3`.`a` as array1_array3_a, 
`exploded_array1_exploded_array3`.`b` as array1_array3_b, 
CAST(`exploded_array1_exploded_array4`.`array5`[1] as INT) as array1_array4_array4_index FROM src_test_data_arrays_of_arrays_arrays_of_arrays
 LATERAL VIEW OUTER EXPLODE(`array1`) view_exploded_array1 AS exploded_array1 
 LATERAL VIEW OUTER EXPLODE(`exploded_array1`.`array2`) view_exploded_array1_exploded_array2 AS exploded_array1_exploded_array2 
 LATERAL VIEW OUTER EXPLODE(`exploded_array1`.`array3`) view_exploded_array1_exploded_array3 AS exploded_array1_exploded_array3 
 LATERAL VIEW OUTER EXPLODE(`exploded_array1`.`array4`) view_exploded_array1_exploded_array4 AS exploded_array1_exploded_array4 
;


