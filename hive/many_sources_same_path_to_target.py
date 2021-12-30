import pytest
# cursor is needed as fixture
from hive import cursor, prepare_database_cursor


# todo: bug (1), when having more than once the same json structure:
#  1: mapped into columns and mapped as string
#  2. external table will use
# struct mapped to columns is used in the first query but GET_JSON_OBJECT() us used in the second
# the order in mapping file is irrelevant

# bug (2) _removed should not be in the second SQL
#
'''
sourceDB,sourceCollection,sourceFieldLocation,sourceDataType,destinationTable,destinationField,destinationDataType,function
test_data,many_sources_same_path_to_target,field1,string,many_sources_same_path_to_target,target_column1,string,
test_data,many_sources_same_path_to_target,field1.a,string,many_sources_same_path_to_target,target_column1,string,
test_data,many_sources_same_path_to_target,field1.b,string,many_sources_same_path_to_target,target_column1,string,
test_data,many_sources_same_path_to_target,field4,string,many_sources_same_path_to_target,target_column1,string,
test_data,many_sources_same_path_to_target,field6[0],string,many_sources_same_path_to_target,target_column1,string,
test_data,many_sources_same_path_to_target,field7,string,many_sources_same_path_to_target,target_column2,int,
test_data,many_sources_same_path_to_target,field8[0],string,many_sources_same_path_to_target,target_column2,int,
test_data,many_sources_same_path_to_target,field8[0].b,string,many_sources_same_path_to_target,target_column2,int,
test_data,many_sources_same_path_to_target,field9,string,many_sources_same_path_to_target,target_column2,int,
test_data,many_sources_same_path_to_target,field2[0],string,many_sources_same_path_to_target,target_column2,int,
test_data,many_sources_same_path_to_target,field2[0].a,string,many_sources_same_path_to_target,target_column2,int,

!echo ------------------------;
!echo ------------------------ many_sources_same_path_to_target;
!echo ------------------------;
DROP TABLE IF EXISTS src_test_data_many_sources_same_path_to_target_many_sources_same_path_to_target
;

CREATE EXTERNAL TABLE src_test_data_many_sources_same_path_to_target_many_sources_same_path_to_target(
`field2` ARRAY<STRUCT<`a`:STRING>>
,`field8` ARRAY<STRUCT<`b`:STRING>>
,`field1` STRUCT<`a`:STRING,`b`:STRING>
,`field6` ARRAY<STRING>
,`field4` STRING
,`field7` STRING
,`field9` STRING
 )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/hive//test_data/many_sources_same_path_to_target';

DROP TABLE IF EXISTS many_sources_same_path_to_target;

CREATE TABLE many_sources_same_path_to_target  AS SELECT 
 COALESCE(
COALESCE(GET_JSON_OBJECT(`field1`,"$.a"), GET_JSON_OBJECT(`field1`,"$.b"),`_removed`.`field1`,`field1`), 
`field4`, 
`field6`[0]) 
as target_column1
, COALESCE(
CAST(`field7` as INT), 
CAST(`field9` as INT), 
CAST(COALESCE(GET_JSON_OBJECT(`field2`[0],"$.a"),`_removed`.`field2`[0],`field2`[0]) as INT), 
CAST(COALESCE(GET_JSON_OBJECT(`field8`[0],"$.b"),`_removed`.`field8`[0],`field8`[0]) as INT)) 
as target_column2 
FROM src_test_data_many_sources_same_path_to_target_many_sources_same_path_to_target
 src_test_data_many_sources_same_path_to_target_many_sources_same_path_to_target;






'''

'''
@Test
    //test simple_vs_struct field
    public void selectSimpleVsStruct() throws URISyntaxException {
        //
        Assert.assertEquals(Collections.singletonList("10"), util.countNotNULLs(shell,HIVETargetTable,"target_column1"));

        Assert.assertEquals(Collections.singletonList("8"), util.countNotNULLs(shell,HIVETargetTable,"target_column2"));

        List<String> result = shell.executeQuery(String.format("select target_column1, target_column2 from %s", HIVETargetTable));

        List<String> expectedResultSet = Arrays.asList(
                new String("0\t20"),
                new String("1\t21"),
                new String("2\t22"),
                new String("3\t80"),
                new String("4\t81"),
                new String("5\t90"),
                new String("6\t23"),
                new String("40\tNULL"),
                new String("41\tNULL"),
                new String("60\tNULL"),
                new String("NULL\t71")
        );

        Assert.assertArrayEquals(expectedResultSet.toArray(), result.toArray());
    }
'''


class TestManySourceSamePathToTargetTest():
    test_name = 'many_sources_same_path_to_target'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    @pytest.mark.skip(reason="this functionality is broken in jive, see comments above")
    def test_select_simple_vs_struct(self, class_cursor):
        column = 'target_column1'
        sql = f'SELECT COUNT(*) FROM {self.test_name} WHERE {column} IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchone()
        assert 10 == rows[0]
