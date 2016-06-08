var adt = {};
adt.adt_test = {};
adt.adt_test.getShardIndex = getDefaultShardIndex;

adt.adt_test_2 = {};
adt.adt_test_2.getShardIndex = getDefaultShardIndex;

function getDefaultShardIndex(pkDataList, shardCount){
	return pkDataList.get(0) % shardCount;
};
