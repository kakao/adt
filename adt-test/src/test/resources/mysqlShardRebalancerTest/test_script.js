
var func_print = function(obj){
	print(obj);
};

var func_iterate_list = function(list){
	for(var i=0; i<list.size(); i++){
		func_print(list.get(i));
	}
};

var func_get_from_list = function(list, index){
	return list.get(index);
};


var testVar = {
	testFunc: function(){
		return 123;
	},
	testVar2: {
		testFunc2: function(string){
			return string + string;
		}
	}
}
