var context;

function Map (var key1, var val1, var context)
{
	var ligne;
	ligne=val1.split(",");
	var map1;

	for(key1 in val1)
	{
		map1[key1][0] = key1;
		map1[key1][1] = ligne[key1];
	}
	context.set(map1, key1);
}

function reduce (var key2, var val2, var context)
{
	var map2;

	for(ke2 in val2)
	{
		map2[i][0] = key2;
		map2[i][1] = ligne[i];
	}
	context.set(map2, key2);
}