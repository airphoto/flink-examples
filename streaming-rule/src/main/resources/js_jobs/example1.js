function process_data(json_data) {
    var obj = JSON.parse(json_data);
    if ("test" == obj["type"])
        var key = obj["appid"]+":"+obj["userid"];
        var value = obj["score"];
        var redisData = {
            "db":1,
            "redisType":0,
            "key":key,
            "field":"",
            "value":value,
            "ttl":"10000"
        };
        return JSON.stringify(new Array(redisData));
}