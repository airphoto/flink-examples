package com.lhs.nashorn;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author lihuasong
 * @description 描述
 * @create 2019/7/12
 **/
public class LogProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
// 设置接入点，即控制台的实例详情页显示的“默认接入点”
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.98.168.143:9092");
// 接入协议
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
// Kafka 消息的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
// 请求的最长等待时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
// 构造 Producer 对象，注意，该对象是线程安全的，一般来说，一个进程内一个 Producer 对象即可；
// 如果想提高性能，可以多构造几个对象，但不要太多，最好不要超过 5 个
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

// 构造一个 Kafka 消息
        String topic = "link_bigdata"; //消息所属的 Topic，请在控制台申请之后，填写在这里
        String topic2 = "link_adv"; //消息所属的 Topic，请在控制台申请之后，填写在这里
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

        Long start = System.currentTimeMillis();
        for (int id=2001;id<=2001;id++){

            String time = format.format(new Date());
            String[] fields = time.split(" ");
            String day = fields[0];
            Integer hour = Integer.parseInt(fields[1].split(":")[0]);
            Integer minute = Integer.parseInt(fields[1].split(":")[1]);
            Integer second = Integer.parseInt(fields[1].split(":")[2]);
//            String value = String.format("{\"table\":\"test.dwm_app_login_pv_2\",\"db\":\"d\",\"td\":%s,\"hr\":%d,\"min\":%d,\"app_id\":10001,\"pv\":\"0|0\",\"tv\":%d}",day,hour,minute,second,second);
            Long current = System.currentTimeMillis();
            Long dayBefore = current - (1000 * 60 * 60 * 24);
            String userRegisteredId = (id-1000)+"";
            String register = String.format("{\"type\": \"register\",\"version\": \"1.04.00\",\"time\": \"%s\",\"ip\": \"210.12.30.134\",\"properties\": {\"gid\": 12,\"appId\": 77781,\"userId\": %s,\"accountType\": %d,\"unionId\": \"oDceqxJS5PchOf414FBji7nbPRdA\",\"nickName\": \"6Zey5p2l5YiY5b635Y2OCg==\",\"sex\": 1,\"headImageUrl\": \"http://wx.qlogo.cn/mmopen/l5WiaUWxW319vVibTyoib4owLxw0icibWU0pBVrj8qwTJ4USNfe1O8wjnYRfR3Oribuv9m6CFsod7zEuicsic9o8Eme0mNDhqAomoiadf/0\",\"clientType\": 2,\"clientIP\": \"61.135.169.125\",\"chId\": \"xiangha\",\"subChId\": \"1QYIQJ6YBQ\",\"deviceId\": \"47871B4B-D779-47E6-A403-7EB3B0018234\",\"uuId\": \"47871B4B-D779-47E6-A403-7EB3B0018234\",\"city\": \"city\",\"province\": \"province\",\"country\": \"country\",\"idfa\": \"idfa\",\"imei\": \"868047043398884\",\"androidId\": \"androidId\",\"mac\": \"mac\",\"ip\": \"ip\",\"ua\": \"ua\",\"aid\": \"aid\",\"oaid\": \"oaid\"}}",dayBefore.toString(),id,2);
            String login = String.format("{\"type\": \"login\",\"version\": \"1.00.00\",\"time\": \"%s\",\"ip\": \"210.12.30.134\",\"properties\": {\"user\": {\"gid\": 12,\"appId\": 77781,\"userId\": %s,\"pluginId\": 1,\"clientType\": 1,\"pathId\": \"LADKNJSOQKD\",\"loginWay\": 1},\"appInfo\": {\"packageName\": \"com.xianlai.mahjongsx\",\"appVersion\": \"1.1.0\",\"hotVersion\": \"1.3.1\",\"chanel\": \"string\",\"subChanel\": \"string\"},\"serverId\": 88888,\"isGuildMember\": true,\"clientIP\": \"61.135.169.125\"}}",current.toString(),userRegisteredId);
            String serverGame = String.format("{\"type\": \"server_event_game\",\"event_type\": \"game_end\",\"server_ip\": \"1.1.1.1\",\"time\": \"%s\",\"common\": {\"unique_id\": \"sdgserhcvg\",\"game_type\": \"majiang\",\"start_time\": \"1313421342134\",\"end_time\": \"1313421342134\",\"game_time\": 600,\"desk_id\": \"38asdfasd\"},\"param\": [{\"user_appid\": 77781,\"user_id\": %s,\"mode_id\": 123,\"leave_time\": \"1313421342134\",\"play_id\": 4,\"room_id\": 1001,\"battle_id\": 1,\"status\": \"win\",\"score\": 23,\"client_type\": 1,\"broke_num\": 1}],\"custom\": [{\"fire_num\": 11,\"gold_before\": 123310000000000,\"gold_after\": 833110000000000}]}",current.toString(),userRegisteredId);
            String matchGame = String.format("{\"type\": \"match_game\",\"version\": \"1.00.00\",\"time\": \"%s\",\"ip\": \"210.12.30.134\",\"properties\": {\"mPluginId\": 213412,\"mId\": 532433,\"mUnionId\": \"kjhgffghiyutrdgfjhcxcfg\",\"mType\": 1,\"playId\": 1,\"game\": {\"turn\": 1,\"tableId\": 6785,\"startTime\": \"1516790873121\",\"number\": 4,\"appIds\": [77781,12001,13001,88888],\"gids\": [12,25,36,168],\"userIds\": [%s,883456,269933,672598],\"clientTypes\": [1,2,1,1],\"pluginNames\": [\"1_ddz_coin\",\"1_ddz_coin\",\"1_ddz_coin\",\"1_ddz_coin\"],\"stageType\": 0,\"setData\": {\"actualSetNum\": 2,\"data\": [{\"startTime\": \"1516791322000\",\"endTime\": \"1516791356018\",\"scores\": [10,-20,10,0],\"userUnderControl\": [0,2],\"extra\": {\"ddz\": {\"transition\": [0,2,0]}}},{\"startTime\": \"1516791380012\",\"endTime\": \"1516791392018\",\"scores\": [100,-20,-20,-60],\"userUnderControl\": [0,2],\"extra\": {\"ddz\": {\"transition\": [0,2,0]}}}]},\"totalScores\": [1200,1000,-900,-900],\"winnerPos\": [0,2]}}}",current.toString(),userRegisteredId);
            String goldSetOver = String.format("{\"type\": \"goldSetOver\",\"version\": \"1.03.00\",\"time\": \"%s\",\"ip\": \"210.12.30.134\",\"properties\": {\"uniqueId\": \"10001_1516791322000_767383\",\"startTime\": \"1516791322000\",\"serverId\": 10001,\"users\": {\"number\": 4,\"gids\": [12],\"appIds\": [77781],\"userIds\": [%s],\"pluginIds\": [1],\"clientTypes\": [4]},\"room\": {\"grade\": [1001],\"rule\": {\"playType\": 2,\"options\": [1]}},\"userUnderControl\": [0],\"serviceCharge\": [10],\"settlement\": [-1200],\"sysSettlement\": [0],\"seniorServiceCharge\": [0],\"extra\": {\"common\": {\"shouChu\": 2,\"jieSuanBeiShu\": [16],\"broken\": [0],\"danGuan\": [0],\"scoreUpLimit\": [1],\"beiShuUpLimit\": [1]},\"ddz\": {\"transition\": [0],\"action\": {\"strategyId\": 2133,\"strategyKey\": \"NowashPlanA\",\"shouJiao\": true,\"qiangDiZhuNum\": [1],\"jiaBeiNum\": [0],\"jieSuanBeiShu\": 2,\"liuJuNum\": 2,\"bomNum\": [0],\"noOutBomNum\": [0],\"oneCardNum\": [3],\"twoCardNumAvg\": [1],\"threeCardNum\": [0],\"threeWithOneCardNum\": [0],\"threeWithTwoCardNum\": [0],\"listCardNum\": [1],\"planeCardNum\": [0],\"listTwoCardNum\": [1],\"lunShu\": 6,\"discardNum\": [9]}}}}}",current.toString(),userRegisteredId);
            String gameOver = String.format("{\"type\": \"gameOver\",\"version\": \"1.02.00\",\"time\": \"%s\",\"ip\": \"210.12.30.134\",\"properties\": {\"contentType\": 1,\"uniqueId\": \"10001_209770\",\"reset\": 1,\"tableCreationTime\": \"1516791320000\",\"startTime\": \"1516791322000\",\"serverId\": 10001,\"tableId\": 209770,\"guildId\": 0,\"rule\": {\"playType\": 2,\"options\": [1,6],\"unit\": 1,\"setNumSpecified\": 8},\"users\": {\"number\": 4,\"gids\": [12,168],\"appId\": 77781,\"userIds\": [%s,672598,903523],\"clientTypes\": [1,2],\"roomOwner\": {\"gid\": 25,\"userId\": 883456,\"clientType\": 2}},\"setData\": {\"actualSetNum\": 2,\"data\": [{\"startTime\": \"1516791380012\",\"endTime\": \"1516791392018\",\"scores\": [100,-60]}]},\"dissolved\": true,\"totalScores\": [200,-100],\"winnerPos\": [0,2],\"payment\": {\"mode\": 1,\"payable\": 3,\"paid\": 3},\"action\": {\"zimo\": [1,0],\"jiepao\": [0,0],\"hu\": [0,0],\"dianpao\": [0,1],\"angang\": [0,1],\"minggang\": [0,0,0,1],\"peng\": [0,1],\"ting\": [0,1],\"mandate\": [0,1],\"huju\": [1,2,1,0]}}}",current.toString(),userRegisteredId);
            String adv = String.format("{\"@timestamp\":\"2020-12-16T09:11:47.763Z\",\"@metadata\":{\"beat\":\"filebeat\",\"type\":\"_doc\",\"version\":\"7.2.0\",\"topic\":\"xlhy-ad-stats\"},\"fields\":{\"topic\":\"xlhy-ad-stats\"},\"ecs\":{\"version\":\"1.0.0\"},\"host\":{\"name\":\"AD_Log_9\"},\"agent\":{\"ephemeral_id\":\"e4914b6c-54d3-49c0-8873-1131e7123e67\",\"hostname\":\"AD_Log_9\",\"id\":\"dbff64f9-1410-40c0-9f24-4fc978534cde\",\"version\":\"7.2.0\",\"type\":\"filebeat\"},\"log\":{\"offset\":50501333,\"file\":{\"path\":\"/work/log/sdk_request_event/20201216/17/AD_Log_9.log\"}},\"message\":\"{\\\"osType\\\":\\\"1\\\",\\\"softVer\\\":\\\"9\\\",\\\"type\\\":2,\\\"sid\\\":5,\\\"mid\\\":\\\"506\\\",\\\"pid\\\":\\\"1\\\",\\\"p1\\\":\\\"868-:03-870-1608109444-5fd9cd848e8fe|||1\\\",\\\"p2\\\":\\\"\\\",\\\"key\\\":\\\"1109627634|||7050498043097599\\\",\\\"net\\\":\\\"1\\\",\\\"cache_config\\\":1,\\\"cache_ad\\\":1,\\\"ad_result\\\":\\\"1\\\",\\\"ad_result_info\\\":\\\"\\\",\\\"time\\\":0,\\\"request_id\\\":\\\"868-:03-870-1608109444-5fd9cd848e8fe\\\",\\\"area_str\\\":\\\"江苏-苏州\\\",\\\"area_id\\\":137,\\\"spot_id\\\":0,\\\"user_id\\\":%s,\\\"game_id\\\":77781,\\\"flow_id\\\":\\\"\\\",\\\"group_id\\\":0,\\\"_h_appid\\\":102,\\\"_h_appkey\\\":\\\"655c97c934d544f2\\\",\\\"_h_appintversion\\\":\\\"2005210000\\\",\\\"_h_deviceid\\\":\\\"868860033414002:89860317780230288686:90:94:97:37:3E:03\\\",\\\"_h_area\\\":\\\"CN\\\",\\\"_h_language\\\":\\\"zh\\\",\\\"_h_system_type\\\":\\\"1\\\",\\\"_h_system_version\\\":\\\"28\\\",\\\"_h_ad_sdk_id\\\":\\\"1\\\",\\\"_h_ad_sdk_intversion\\\":48,\\\"now\\\":1608109907,\\\"ip\\\":\\\"221.224.32.154\\\"}\"}",userRegisteredId);

            ProducerRecord<String, String> regm = new ProducerRecord<String, String>(topic, register);
            ProducerRecord<String, String> logm = new ProducerRecord<String, String>(topic, login);
            ProducerRecord<String, String> sergm = new ProducerRecord<String, String>(topic, serverGame);
            ProducerRecord<String, String> mgm = new ProducerRecord<String, String>(topic, matchGame);
            ProducerRecord<String, String> gsm = new ProducerRecord<String, String>(topic2, goldSetOver);
            ProducerRecord<String, String> gom = new ProducerRecord<String, String>(topic2, gameOver);
            ProducerRecord<String, String> adm = new ProducerRecord<String, String>(topic2, adv);



            try {
                // 发送消息，并获得一个 Future 对象

                producer.send(regm);
                producer.send(logm);
                producer.send(sergm);
                producer.send(mgm);
                producer.send(gsm);
                producer.send(gom);
                producer.send(adm);
//                Thread.sleep(5);
            } catch (Exception e) {
                // 要考虑重试
                System.out.println("error occurred");
                e.printStackTrace();
            }finally {
                producer.flush();
                System.out.println(id+"->"+userRegisteredId);
            }
        }
        Long end = System.currentTimeMillis();
        System.out.println("start :"+start+" duration"+(end-start));
    }
}