package cn.xiaows.taggen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class ReviewTags {

    public static String extractTags(String jsonStr) {
        JSONObject object = JSON.parseObject(jsonStr);
        if (object == null || !object.containsKey("extInfoList")) {
            return "";
        }
        JSONArray array = object.getJSONArray("extInfoList");
        if (array == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.size(); i++) {
            JSONObject obj = array.getJSONObject(i);
            if (obj != null && obj.containsKey("title") && obj.getString("title").equals("contentTags") && obj.containsKey("values")) {
                JSONArray arr = obj.getJSONArray("values");
                if (arr == null) {
                    continue;
                }
                boolean begin = true;
                for (int j = 0; j < arr.size(); j++) {
                    if (begin)
                        begin = false;
                    else
                        sb.append(",");
                    sb.append(arr.getString(j));
                }
            }
        }
        // System.out.println(sb);
        return sb.toString();
    }

    public static void main(String[] args) {
        String s = "{\"reviewPics\":null,\"extInfoList\":[{\"title\":\"contentTags\",\"values\":[\"午餐\",\"分量适中\"],\"desc\":\"\",\"defineType\":0},{\"title\":\"tagIds\",\"values\":[\"684\",\"240\"],\"desc\":\"\",\"defineType\":0}],\"expenseList\":null,\"reviewIndexes\":[2],\"scoreList\":null}";
        System.out.println(extractTags(s));
        System.out.println(extractTags(""));
        System.out.println(extractTags(null));
    }
}
