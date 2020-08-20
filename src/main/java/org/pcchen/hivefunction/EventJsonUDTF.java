package org.pcchen.hivefunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

/**
 * 时间json进行多行解析输出
 *
 * @author ceek
 * @create 2020-08-19 16:45
 **/
public class EventJsonUDTF extends GenericUDTF {
    /**
     * 该方法中用来指定输出参数的名称和类型
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    public StructObjectInspector initialize(ObjectInspector[] argOIs)
            throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * 输入一条记录，输出若干条记录
     * @param objects
     * @throws HiveException
     */
    @Override
    public void process(Object[] objects) throws HiveException {
        //获取传入的et
        String eventLine = objects[0].toString();

        //如果传进来的数据为空，直接返回过滤掉该数据
        if(StringUtils.isBlank(eventLine)) {
            return;
        } else {
            //获取一共有几个事件（ad/facoriters）
            try {
                JSONArray jsonArray = new JSONArray(eventLine);

                if(null == jsonArray) {
                    return;
                }

                for(int i = 0; i < jsonArray.length(); i++) {
                    String[] result = new String[2];

                    //取出每个事件名称
                    result[0] = jsonArray.getJSONObject(i).getString("en");
                    //取出时间的整体json串
                    result[1] = jsonArray.getString(i);

                    //结果返回
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
