package com.atguigu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author JohnZhuang
 * @version 1.0
 * @description: TODO
 * @date 2023/2/27 16:04
 */
public class MyUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("只接受一个参数");
        }

        ObjectInspector argument = objectInspectors[0];
        if (ObjectInspector.Category.PRIMITIVE != argument.getCategory()) {
            throw new UDFArgumentException("只接受基本数据类型的参数");
        }

        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) argument;
        if (primitiveObjectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受String类型的参数");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector; // 输出结果的类型
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        DeferredObject argument = deferredObjects[0];
        Object value = argument.get();

        if (value == null) {
            return 0;
        } else {
            return value.toString().length();
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
