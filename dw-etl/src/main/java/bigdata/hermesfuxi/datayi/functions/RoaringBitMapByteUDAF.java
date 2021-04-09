package bigdata.hermesfuxi.datayi.functions;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 将 IntegerType 数据编码为 Roaringbitmap，并序列化为二进制输出
 *  与 之前 之间的区别，除了输入值不同之外，主要在于最后的返回值不同 RoaringBitMapByteUDAF 返回值是binary，即 evaluate 的编写不一样
 */
public class RoaringBitMapByteUDAF extends UserDefinedAggregateFunction {
    /**
     * // 聚合函数的输入数据结构
     */
    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("field", DataTypes.IntegerType, true));
        return DataTypes.createStructType(structFields);
    }

    /**
     * 聚缓存区数据结构   //聚合的中间过程中产生的数据的数据类型定义
     */
    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("field", DataTypes.BinaryType, true));
        return DataTypes.createStructType(structFields);
    }

    /**
     * 聚合函数返回值数据结构
     */
    @Override
    public DataType dataType() {
        return DataTypes.BinaryType;
    }

    /**
     * 聚合函数是否是幂等的，即相同输入是否总是能得到相同输出
     */
    @Override
    public boolean deterministic() {
        //是否强制每次执行的结果相同
        return true;
    }

    /**
     * 初始化缓冲区
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        //初始化
        buffer.update(0, null);
    }

    /**
     *  给聚合函数传入一条新数据进行处理
     *  buffer.getInt(0)获取的是上一次聚合后的值
     *   //用输入数据input更新buffer值,类似于combineByKey
     */

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        // 相同的executor间的数据合并
        Object in = input.get(0);
        Object out = buffer.get(0);
        RoaringBitmap outRR = new RoaringBitmap();
        // 1. 输入为空直接返回不更新
        if(in == null){
            return ;
        }

        // 2. 源为空则直接更新值为输入
        int inInt = Integer.parseInt(in.toString());
        byte[] inBytes = null ;
        if(out == null){
            outRR.add(inInt);
            try{
                // 将RoaringBitmap的数据转成字节数组
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream ndos = new DataOutputStream(bos);
                outRR.serialize(ndos);
                inBytes = bos.toByteArray();
                ndos.close();
            }   catch (IOException e) {
                e.printStackTrace();
            }
            buffer.update(0, inBytes);
            return ;
        }
        // 3. 源和输入都不为空使用 bitmap去重合并
        byte[] outBytes = (byte[]) buffer.get(0);
        byte[] result = outBytes;
        try {
            outRR.deserialize(new DataInputStream(new ByteArrayInputStream(outBytes)));
            outRR.add(inInt);
            ByteArrayOutputStream boss = new ByteArrayOutputStream();
            DataOutputStream ndosn = new DataOutputStream(boss);
            outRR.serialize(ndosn);
            result = boss.toByteArray();
            ndosn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.update(0, result);
    }


    /**
     *  合并聚合函数缓冲区
     *      //合并两个buffer,将buffer2合并到buffer1.在合并两个分区聚合结果的时候会被用到,类似于reduceByKey
     *    //这里要注意该方法没有返回值，
     *    在实现的时候是把buffer2合并到buffer1中去，你需要实现这个合并细节。
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //不同excutor间的数据合并
        // 合并两个聚合buffer，该函数在聚合并两个部分聚合数据集的时候调用
        //update(buffer1, buffer2);
        RoaringBitmap inRBM = new RoaringBitmap();
        RoaringBitmap outRBM = new RoaringBitmap();
        Object out = buffer1.get(0);
        byte[] inBytes = (byte[]) buffer2.get(0);
        if(out == null){
            buffer1.update(0, inBytes);
            return ;
        }
        byte[] outBitBytes = (byte[]) out;
        byte[] resultBit = outBitBytes;
        try {
            outRBM.deserialize(new DataInputStream(new ByteArrayInputStream(outBitBytes)));
            inRBM.deserialize(new DataInputStream(new ByteArrayInputStream(inBytes)));
            RoaringBitmap rror = RoaringBitmap.or(outRBM, inRBM) ;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream ndosn = new DataOutputStream(bos);
            rror.serialize(ndosn);
            resultBit = bos.toByteArray();
            ndosn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer1.update(0, resultBit);
    }

    /**
     * 计算最终结果
     */

    @Override
    public Object evaluate(Row buffer) {
        //根据Buffer计算结果
        Object val = buffer.get(0);
        byte[] outBitBytes = (byte[]) val;
        return outBitBytes;
    }
}
