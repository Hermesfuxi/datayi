package create;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class ProductInfoPageInfoDataMonitor {
    public static void main(String[] args) throws IOException {
        String[] categories = new String[]{
                "文胸","杀虫剂","打火机","水墨画","雕塑","兽医器具","农机配件","吊带","抹胸","商务男袜","太阳镜",
                "吊带","农药","皮带","蜡烛","皮鞭","笔记本电脑"};

        // 造品牌
        ArrayList<String> brands = new ArrayList<>();
        for(int i=0;i<10;i++){
            brands.add(RandomStringUtils.randomAlphabetic(4,5));
        }

        // 造栏目
        ArrayList<String> column = new ArrayList<>();
        for(int i=0;i<30;i++){
            column.add(RandomStringUtils.randomAlphabetic(2,3));
        }

        // 造商品信息数据
        BufferedWriter bw = new BufferedWriter(new FileWriter("productinfo.csv"));
        for(int i = 1;i<=999;i++){
            String data = i +"," + categories[RandomUtils.nextInt(0,categories.length)] + "," + brands.get(RandomUtils.nextInt(0,10));
            bw.write(data);
            bw.newLine();
        }

        bw.close();
        // 页面信息数据
        BufferedWriter bw2 = new BufferedWriter(new FileWriter("pageinfo.csv"));
        for(int i = 1;i<=999;i++){
            String data = i +"," + column.get(RandomUtils.nextInt(0,30));
            bw2.write(data);
            bw2.newLine();
        }
        bw2.close();
    }

}
