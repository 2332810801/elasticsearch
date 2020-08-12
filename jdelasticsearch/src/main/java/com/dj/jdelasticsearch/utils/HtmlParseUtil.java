package com.dj.jdelasticsearch.utils;

import com.dj.jdelasticsearch.Pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.naming.Name;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtil {
    public static void main(String[] args) throws IOException {
        new HtmlParseUtil().ParseJD("java").forEach(System.out::println);
    }

    public  List<Content> ParseJD(String keywords) throws IOException {
        //获取请求 ：https://search.jd.com/Search?keyword=java
        //需要联网
        String encode = URLEncoder.encode(keywords, "UTF-8");//url字符转义
        String url="https://search.jd.com/Search?keyword="+ keywords;
        //解析网页 (Jsoup返回的Document对象就是JS的Document对象)
        Document document = Jsoup.parse(new URL(url), 30000);
        //所有js的方法都可以使用
        Element element = document.getElementById("J_goodsList");
        //获取所有的li标签
        Elements elements = element.getElementsByTag("li");

        ArrayList<Content> goods = new ArrayList<>();
        for (Element el : elements) {
            //关于图片特别多的网站 所有图片都是懒加载
            String img = el.getElementsByTag("img").eq(0).attr("src");//图片
            String price = el.getElementsByClass("p-price").eq(0).text();//价格
            String title = el.getElementsByClass("p-name").eq(0).text();//标题

            Content content = new Content(title, img, price);
            goods.add(content);

        }
        return goods;
    }
}
