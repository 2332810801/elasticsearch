package com.dj.jdelasticsearch.controller;

import com.dj.jdelasticsearch.Service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.awt.*;
import java.awt.image.Kernel;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class ContentController {
    @Autowired
    ContentService service;
    @GetMapping("/parse/{keywords}")
    public boolean parse(@PathVariable("keywords") String keywords) throws IOException {
       return service.parseContent(keywords);
    }
    @GetMapping("/search/{keywords}/{page}/{pageSize}")
    public List<Map<String,Object>> search(@PathVariable("keywords") String keywords,@PathVariable("page") int page,@PathVariable("pageSize") int pageSize) throws IOException {
        return  service.searchHighlightPage(keywords, page, pageSize);
    }
}
