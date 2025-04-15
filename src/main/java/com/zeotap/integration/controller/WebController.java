package com.zeotap.integration.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Controller for serving web pages
 */
@Controller
public class WebController {

    /**
     * Serves the main application page
     *
     * @return View name
     */
    @GetMapping("/")
    public String index() {
        return "index";
    }
}