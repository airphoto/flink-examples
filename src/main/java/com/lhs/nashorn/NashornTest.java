package com.lhs.nashorn;

import java.lang.*;
import java.util.Arrays;
import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class NashornTest {
    public static void main(String[] args) throws Exception {
        test1();
    }

    public static void test1() throws Exception {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
        engine.eval("print('hello world')");
    }
}
