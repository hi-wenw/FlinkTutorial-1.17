package com.atguigu.functions;

import com.atguigu.a_00_bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class baozi_filter implements FilterFunction<WaterSensor> {
    public Integer vc;

    public baozi_filter(Integer vc) {
        this.vc = vc;
    }

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return this.vc.equals(value.getVc());
    }
}
