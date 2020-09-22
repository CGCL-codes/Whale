package org.apache.storm.poisson;

import cern.jet.random.Poisson;
import org.apache.storm.topology.base.BaseComponent;


public abstract class PoissonSpout extends BaseComponent implements IPoissonSpout {
    private int expect;
    public int e;

    public PoissonSpout(int expect) {
        this.expect = expect;
        this.e = Poisson.staticNextInt(expect);
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public void nextTuple() {
        e = Poisson.staticNextInt(expect);
        long n = System.nanoTime();
        for (int i = 0; i < e; i++) {
            next();
        }
        while (n + 1_000_000_000 >= System.nanoTime()) {
            ;
        }
    }
}
