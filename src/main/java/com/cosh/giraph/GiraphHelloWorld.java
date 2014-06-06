package com.cosh.giraph;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by cosh on 06.06.14.
 */
public class GiraphHelloWorld extends
        BasicComputation<IntWritable, IntWritable,
                NullWritable, NullWritable> {
    @Override
    public void compute(Vertex<IntWritable,
                IntWritable, NullWritable> vertex,
                        Iterable<NullWritable> messages) {
        System.out.print("Hello world from the: " +
                vertex.getId().toString() + " who is following:");
        for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
            System.out.print(" " + e.getTargetVertexId());
        }
        System.out.println("");
        vertex.voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GiraphRunner(), args));
    }
}