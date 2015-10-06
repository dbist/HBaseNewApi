package com.hortonworks.hbase;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author aervits
 */
public class CreateTableWithSplits {

    private static final Logger LOG = Logger.getLogger(HBaseAPI.class.getName());

    private static final String TABLE_NAME = "clicks_tbl";
    private static final String CF_DEFAULT = "cf";

    public static void createOrOverwrite(Admin admin, HTableDescriptor table, byte[][] splits) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table, splits);
    }

    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            //byte[] b = String.format("%016x", key).getBytes();
            byte[] b = String.format("%x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    public static void createTableWithSplits(Configuration config, String numRegions) {
        try (Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            //table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.SNAPPY));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT)
                    .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF));
            
            //RUN THIS NEXT TO SEE IF FAST_DIFF MADE A DIFFERENCE
            //THEN FOLLOW UP WITH OTHER PROPERTIES LIKE IN MEMORY AND ANYTHING TO IMPROVE WRITES
            
            //beginning key:
            //01088a3
            byte[][] splits = getHexSplits("0087e3", "fffe4b", Integer.parseInt(numRegions));

            LOG.info(String.format("Creating table with %s regions", numRegions));
            createOrOverwrite(admin, table, splits);
            LOG.info("Done.");
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }
    }

    public static void write(Configuration config) {

        TableName tableName = TableName.valueOf(TABLE_NAME);
        /**
         * a callback invoked when an asynchronous write fails.
         */
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.log(Level.SEVERE, "Failed to send put {0}.", e.getRow(i));
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                .listener(listener);
        try (Connection connection = ConnectionFactory.createConnection(config);
                final BufferedMutator mutator = connection.getBufferedMutator(params)) {

            List<Put> puts = new ArrayList<>();
            int count = 0;
            Put put;

            for (int i = 0; i < 1000000; i++) {
                String rowKey = UUID.randomUUID().toString();

                put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("cnt"), Bytes.toBytes(count));
                puts.add(put);
                count++;

                if ((count % 10000) == 0) {
                    mutator.mutate(puts);
                    LOG.log(Level.INFO, "Count: {0}", count);
                    puts.clear();
                }
            }
            mutator.mutate(puts);

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }
    }

    public static void main(String... args) throws IOException {
        if(args.length < 1) {
            throw new RuntimeException("Please pass a number of regions");
        }
        
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", "jetmaster2.jetnetname.artem.com,jetslave5.jetnetname.artem.com,jetslave1.jetnetname.artem.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        long start = System.currentTimeMillis();
        createTableWithSplits(config, args[0]);

        long end = System.currentTimeMillis();
        LOG.log(Level.INFO, "Time: {0}", (end - start) / 1000);
    }
}
