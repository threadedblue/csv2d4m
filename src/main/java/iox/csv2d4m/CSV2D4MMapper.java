package iox.csv2d4m;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.mit.ll.graphulo.util.D4MTableWriter;
import edu.mit.ll.graphulo.util.D4MTableWriter.D4MTableConfig;
import iox.accumulo.AccumuloAccess;

public class CSV2D4MMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private static final Logger log = LoggerFactory.getLogger(CSV2D4MMapper.class);

	D4MTableWriter d4mTW;

	String[] colNames = {};

	String lastValue;
	Configuration cfg;

	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context ctx)
			throws IOException, InterruptedException {
		log.trace("setup==>0");
		super.setup(ctx);
		log.trace("setup==>1");

		log.trace("setup==>3");
		cfg = ctx.getConfiguration();
		String zookeeperURI = cfg.get(CSV2D4MDriver.ZOOKEEPER_URI);
		String accumuloInstance = cfg.get(CSV2D4MDriver.ACCUMULO_INSTANCE);
		String tableName = cfg.get(CSV2D4MDriver.TABLE_NAME);
		String credentials = cfg.get(CSV2D4MDriver.ACCUMULO_CREDENTIALS);
		AccumuloAccess db = new AccumuloAccess(zookeeperURI, accumuloInstance, credentials);
		D4MTableConfig tconf = new D4MTableConfig();
		tconf.baseName = tableName;
		tconf.connector = db.getConnection();
		tconf.useTable = true;
		tconf.useTableT = true;
		tconf.useTableDeg = true;
		tconf.useTableDegT = true;
		d4mTW = new D4MTableWriter(tconf);
		D4MTableWriter.createTableSoft(tableName, db.getConnection(), false);
		log.debug("<==setup" + " zoo=" + zookeeperURI);
	}

	@Override
	protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		ctx.getCounter(CSV2D4MDriver.COUNTER.RECORDS).increment(1);
		String row = key.toString();
		log.trace("map==>0");
		lastValue = value.toString();
		if (value.getLength() == 0) {
			return;
		}
		String s = value.toString();
		String[] line = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		if (line.length == 0) {
			log.error("s=" + s);
			return;
		}
		log.trace("map==>1" + s);
		if (colNames.length == 0) {
			colNames = line;
			log.info("colNames len=" + colNames.length + " colNames=" + Arrays.toString(colNames));
		} else {

			if (colNames.length != line.length) {
				log.warn("Size mismatch: " + " colNames len=" + colNames.length + " line len=" + line.length
						+ " using minimum.");
				for (int i = 0; i < Math.min(colNames.length, line.length); i++) {
					log.warn("r=" + row + " c=" + colNames[i] + " v=" + line[i]);
				}
			}
			try {
				for (int i = 0; i < Math.min(colNames.length, line.length); i++) {
					d4mTW.ingestRow(new Text(row), new Text(colNames[i]), new Value(line[i]));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				log.error("row=" + row + " len=" + line.length + " line=" + Arrays.toString(line));
				log.error("", e);
			} catch (Exception e) {
				log.error("", e);
			}
			if (ctx.getCounter(CSV2D4MDriver.COUNTER.RECORDS).getValue() % 5000 == 0) {
				log.debug("inserted=" + ctx.getCounter(CSV2D4MDriver.COUNTER.RECORDS).getValue());
				d4mTW.flushBuffers();
			}
		}
		log.trace("<==map");
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context ctx)
			throws IOException, InterruptedException {
		super.cleanup(ctx);
		log.info("cleanup v=" + lastValue + " count=" + ctx.getCounter(CSV2D4MDriver.COUNTER.RECORDS).getValue());
		d4mTW.flushBuffers();
		d4mTW.close();
	}

}
