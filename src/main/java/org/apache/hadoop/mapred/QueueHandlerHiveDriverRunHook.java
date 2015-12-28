package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.HiveDriverRunHook;
import org.apache.hadoop.hive.ql.HiveDriverRunHookContext;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.QueueAclsInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QueueHandlerHiveDriverRunHook implements HiveDriverRunHook {

	private static final String MR_QUEUE_NAME_PROPERTY = "mapred.job.queue.name";
	private static final String TEZ_QUEUE_NAME_PROPERTY = "tez.queue.name";
	private static final String HIVE_EXECUTION_ENGINE_PROPERTY = "hive.execution.engine";

	private static final Log LOG = LogFactory
			.getLog(QueueHandlerHiveDriverRunHook.class);

	@Override
	public void postDriverRun(HiveDriverRunHookContext context)
			throws Exception {
	}

	@Override
	public void preDriverRun(HiveDriverRunHookContext context) throws Exception {

		HiveConf config = (HiveConf) context.getConf();

		// check if hive execution engine is set to tez. If so, queue name
		// property should be tez.queue.name.
		String queue_property = MR_QUEUE_NAME_PROPERTY;
		String hiveExecEngine = config
				.get(QueueHandlerHiveDriverRunHook.HIVE_EXECUTION_ENGINE_PROPERTY);

		if (hiveExecEngine != null && hiveExecEngine.equalsIgnoreCase("tez")) {
			queue_property = TEZ_QUEUE_NAME_PROPERTY;
		}

		String queue = config.get(queue_property);

		// if queue name is specified as default, try to find a better qualified
		// queue
		// if (queue==null || "default".equals(queue)) {
		String newQueue = getQualifiedQueue(context.getConf(), queue);
		if (newQueue != null && !newQueue.equalsIgnoreCase(queue)) {
			config.set(queue_property, newQueue);
			LOG.info("queue name overriden to " + queue
					+ " From default for the user " + config.getUser());
			// }
		}
	}

	private String getQualifiedQueue(Configuration config, String queueInp) {

		String queue = null;
		JobConf job = new JobConf(config);
		JobClient jobClient;
		try {
			jobClient = new JobClient(job);
			QueueAclsInfo[] infos = jobClient.getQueueAclsForCurrentUser();
			// first check default queue
			for (QueueAclsInfo info : infos) {
				String name = info.getQueueName();
				boolean qualified = false;

				if ("default".equals(name)) {
					LOG.info("优先判断 default Queue");
					String[] ops = info.getOperations();
					if (ops != null && ops.length > 0) {
						for (String op : ops) {
							if ("SUBMIT_APPLICATIONS".equals(op)) {
								qualified = true;
								break;
							}
						}
					}
				}
				if (qualified) {
					queue = name;
					break;
				}
			}

			if (queue == null || queue.length() == 0) {
				for (QueueAclsInfo info : infos) {
					String name = info.getQueueName();
					// exclude root queue
					if (name != null && name.indexOf("root") == -1) {
						boolean qualified = false;
						String[] ops = info.getOperations();
						if (ops != null && ops.length > 0) {
							for (String op : ops) {
								if ("SUBMIT_APPLICATIONS".equals(op)) {
									qualified = true;
									break;
								}
							}
						}
						if (qualified) {
							queue = name;
							break;
						}
					}
				}
			}

		} catch (IOException e) {
			LOG.error(
					"Ignoring error while executing the pre driver run hook. error occured during the queue determination.",
					e);
			;
		}
		return queue;
	}
}
