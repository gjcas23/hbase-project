package com.xwtech.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * HBase 工具类
 * Created by babylon on 2016/11/29.
 */
public class HBaseUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

	private static Configuration conf;
	private static Connection conn;

	public static void init(){
		String zkhost = LocationConfig.zkHost();
		try {
			if (conf == null) {
				conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", zkhost);
				conf.set("hbase.zookeeper.property.clientPort", "2181");
			}
		} catch (Exception e) {
			logger.error("HBase Configuration Initialization failure !");
			throw new RuntimeException(e) ;
		}
	}

	/**
	 * 获得链接
	 * @return
     */
	public static synchronized Connection getConnection() {
		init();
		try {
            if(conn == null || conn.isClosed()){
                conn = ConnectionFactory.createConnection(conf);
            }
		} catch (IOException e) {
			logger.error("HBase 建立链接失败 ", e);
		}
		return conn;

	}


	/**
	 * 获取  Table
	 * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	public static Table getTable(String tableName){
		try {
			return getConnection().getTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.error("Obtain Table failure !", e);
		}
		return null;
	}

	/**
	 * 获取  HTable
	 * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	/*public static HTable getHTable(String tableName){
		try {
			return (HTable)getConnection().getTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.error("Obtain Table failure !", e);
		}
		return null;
	}*/

	/**
	 * 获取  HTable
	 * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	public static HTable getHTable(String tableName,Connection conn){
		try {
			return (HTable)conn.getTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.error("Obtain Table failure !", e);
		}
		return null;
	}

	/**
	 * 给 table 创建 snapshot
	 * @param snapshotName 快照名称
     * @param tableName 表名
	 * @return
	 * @throws IOException
	 */
	public static void snapshot(String snapshotName, TableName tableName){
		try {
            Admin admin = getConnection().getAdmin();
            admin.snapshot(snapshotName, tableName);
		} catch (Exception e) {
			logger.error("Snapshot " + snapshotName + " create failed !", e);
		}
	}

    /**
     * 获得现已有的快照
     * @param snapshotNameRegex 正则过滤表达式
     * @return
     * @throws IOException
     */
    public static List<HBaseProtos.SnapshotDescription> listSnapshots(String snapshotNameRegex){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotNameRegex))
                return admin.listSnapshots(snapshotNameRegex);
            else
                return admin.listSnapshots();
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotNameRegex + " get failed !", e);
        }
        return null;
    }

    /**
     * 批量删除Snapshot
     * @param snapshotNameRegex 正则过滤表达式
     * @return
     * @throws IOException
     */
    public static void deleteSnapshots(String snapshotNameRegex){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotNameRegex))
                admin.deleteSnapshots(snapshotNameRegex);
            else
                logger.error("SnapshotNameRegex can't be null !");
        } catch (Exception e) {
            logger.error("Snapshots " + snapshotNameRegex + " del failed !", e);
        }
    }

    /**
     * 单个删除Snapshot
     * @param snapshotName 正则过滤表达式
     * @return
     * @throws IOException
     */
    public static void deleteSnapshot(String snapshotName){
        try {
            Admin admin = getConnection().getAdmin();
            if(StringUtils.isNotBlank(snapshotName))
                admin.deleteSnapshot(snapshotName);
            else
                logger.error("SnapshotName can't be null !");
        } catch (Exception e) {
            logger.error("Snapshot " + snapshotName + " del failed !", e);
        }
    }



	/**
	 * 检索指定表的第一行记录。<br>
	 * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
	 * @param tableName 表名称(*)。
	 * @param filterList 过滤器集合，可以为null。
	 * @return
	 */
	public static Result selectFirstResultRow(String tableName,FilterList filterList) {
		if(StringUtils.isBlank(tableName)) return null;
		Table table = null;
		try {
			table = getTable(tableName);
			Scan scan = new Scan();
			if(filterList != null) {
				scan.setFilter(filterList);
			}
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();
			int index = 0;
			while(iterator.hasNext()) {
				Result rs = iterator.next();
				if(index == 0) {
					scanner.close();
					return rs;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
    /**
     * 异步往指定表添加数据
     * @param tablename  	表名
     * @param puts	 			需要添加的数据
	 * @return long				返回执行时间
     * @throws IOException
     */
	public static long put(String tablename, List<Put> puts) throws Exception {
		long currentTime = System.currentTimeMillis();
		Connection conn = getConnection();
		final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
				for (int i = 0; i < e.getNumExceptions(); i++) {
					System.out.println("Failed to sent put " + e.getRow(i) + ".");
					logger.error("Failed to sent put " + e.getRow(i) + ".");
				}
			}
		};
		BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
				.listener(listener);
		params.writeBufferSize(5 * 1024 * 1024);

		final BufferedMutator mutator = conn.getBufferedMutator(params);
		try {
			mutator.mutate(puts);
			mutator.flush();
		} finally {
			mutator.close();
			closeConnect(conn);
		}
		return System.currentTimeMillis() - currentTime;
	}

	/**
	 * 异步往指定表添加数据
	 * @param tablename  	表名
	 * @param put	 			需要添加的数据
	 * @return long				返回执行时间
	 * @throws IOException
	 */
	public static long put(String tablename, Put put) throws Exception {
		return put(tablename, Arrays.asList(put));
	}

	/**
	 * 往指定表添加数据
	 * @param tablename  	表名
	 * @param puts	 			需要添加的数据
	 * @return long				返回执行时间
	 * @throws IOException
	 */
	public static long putByHTable(String tablename, List<?> puts) throws Exception {
		long currentTime = System.currentTimeMillis();
		Connection conn = getConnection();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(tablename));
		htable.setAutoFlushTo(false);
		htable.setWriteBufferSize(5 * 1024 * 1024);
		try {
			htable.put((List<Put>)puts);
			htable.flushCommits();
		} finally {
			htable.close();
			closeConnect(conn);
		}
		return System.currentTimeMillis() - currentTime;
	}
    
	/**
	 * 删除单条数据
	 * @param tablename
	 * @param row
	 * @throws IOException
	 */
	public static void delete(String tablename, String row) throws IOException {
		Table table = getTable(tablename);
        if(table!=null){
			try {
				Delete d = new Delete(row.getBytes());
				table.delete(d);
			} finally {
				table.close();
			}
        }
	}

	/**
	 * 删除多行数据
	 * @param tablename
	 * @param rows
	 * @throws IOException
	 */
	public static void delete(String tablename, String[] rows) throws IOException {
		Table table = getTable(tablename);
		if (table != null) {
			try {
				List<Delete> list = new ArrayList<Delete>();
				for (String row : rows) {
					Delete d = new Delete(row.getBytes());
					list.add(d);
				}
				if (list.size() > 0) {
					table.delete(list);
				}
			} finally {
				table.close();
			}
		}
	}

	/**
	 * 关闭连接
	 * @throws IOException
	 */
	public static void closeConnect(Connection conn){
		if(null != conn){
			try {
//				conn.close();
			} catch (Exception e) {
                logger.error("closeConnect failure !", e);
			}
		}
	}

	/**
	 * 获取单条数据
	 * @param tablename
	 * @param row
	 * @return
	 * @throws IOException
	 */
	public static Result getRow(String tablename, byte[] row) {
		Table table = getTable(tablename);
		Result rs = null;
		if(table!=null){
			try{
				Get g = new Get(row);
				rs = table.get(g);
			} catch (IOException e) {
                logger.error("getRow failure !", e);
			} finally{
				try {
					table.close();
				} catch (IOException e) {
                    logger.error("getRow failure !", e);
				}
			}
		}
		return rs;
	}

	/**
	 * 获取多行数据
	 * @param tablename
	 * @param rows
	 * @return
	 * @throws Exception
	 */
	public static <T> Result[] getRows(String tablename, List<T> rows) {
        Table table = getTable(tablename);
        List<Get> gets = null;
        Result[] results = null;
        try {
            if (table != null) {
                gets = new ArrayList<Get>();
                for (T row : rows) {
                    if(row!=null){
                        gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
                    }else{
                        throw new RuntimeException("hbase have no data");
                    }
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            logger.error("getRows failure !", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table.close() failure !", e);
            }
        }
		return results;
	}

	/**
	 * 扫描整张表，注意使用完要释放。
	 * @param tablename
	 * @return
	 * @throws IOException
	 */
	public static ResultScanner get(String tablename) {
		Table table = getTable(tablename);
		ResultScanner results = null;
		if (table != null) {
			try {
				Scan scan = new Scan();
				scan.setCaching(1000);
				results = table.getScanner(scan);
			} catch (IOException e) {
                logger.error("getResultScanner failure !", e);
			} finally {
				try {
					table.close();
				} catch (IOException e) {
                    logger.error("table.close() failure !", e);
				}
			}
		}
		return results;
	}

	/**
	 * 格式化输出结果
	 */
	public static void formatRow(KeyValue[] rs){
		for(KeyValue kv : rs){
			System.out.println(" column family  :  " + Bytes.toString(kv.getFamily()));
			System.out.println(" column   :  " + Bytes.toString(kv.getQualifier()));
			System.out.println(" value   :  " + Bytes.toString(kv.getValue()));
			System.out.println(" timestamp   :  " + String.valueOf(kv.getTimestamp()));
			System.out.println("--------------------");
		}
	}

	/**
	 * byte[] 类型的长整形数字转换成 long 类型
	 * @param byteNum
	 * @return
	 */
	public static long bytes2Long(byte[] byteNum) {
		long num = 0;
		for (int ix = 0; ix < 8; ++ix) {
			num <<= 8;
			num |= (byteNum[ix] & 0xff);
		}
		return num;
	}

}