package com.customer.auth;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

/**
 * 用来判断hiveServer2 用户名密码
 * @author yxl
 *
 */
public class HiveServer2Auth implements PasswdAuthenticationProvider {
	
	private static final Log LOG = LogFactory
			.getLog(HiveServer2Auth.class);

	@Override
	public void Authenticate(String user, String password)
			throws AuthenticationException {
		
		if(StringUtils.isEmpty(user)|| StringUtils.isEmpty(password)){
			throw new AuthenticationException("user auth check fail");
		}
		
		HiveConf hiveConf = new HiveConf();
        Configuration conf = new Configuration(hiveConf);
        String filePath = conf.get("hive.server2.custom.authentication.file");
        LOG.info("hive.server2.custom.authentication.file [" + filePath + "]");
        File file = new File(filePath);
        if(!file.exists()){
        	   LOG.error("hiveServer2 authentication file not exits");
           throw new AuthenticationException("user [" + user + "] auth check fail");
        }
        
        FileReader fileReader = null;
		try {
			fileReader = new FileReader(file);
		} catch (FileNotFoundException e) {
			LOG.error(e);
			throw new AuthenticationException("user [" + user + "] auth check fail");
		}
        Properties properties = new Properties();
        try {
			properties.load(fileReader);
		} catch (IOException e) {
			LOG.error(e);
			throw new AuthenticationException("user [" + user + "] auth check fail");
		}
        
        String value = properties.getProperty(user);
        if(StringUtils.isEmpty(value) || !password.equals(value) ){
        	    LOG.info("valid:["+user+"] password:"+ "["+password+"] value:" + "["+value+"]");
			throw new AuthenticationException("user [" + user + "] auth check fail");
        }
		
	}
    
	
}
