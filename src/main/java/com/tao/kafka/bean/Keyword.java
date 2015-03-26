package com.tao.kafka.bean;

import java.io.Serializable;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Keyword implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -674890731988120736L;
	
	private String id;
	
	private String user;
	
	private String keyword;
	
	private String date;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getKeyword() {
		return keyword;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	
	private String getCurrent()
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String date = format.format(new Date());
		return date;
	}
	
	public String toString()
	{
		String keyword = "[info kafka producer:]";
        keyword = keyword + this.getId() + "-" + this.getUser() + "-"
                + this.getKeyword() + "-" + this.getCurrent();
        return keyword;
	}
}
