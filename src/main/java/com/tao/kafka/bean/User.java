package com.tao.kafka.bean;

import java.io.Serializable;

public class User implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -306528170994738123L;
	
	private String id;
	private String name;
	private String sex;
	private String age;
	private String addr;
	
	public User() {
	}

	public User(String id, String name, String sex, String age, String addr) {
		this.id = id;
		this.name = name;
		this.sex = sex;
		this.age = age;
		this.addr = addr;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSex() {
		return sex;
	}
	public void setSex(String sex) {
		this.sex = sex;
	}
	public String getAge() {
		return age;
	}
	public void setAge(String age) {
		this.age = age;
	}
	public String getAddr() {
		return addr;
	}
	public void setAddr(String addr) {
		this.addr = addr;
	}
	
	public String toString() {
		return "User [addr=" + addr + ", age=" + age + ", id=" + id + ", name="
				+ name + ", sex=" + sex + "]";
	}
}
