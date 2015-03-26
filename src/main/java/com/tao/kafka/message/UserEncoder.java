package com.tao.kafka.message;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import com.tao.kafka.bean.User;
import com.tao.kafka.util.BeanUtils;

public class UserEncoder implements Encoder<User>{

	public UserEncoder(VerifiableProperties props) {
		 
	}
	
	public byte[] toBytes(User user)
	{
		return BeanUtils.object2Bytes(user);
	}

}
