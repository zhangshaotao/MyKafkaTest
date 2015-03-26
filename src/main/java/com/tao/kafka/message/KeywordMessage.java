package com.tao.kafka.message;

import kafka.message.Message;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import com.tao.kafka.bean.Keyword;

public class KeywordMessage implements Encoder<Keyword>{

	public KeywordMessage(VerifiableProperties props) {

    }
	
	public byte[] toBytes(Keyword keyword) {
		return keyword.toString().getBytes();
	}
	
	public Message toMessage(Keyword words) {
        return new Message(words.toString().getBytes());
    }
}
