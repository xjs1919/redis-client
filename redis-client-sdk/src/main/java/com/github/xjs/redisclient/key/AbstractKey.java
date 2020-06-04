/**
 * 
 */
package com.github.xjs.redisclient.key;

/**
 * @author 605162215@qq.com
 *
 * @date 2017年9月11日 上午11:31:28<br/>
 * 
 * 缓存的key的前缀只继承这个基类就可以
 */
public abstract class AbstractKey implements KeyPrefix {
	
	private String value;
	private int timeout;
	
	public AbstractKey(String value){
		this(value, NEVER_EXPIRE);
	}
	
	public AbstractKey(String value, int timeout){
		this.value = value;
		this.timeout = timeout;
	}
	
	@Override
	public String getPrefix(){
		String simpleName = this.getClass().getSimpleName();
		int pos = simpleName.indexOf("Key");
		if(pos > 0){
			simpleName = simpleName.substring(0, pos);
		}
		return simpleName + ":" + value;
	}
	
	@Override
	public int getExpireSeconds() {
		return this.timeout;
	}
}
