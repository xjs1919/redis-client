/**
 * 
 */
package com.github.xjs.redisclient.key;

/**
 * @author 605162215@qq.com
 *
 * @date 2017年9月11日 下午2:09:09
 */
public class PrefixAndKey {
	private KeyPrefix prefix;
	private String key;
	public KeyPrefix getPrefix() {
		return prefix;
	}
	public void setPrefix(KeyPrefix prefix) {
		this.prefix = prefix;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
}
