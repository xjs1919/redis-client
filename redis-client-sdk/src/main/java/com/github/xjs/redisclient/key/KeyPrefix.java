/**
 * 
 */
package com.github.xjs.redisclient.key;

/**
 * @author 605162215@qq.com
 *
 * @date 2017年9月11日 上午11:31:19<br/>
 * 
 * Redis缓存的Key的前缀必须要实现这个接口，实际使用中只需要实现{@link AbstractKey}便可
 */
public interface KeyPrefix {
	public static final int NEVER_EXPIRE = 0;
	public String getPrefix();
	public int getExpireSeconds();
}
