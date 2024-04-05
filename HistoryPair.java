/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

/**
 * @author Alex
 *
 */
@JsonAutoDetect(fieldVisibility = Visibility.NON_PRIVATE, getterVisibility = Visibility.NONE, creatorVisibility = Visibility.NON_PRIVATE)
public class HistoryPair  implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String credentials;
	
	Date viewtime;

	/**
	 * @param credentials
	 * @param viewtime
	 */
	public HistoryPair(String credentials, Date viewtime) {
		super();
		this.credentials = credentials;
		this.viewtime = viewtime;
	}

	/**
	 * @return the credentials
	 */
	public String getCredentials() {
		return credentials;
	}

	/**
	 * @param credentials the credentials to set
	 */
	public void setCredentials(String credentials) {
		this.credentials = credentials;
	}

	/**
	 * @return the viewtime
	 */
	public Date getViewtime() {
		return viewtime;
	}

	/**
	 * @param viewtime the viewtime to set
	 */
	public void setViewtime(Date viewtime) {
		this.viewtime = viewtime;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "HistoryPair [credentials=" + credentials + ", viewtime="
				+ viewtime + "]";
	}
	
	

}
