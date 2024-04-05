package org.bgu.ise.ddb;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;




@JsonAutoDetect(fieldVisibility = Visibility.NON_PRIVATE, getterVisibility = Visibility.NONE, creatorVisibility = Visibility.NON_PRIVATE)
public class MediaItems implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String title;
	
	int prodYear;
	
	
	

	/**
	 * @param title
	 * @param prodYear
	 * @param titleLength
	 */
	public MediaItems(String title, int prodYear) {
		super();
		this.title = title;
		this.prodYear = prodYear;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return the prodYear
	 */
	public int getProdYear() {
		return prodYear;
	}

	/**
	 * @param prodYear the prodYear to set
	 */
	public void setProdYear(int prodYear) {
		this.prodYear = prodYear;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MediaItems [title=" + title + ", prodYear=" + prodYear
				+ "]";
	}
	
	

}
