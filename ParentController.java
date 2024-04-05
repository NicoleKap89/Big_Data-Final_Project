/**
 * 
 */
package org.bgu.ise.ddb;

import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.servlet.HandlerMapping;

/**
 * @author Alex
 *
 */
public class ParentController {
	
	@ModelAttribute
	public void tagController(HttpServletRequest request) {
		Set<org.springframework.http.MediaType> supportedMediaTypes = new HashSet<>();
		supportedMediaTypes.add(MediaType.APPLICATION_JSON);
		request.setAttribute(HandlerMapping.PRODUCIBLE_MEDIA_TYPES_ATTRIBUTE,
				supportedMediaTypes);
	}

}
