import org.apache.commons.lang.RandomStringUtils;

public class randomNum {

	public String generateRandomId(int len){ 
  		return RandomStringUtils.random(len, "0123456789"); 
  	} 
	
	public int randomPick(int limit){ 
	  		int out1 = (int)(Math.random() * limit); 
	  		return out1; 
		} 
}