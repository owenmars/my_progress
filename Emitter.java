import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class Emitter {
	private static final int TIMELIMIT = 21600;	// 6 hr = 21600 sec

	private boolean chkDate (String tranDt) throws ParseException{
	  //String time1 = "2018-08-20-12:00:00";
		String time1=tranDt;
			Date today = Calendar.getInstance().getTime();
			SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
			String todaynow = formater.format(today);
		//	System.out.println("now is:" + todaynow); 
			Date date1 = formater.parse(time1);
			Date daten = formater.parse(todaynow);
			long diff = (daten.getTime() - date1.getTime())/1000;
		
			//System.out.println("time diff:" + diff);
			if (diff < TIMELIMIT) {			
				return true;
			} else {
				return false;
			}
	}		
			
	public List<String> generateFeed(String uin) throws ParseException{
	 List<String> dataFeed = new ArrayList<String>();
	 List<String> dataFeedRecent = new ArrayList<String>();

	 	//System.out.println("Emitter generateFeed:" + uin); 
		randomNum cardIdGen = new randomNum();
		randomNum itemIdGen = new randomNum();
		randomNum amtGen = new randomNum();
	
		for (int t = 0; t < 3; t++){
		dataFeed.add(cardIdGen.generateRandomId(16) + "," + itemIdGen.randomPick(9) + "," + amtGen.generateRandomId(3) + "," + "2018-08-20-22:00:00");
		}
		for (int t = 0; t < 3; t++){
		dataFeed.add(cardIdGen.generateRandomId(16) + "," + itemIdGen.randomPick(9) + "," + amtGen.generateRandomId(3) + "," + "2018-08-21-09:30:00");
		}
		dataFeed.add(uin + "," + itemIdGen.randomPick(9) + "," + amtGen.generateRandomId(3) + "," + "2018-08-20-23:00:00");
		dataFeed.add(uin + "," + itemIdGen.randomPick(9) + "," + amtGen.generateRandomId(3) + "," + "2018-08-21-08:00:00");
		dataFeed.add(uin + "," + itemIdGen.randomPick(9) + "," + amtGen.generateRandomId(3) + "," + "2018-08-21-08:25:00");
		dataFeed.add(uin + "," + itemIdGen.randomPick(9) + "," + amtGen.generateRandomId(3) + "," + "2018-08-21-09:30:00");
// dataFeed is the list of ALL transactions
		
			for (String tranrec : dataFeed) {
			String[] parts = tranrec.split(",");
			String tranrecDate = parts[3];
		//	System.out.println(tranrecDate);
		//	System.out.println(chkDate(tranrecDate));
				if (chkDate(tranrecDate)) {
					dataFeedRecent.add(tranrec);
				}
			}
// dataFeedRecent is the list of RECENT transactions (i.e. recent 6 hours)			
		return dataFeedRecent;
	}

}