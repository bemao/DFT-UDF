import org.apache.pig.EvalFunc;  //location: $PIG_HOME/pig-0.14.0-mapr-1504.jar
import org.apache.pig.data.Tuple; //location: $PIG_HOME/pig-0.14.0-mapr-1504.jar
import org.apache.pig.data.DataBag; //location: /opt/mapr/hadoop/hadoop-2.5.1/share/hadoop/common/hadoop-common-2.5.1-mapr-1501.jar
import org.apache.pig.data.TupleFactory; //used for building new Tuples

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class DFT extends EvalFunc<Tuple>
	{
		public static ArrayList<Integer> create_date_list(ArrayList<String> dates, String first_date){
			// takes list of dates at which transactions occurred and the year
			// returns array of length 365 where each entry corresponds to transaction 
			// amount (0.0 on days with no transaction)
					
			Date start_date = null;
			ArrayList<Integer> date_list = new ArrayList<Integer>();
			
			SimpleDateFormat date_parser = new SimpleDateFormat("yyyy-MM-dd");
			try {
				start_date = date_parser.parse(first_date);
			} catch (ParseException e) {
				e.printStackTrace();
				return date_list;
			}
			
			for (String date : dates){
				try{
					Date parsed_date = date_parser.parse(date);
					long diff = parsed_date.getTime() - start_date.getTime();
					long diff_in_days = TimeUnit.MILLISECONDS.toDays(diff);				
					date_list.add((int)diff_in_days);
					
				} catch (ParseException e) {
					
				}
				
			}
			
			return date_list;
		}
	
		public static int time_span(String first_date, String last_date){
			
			SimpleDateFormat date_parser = new SimpleDateFormat("yyyy-MM-dd");
			
			Date start_date = null;
			Date end_date = null;
			
			try {
				start_date = date_parser.parse(first_date);
				end_date = date_parser.parse(last_date);
			} catch (ParseException e) {
				e.printStackTrace();
				return 0;
			}
			
			long diff = end_date.getTime() - start_date.getTime();
			long diff_in_days = TimeUnit.MILLISECONDS.toDays(diff);
			
			return (int)diff_in_days; // ok because time periods are short
		}
		
		public static Double[] DFT_k(int k, ArrayList<Integer> dts, int N, double wt){
			// return DFT_k for given k

			Double[] DFT = new Double[2];
			DFT[0] = 0.0;
			DFT[1] = 0.0;
			
			for (Integer i = 0; i<N; i++){
				if (dts.contains(i)) {
					DFT[0] += (1-wt)*Math.cos(2*Math.PI*k*i/N);
					DFT[1] += (1-wt)*Math.sin(2*Math.PI*k*i/N);
				} else {
					DFT[0] += -wt*Math.cos(2*Math.PI*k*i/N);
					DFT[1] += -wt*Math.sin(2*Math.PI*k*i/N);
				}
			}
			
			return DFT;
		}
		
		public static Double[] DFT(ArrayList<String> dates, String first_date, String last_date, int num){
			
			ArrayList<Integer> trans_dates = create_date_list(dates, first_date);
			ArrayList<Double[]> DFT_list = new ArrayList<Double[]>();
			Double[] out = new Double[num];
			
			int N = time_span(first_date, last_date);
			if (N == 0){
				return out;
				}
			Integer mp = 0;
			
			for (Integer dt : trans_dates){
				mp += dt;
			}
			
			double wt = (double)mp/(double)N;
			
			// creates DFT
			for (int k = 0; k<N; k++) {
				DFT_list.add(DFT_k(k, trans_dates, N, wt));
			}
			
			// creates power spectrum
			ArrayList<Double[]> power_spectrum = new ArrayList<Double[]>();
			int i = 0;
			for (Double[] dft : DFT_list){
				Double[] entry = new Double[2];
				
				entry[0] = i*1.0;
				entry[1] = dft[0]*dft[0] + dft[1]*dft[1];
				
				power_spectrum.add(entry);
				i++;
			}
			
			//sorts power spectrum by power, k
			
			Collections.sort(power_spectrum, new Comparator<Double[]>() {
				@Override
				public int compare(Double[] d1, Double[] d2){
					
					if (d1[1] < d2[1]){
						return 1;
					} else if (d1[1] == d2[1]) {
						if (d1[0] > d2[0]) {
							return -1;
						}
						else {
							return 1;
						}
					} else {
						return -1;
						
					}
				}
			});
			
			for (int j = 0; j<num; j++){
				out[j] = power_spectrum.get(j)[0];
			}
			
			return out;
			}
		
		public Tuple exec(Tuple input) throws IOException {
				DataBag bag = (DataBag)input.get(0);
				Iterator it = bag.iterator();
				
				ArrayList<String> dates = new ArrayList<String>();
				
				while(it.hasNext()){
					Tuple t = (Tuple)it.next();   //The bag contains a set of tuples (YYYY-mm-dd)
					if(t != null && t.size() > 0 && t.get(0) != null){
					String date = t.get(0).toString();
						if (date.length() == 10){
							dates.add(date);
							}
					}
				}
				
				String first_date = (String)input.get(1);
				String end_date = (String)input.get(2);
				int num = (int)input.get(3);
				
				TupleFactory tf = TupleFactory.getInstance();
				Tuple out_tpl = tf.newTuple(DFT(dates, first_date, end_date, num));
				
				return out_tpl;
			}
}
