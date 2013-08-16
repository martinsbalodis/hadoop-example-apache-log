package lv.edu.linux.hadoop.apache.hadoop.apache.log;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class GeoIPDb {

	public List<GeoIpRecord> records = new ArrayList<GeoIpRecord>();

	GeoIPDb(String geoip_db_loc){

		try {
			Path pt = new Path(geoip_db_loc);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();
			while (line != null) {
				
				String[] data = line.split(",");
				GeoIpRecord rec = new GeoIpRecord(Long.decode(data[0]), Long.decode(data[1]), data[2]);
				records.add(rec);
				line = br.readLine();
			}
		} catch (Exception e) {
			System.err.println(e);
		}
	}

	public static class GeoIpRecord {

		long ip_start;
		long ip_end;
		String country;

		GeoIpRecord(long ip_start, long ip_end, String country) {
			this.ip_start = ip_start;
			this.ip_end = ip_end;
			this.country = country;
		}

		int cmp(long ip) {
			if (ip < ip_start) {
				return -1;
			} else if (ip > ip_end) {
				return 1;
			} else {
				return 0;
			}
		}
	}
}
