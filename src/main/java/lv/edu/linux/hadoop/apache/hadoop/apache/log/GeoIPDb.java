package lv.edu.linux.hadoop.apache.hadoop.apache.log;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class GeoIPDb {

	public List<GeoIPDb.GeoIpRecord> records = new ArrayList<GeoIPDb.GeoIpRecord>();

	GeoIPDb(String geoip_db_loc) {
		System.out.println("Initializing geoip DB");
		try {
			Path pt = new Path(geoip_db_loc);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();
			while (line != null) {

				String[] data = line.split(",");
				GeoIPDb.GeoIpRecord rec = new GeoIPDb.GeoIpRecord(Long.decode(data[0]), Long.decode(data[1]), data[2]);
				records.add(rec);
				line = br.readLine();
			}
		} catch (Exception e) {
			System.err.println(e);
		}
	}

	String ipToCountry(String ip) {
		
		System.out.println(ip);
		
		try {
			long ip_int = ipToLong(ip);

			int bottom_record = 0;
			int top_record = records.size() - 1;

			GeoIPDb.GeoIpRecord rec;

			while (true) {
				int middle = bottom_record + ((top_record - bottom_record) / 2);
				rec = records.get(middle);
				int cmp = rec.cmp(ip_int);
				if (cmp == 0) {
					return rec.country;
				} else if (cmp > 0) {
					bottom_record = middle + 1;
				} else if (cmp < 0) {
					top_record = middle - 1;
				}
				if (top_record - bottom_record <=0) {
					break;
				}
			}
		} catch (Exception e) {
			return "Error";
		}
		return "Unknown";
	}

	public static long ipToLong(String ipAddress) {

		// @TODO ipv6 is not supported
		long result = 0;
		String[] atoms = ipAddress.split("\\.");

		for (int i = 3; i >= 0; i--) {
			result |= (Long.parseLong(atoms[3 - i]) << (i * 8));
		}

		return result & 0xFFFFFFFF;
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
