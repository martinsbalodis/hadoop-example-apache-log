package lv.edu.linux.hadoop.apache.hadoop.apache.log;

public class GeoIPDb {
	
	static geoIpRecord[] geoip_db = {
new geoIpRecord(2130706433, 2130706433, "localhost"),
new geoIpRecord(86824960l, 86827007l, "Latvia"),
new geoIpRecord(93904896l, 93906943l, "Latvia"),
new geoIpRecord(95617024l, 95625215l, "Latvia"),
new geoIpRecord(521715712l, 521717759l, "Latvia"),
new geoIpRecord(522866688l, 522870783l, "Latvia"),
new geoIpRecord(522866688l, 522870783l, "Latvia"),



	};
	
	public static class geoIpRecord {
			
		long ip_start;
		long ip_end;
		String country;

		geoIpRecord(long ip_start, long ip_end, String country) {
			this.ip_start = ip_start;
			this.ip_end = ip_end;
			this.country = country;
		}

		int cmp(long ip) {
			if(ip < ip_start) {
				return -1;
			}
			else if(ip > ip_end) {
				return 1;
			}
			else {
				return 0;
			}
		}
	}
}
