import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataExtract {
	public static void main(String [] args){
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
		Date date = new Date();
		String title = sdf.format(date);
		try(java.sql.Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/mirage","mirage","Swe@0104");
				java.sql.Statement st = conn.createStatement();

				){
			st.execute("create table if not exists table"+title+" (id int, day varchar(10), code varchar(10), duedate varchar(10), time varchar(10), price int, amount int) engine = InnoDB");
			java.sql.PreparedStatement pstmt = conn.prepareStatement("insert into table"+title+" values (?,?,?,?,?,?,?)");	
			
			String path = "/home/hadoop/Daily"+title+".csv";
			BufferedReader read = new BufferedReader(new InputStreamReader(new FileInputStream(path),"big5"));
			int count = 0;
			int total = 1;
			int id = 0;
			System.out.println("start...");
			while(read.ready()){
				String text = read.readLine();
				String line = text.replaceAll("\\s", "");
				if(count==0){
					count++;
					System.out.println("skip");
					continue;			
				}
				else{
					String[] cont = line.split(",");
					
					if(cont.length>6 && cont[1].contains("TX")){
						id++;
						pstmt.setInt(1, id);
						pstmt.setString(2, cont[0]);
						pstmt.setString(3, cont[1]);
						pstmt.setString(4, cont[2]);
						pstmt.setString(5, cont[3]);
						pstmt.setInt(6, Integer.parseInt(cont[4]));
						pstmt.setInt(7, Integer.parseInt(cont[5]));
						pstmt.addBatch();
						total++;
					}
				}
				if(total%3000==0){
					System.out.println("current:"+total);
					pstmt.executeBatch();
				}	
			}
			pstmt.executeBatch();
			System.out.println("current:"+total);
			System.out.println("finished");
			read.close();
	} 
	catch (SQLException | IOException e) {
			e.printStackTrace();
		}
		
	}
}
