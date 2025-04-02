import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import java.util.Properties;
import java.util.Scanner;



public class SQLServer{
	static Connection connection;

	public static void main(String[] args) throws Exception{
		//startup
		MyDatabase db = new MyDatabase();
		runConsole(db);
	}
	
	public static void runConsole(MyDatabase db)
	{
		Scanner console = new Scanner(System.in);
		System.out.println("Welcome to the Winnipeg Council Transparency Database!");
		String line = "";
		String[] parts;
		String arg = "";
		while (line != null && !line.equals("q") && !line.equals("quit"))
		{
			parts = splitLine(line);
			arg = findArg(line);
			if(parts.length > 0)
			{
				if (parts[0].equals("help") || argCheck(arg)) {
					if(argCheck(arg)) System.out.println("BAD ACTOR \n");
					printHelp();	
				}
			 	else if(parts[0].equals("deleteTables")) {
					System.out.println("\nABOUT TO DELETE TABLES!! ARE YOU SURE? TYPE confirm");
					line = console.nextLine();
					if(line.contains("confirm")) {
						System.out.println("BOOM! TABLES GONE!");
						db.deleteTables();
					}else {
						System.out.println("DELETE CANCELLED!");
					}
				}
				else if(parts[0].equals("populateTables")) {
					System.out.println("\nABOUT TO POPULATE TABLES!! ARE YOU SURE? TYPE confirm");
						line = console.nextLine();
						if(line.contains("confirm")) {
							System.out.println("BOOM! TABLES BACK IN 30 MINUTES!");
							db.fillTables();
						}
						else {
						System.out.println("REPOPULATE CANCELLED!");
						}		
				}
				else {
					System.out.println("Type help for help");
				}
		}
			System.out.print("db > ");
			line = console.nextLine();
	}
		console.close();
}
	
	private static void printHelp() 
	{
		System.out.println("CPG49 Database");
		
	}

	private static String[] splitLine(String line) {
		String[] parts;
		parts = line.split("\\s+");
		return parts;
	}
	private static String findArg(String line){
		String arg = "";
		if (line.indexOf(" ") > 0) {
			arg = line.substring(line.indexOf(" ")).trim();
		}
		return arg;
	}
	private static boolean argCheck(String arg){ //more rudimentary sql injection defence, others from preparedStatements
		return (arg.contains("-") || arg.contains("\'"));
	}
}

class MyDatabase {
	private Connection connection; 
	private static final String MAX_OUTPUT = "TOP 1000 ";

	public MyDatabase(){//constructor to establish connection to database
		//Reads config file auth.cfg for username and password to connect to database on Uranium
		Properties prop = new Properties();
		String fileName = "auth.cfg";
	
		try{
			FileInputStream configFile = new FileInputStream(fileName);
			prop.load(configFile);
			configFile.close();
		} catch (FileNotFoundException ex){
			System.out.println("Could not find config file.");
			System.exit(1);
		} catch (IOException ex) {
			System.out.println("Error reading config file.");
			System.exit(1);
		}
		String username = (prop.getProperty("username"));
		String password = (prop.getProperty("password"));

		if (username == null || password == null){
			System.out.println("Username or password not provided.");
			System.exit(1);	
		}
		
		//Creates string to connect to database
		String connectionURL = 
					"jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;" 
					+ "database=cs3380;" 
					+ "user=" + username + ";"
					+ "password=" + password + ";"
					+ "encrypt=false;"
					+ "trustServerCertificate=false;"
					+ "loginTimeout=30;";
		ResultSet resultSet = null;
		try {
			connection = DriverManager.getConnection(connectionURL);
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	//1
	public void searchTitle(String name) {
		try {
			String sqlMessage = "select OriginalTitle, StartYear, Runtime, Name "+
								"from Titles join HaveCrew on Titles.TitleID = HaveCrew.TitleID "+
								"join People on HaveCrew.NameID = People.NameID "+
								"join Professions on HaveCrew.ProfessionID = Professions.ProfessionID "+
								"where OriginalTitle like ? and ProfessionName like 'director' "+
								"order by StartYear desc;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, "%"+name+"%");
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-30s\t|\t%-10s\t|\t%-10s\t|\t%-30s","Title", "Year", "Runtime", "Director"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-30s\t|\t%-10d\t|\t%-10d\t|\t%-30s", truncateString(resultSet.getString(1),30), resultSet.getInt(2), resultSet.getInt(3), truncateString(resultSet.getString(4),30)));
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}		
	
	public void deleteTables() { 
    		try 
		{ 
    			File myObj = new File("./SQL_Files/DeleteTables.sql"); 
      			Scanner myReader = new Scanner(myObj); 
      			while (myReader.hasNextLine()) 
			{ 
        			String data = myReader.nextLine(); 
				try{
						PreparedStatement statement = connection.prepareStatement(data);
						statement.execute();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
      			}	 
      			myReader.close(); 
    		} 
		catch (FileNotFoundException e) { 
      			System.out.println("An error occurred."); 
      			e.printStackTrace(); 
    		} 
  	}

	public void fillTables() { 
        	String[] files = {"MakeTables.sql", "Titles.sql", "Genres.sql", "People.sql", "Professions.sql",
						"AlternateTitles.sql", "TitleGenres.sql", "PeopleProfessions.sql", "KnownFor.sql",
						"HaveCrew.sql", "Movie.sql", "TVShow.sql", "Episode.sql"};
    		for (int i = 0; i < files.length; i++) { 
			try { 
				System.out.println(files[i]); 
				File myObj = new File("./SQL_Files/" + files[i]); 
				Scanner myReader = new Scanner(myObj); 
				while (myReader.hasNextLine()) { 
					String data = myReader.nextLine(); 
					try
					{
						PreparedStatement statement = connection.prepareStatement(data);
						statement.execute();
					} catch (SQLException e) {
						e.printStackTrace(System.out);
					}
				} 
				myReader.close(); 
			} catch (FileNotFoundException e) { 
				System.out.println("An error occurred."); 
				e.printStackTrace(); 
			} 
    		} 
  	}

	public String truncateString(String text, int length)
	{
		if(text.length() <= length) return text;
		else return text.substring(0,length);
	}
}
