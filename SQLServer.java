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

//Main
public class SQLServer{
	static Connection connection;

	public static void main(String[] args) throws Exception{
		//setup
		MyDatabase db = new MyDatabase();
		runConsole(db);
	}
	
	public static void runConsole(MyDatabase db)
	{
		Scanner console = new Scanner(System.in);
		printWelcome();
		String line = "";
		String[] parts;
		String arg = "";
		int mode = 0;
		while (line != null && !line.equals("q") && !line.equals("quit"))
		{
			parts = splitLine(line);
			arg = findArg(line);
			if(mode == 0)
			{
				if(parts[0].equals("1"))
				{
					mode = 1;
				}
				else if(parts[0].equals("2"))
				{
					mode = 2;
				}
				else if(parts[0].equals("3"))
				{
					mode = 3;
				}
				else
				{
					System.out.println("Enter 1 for Browse Mode, 2 for Search Mode, or 3 for Maintenance Mode.");
				}
			}
			else if(parts.length > 0)
			{
				if (parts[0].equals("help") || argCheck(arg)) {
					printHelp(mode);	
				}
				else if(mode == 1)//Browse Mode. All "lookup" queries go here
				{
					if(parts[0].equals("2"))
					{
						mode = 2;
					}
					else if(parts[0].equals("3"))
					{
						mode = 3;
					}
				}
				else if(mode == 2)//Search Mode. All "Show" and "Aggregate" queries go here
				{
					if(parts[0].equals("1"))
					{
						mode = 1;
					}
					else if(parts[0].equals("3"))
					{
						mode = 3;
					}
				}
				else if(mode == 3)//Maintenance Mode. Delete and restore queries go here
				{
					if(parts[0].equals("1"))
					{
						mode = 1;
					}
					else if(parts[0].equals("2"))
					{
						mode = 2;
					}
					else if(parts[0].equals("deleteTables")) 
					{
						System.out.println("\nABOUT TO DELETE TABLES!! ARE YOU SURE? TYPE confirm");
						line = console.nextLine();
						if(line.contains("confirm")) {
							System.out.println("BOOM! TABLES GONE!");
							db.deleteTables();
						}
						else {
							System.out.println("DELETE CANCELLED!");
						}
					}
					else if(parts[0].equals("populateTables")) 
					{
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
				}
				
				else 
				{
					System.out.println("Type help for help");
				}
			}
			System.out.print("db > ");
			line = console.nextLine();
		}
		console.close();
	}

	//Info Functions
	private static void printWelcome()
	{
		System.out.println("Welcome to the Winnipeg Council Transparency Database!");
		System.out.println("How to use:");
		System.out.println("-----------");
		System.out.println("Browse Mode if you want to lookup data");
		System.out.println("Search Mode if you want to search for relationships in data");
		System.out.println("Maintenance Mode for clearing and re-populating the database");
		System.out.println("Enter q to quit");
	}
	private static void printHelp(int mode) 
	{
		if(mode == 1)//Browse Mode
		{
			System.out.println("Browse Mode Commands:");		
		}
		else if(mode == 2)//Search Mode
		{
			System.out.println("Search Mode Commands:");
		}
		else if(mode == 3)//Maintenance Mode
		{
			System.out.println("Maintenance Mode Commands:");
		}
	}

	//Utillity Functions
	private static String[] splitLine(String line) 
	{
		String[] parts;
		parts = line.split("\\s+");
		return parts;
	}
	private static String findArg(String line)
	{
		String arg = "";
		if (line.indexOf(" ") > 0) {
			arg = line.substring(line.indexOf(" ")).trim();
		}
		return arg;
	}
	private static boolean argCheck(String arg)
	{ //more rudimentary sql injection defence, others from preparedStatements
		return (arg.contains("-") || arg.contains("\'"));
	}
	
}

//Database connection
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

	//Queries
	public void searchWard(String ward) {
		try {
			String sqlMessage = "SELECT WID, WardE, WardF FROM Ward WHERE Ward.WardE LIKE %?% OR Ward.WardF LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, ward);
			statement.setString(2, ward);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-10d\t|\t%-20s\t|\t%-20s\t|","WID", "WardE", "WardF"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-10d\t|\t%-20s\t|\t%-20s\t|", truncateString(resultSet.getString(1),30), truncateString(resultSet.getString(2), 10), truncateString(resultSet.getString(3), 10)));
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void searchCouncillor(String name)
	{
		try
		{
			String sqlMessage = "SELECT CID, WID, present, name, phone, fax, websiteurl, YearsServed, Ward FROM Councillors NATURAL JOIN YearsServed WHERE Councillors.name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, name);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-10s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL", "YearsServed"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-20s\\t|\\t%-10s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}
	
	public void councillorByNBH(String nbhString)
	{
		try 
		{
			String sqlMessage = "SELECT CID, WID, Present, Name, Phone, Fax, WebsiteURL FROM Councillors NATURAL JOIN CouncilNeighbourhoods WHERE CouncilNeighbourhoods.name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, nbhString);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-10s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next()){
				System.out.println(String.format("%-20s\\t|\\t%-10s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		} 
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void searchCouncilYear(String year)
	{
		try
		{
			String sqlMessage = "SELECT Councillors.* FROM Councillors NATURAL JOIN YearsServed WHERE YearsServed.Year=?;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, year);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-10s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "WID", "Present", "Name", "Phone", "Fax", "WebsiteURL"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\\t|\\t%-10s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|\\t%-20s\\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void searchBusinessName(String name)
	{
		try
		{
			String sqlMessage = "SELECT TID, Name, Address, Phone, Email, isBusiness, isVendor FROM ThirdParty WHERE ThirdParty.isBusiness=TRUE AND ThirdParty.Name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, name);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","TID", "Name", "Address", "Phone", "Email", "isBusiness", "isVendor"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void businessByOwner(String owner)
	{
		try
		{
			String sqlMessage = "SELECT TID, Name, Address, Phone, Email, isBusiness, isVendor FROM ThirdParty NATURAL JOIN Owns NATURAL JOIN BusinessOwner WHERE BusinessOwner.Name LIKE %?%;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, owner);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","TID", "Name", "Address", "Phone", "Email", "isBusiness", "isVendor"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void expensesByCouncillor(String cid)
	{
		try
		{
			String sqlMessage = "SELECT BuysFrom.PurchaseID as PurchaseID, BuysFrom.Date as Date,"
					+ "ThirdParty as From, BuysFrom.ExpenseType as Type, BuysFrom.Account as Account,"
					+ "BuysFrom.Amount as Amount, BuysFrom.Description as Description,"
					+ "BuysFrom.Department as Department"
					+ "FROM Councillors JOIN BuysFrom ON Councillors.CID=BuysFrom.CID JOIN ThirdParty ON BuysFrom.Vendor=ThirdParty.TID"
					+ "WHERE Councillors.CID=? ORDER BY Date DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","PurchaseID", "Date", "From", "Type", "Account", "Amount", "Description", "Department"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void giftsByCouncillor(String cid)
	{
		try
		{
			String sqlMessage = "SELECT GID, DateRecorded, Councillor, RecipientSelf, RecipientDependent, RecipientStaff, Source, DateGifted, Reason, Intent, Gift.Description, Gift.Value"
					+ "FROM Councillors JOIN Gifts ON Councillors.CID=Gifts.CID JOIN Gift ON Gifts.GID=Gift.GID JOIN ThirdParty ON Gifts.Source=ThirdParty.TID"
					+ "WHERE Councillors.CID=? ORDER BY Gifts.DateGifted DESC;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|","GID", "DateRecorded", "Councillor", "RecipientSelf", "RecipientDependent", "RecipientStaff", "Source", "DateGifted", "Reason", "Intent", "Description", "Value"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8), resultSet.getString(9), resultSet.getString(10), resultSet.getString(11), resultSet.getString(12)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void giftsByDate(String cid, String date, String date2)
	{
		try
		{
			String sqlMessage = "SELECT GID, DateRecorded, Councillor, RecipientSelf, RecipientDependent, RecipientStaff, Source, DateGifted, Reason, Intent FROM Gifts WHERE DateGifted BETWEEN ? AND ? AND (? IS NULL OR Councillor = ?);";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, date);
			statement.setString(2, date2);
			statement.setString(3, cid);
			statement.setString(4, cid);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|","GID", "DateRecorded", "Councillor", "RecipientSelf", "RecipientDependent", "RecipientStaff", "Source", "DateGifted", "Reason", "Intent"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|\t%-20s\t|t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3), resultSet.getString(4), resultSet.getString(5), resultSet.getString(6), resultSet.getString(7), resultSet.getString(8), resultSet.getString(9), resultSet.getString(10)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void totalPriceGifts(String councillor)
	{
		try
		{
			String sqlMessage = "select councillor.cid, councillor.name, sum(gift.value) as totalValue" 
								+ "from Gifts join Gift on gifts.gid = gift.gid where councillor.name like %?% group by councillor.cid, councillor.name;";
			PreparedStatement statement = connection.prepareStatement(sqlMessage);
			statement.setString(1, councillor);
			ResultSet resultSet = statement.executeQuery();
			System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|","CID", "Name", "TotalValue"));
			System.out.println("---------------------------------------------------------------------------------------------------------------");
			while(resultSet.next())
			{
				System.out.println(String.format("%-20s\t|\t%-20s\t|\t%-20s\t|", resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)));
			}
		}
		catch (SQLException e)
		{
			e.printStackTrace(System.out);
		}
	}

	public void deleteTables() 
	{ 
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

	public void fillTables() 
	{ 
        	String[] files = {"MakeTables.sql", "Titles.sql", "Genres.sql", "People.sql", "Professions.sql",
						"AlternateTitles.sql", "TitleGenres.sql", "PeopleProfessions.sql", "KnownFor.sql",
						"HaveCrew.sql", "Movie.sql", "TVShow.sql", "Episode.sql"};
    		for (int i = 0; i < files.length; i++) 
		{ 
			try 
			{ 
				System.out.println(files[i]); 
				File myObj = new File("./SQL_Files/" + files[i]); 
				Scanner myReader = new Scanner(myObj); 
				while (myReader.hasNextLine()) 
				{ 
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
