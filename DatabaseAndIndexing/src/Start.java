import static java.lang.System.out;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Start {

	static String prompt = "hasanth> ";
	static String version = " Version 1";
	static String copyright = "© 2018 Hasanth Sista";
	static boolean isExit = false;
	
	static long pageSize = 512;
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {

		Initialization initialization=new Initialization();
		initialization.onStartUp();
		
		splashScreen();

		String userCommand = "";

		while (!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

	}

	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to HasanthSistaLite");
		System.out.println("HasanthSistaLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}

	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("HasanthSistaLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		//System.out.println(commandTokens);
		
		switch (commandTokens.get(0)) {
		case "select":
			System.out.println("CASE: SELECT");
			Parsing.parseQuery(userCommand);
			break;
		case "show":
			System.out.println("CASE: SHOW");
			Parsing.showTables();
			break;
		case "drop":
			System.out.println("CASE: DROP");
			Parsing.dropTable(userCommand);
			break;
		case "create":
			System.out.println("CASE: CREATE");
			Parsing.parseCreateTable(userCommand);
			break;
		case "update":
			System.out.println("CASE: UPDATE");
			Parsing.parseUpdate(userCommand);
			break;
		case "delete":
			System.out.println("CASE: DELETE");
			Parsing.parseDelete(userCommand);
			break;
		case "insert":
			System.out.println("CASE: INSERT");
			Parsing.parseInsert(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

}
