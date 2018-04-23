import static java.lang.System.out;

import java.io.File;
import java.io.RandomAccessFile;

public class Initialization {

	static int pageSize = 512;

	public void onStartUp() {
		try {
			File file = new File("data");
			createDatabase(); // remove it later
			
			if (file.mkdir()) {
				System.out.println("The data folder doesn't exist, create database");
				createDatabase();
			} else {

				String[] oldFiles = file.list();
				
				boolean metaTables = false;
				boolean metaColumns = false;

				for (int i = 0; i < oldFiles.length; i++) {
					if (oldFiles[i].equals("hasanth_tables.tbl"))
						metaTables = true;
					if (oldFiles[i].equals("hasanth_columns.tbl"))
						metaColumns = true;
				}

				if (!metaTables) {
					System.out.println("Hasanth_tables does not exist, create table");
					System.out.println();
					createDatabaseMetaTable();
				}

				if (!metaColumns) {
					System.out.println("Hasanth_columns table does not exist, create table");
					System.out.println();
					createDatabaseMetaColumn();
				}

			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void createDatabase() {

		try {
			File file = new File("data");
			file.mkdir();
			String[] oldTables = file.list();
			for (int i = 0; i < oldTables.length; i++) {
				File oldFiles = new File(file, oldTables[i]);
				oldFiles.delete();
			}
		} catch (Exception e) {
			System.out.println(e);
		}

		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x00);
			
			davisbaseTablesCatalog.write(0x00); // start of content
			davisbaseTablesCatalog.write(0x00);
			

			davisbaseTablesCatalog.write(0xFF); // last page
			davisbaseTablesCatalog.write(0xFF); 
			davisbaseTablesCatalog.write(0xFF);
			davisbaseTablesCatalog.write(0xFF);
			
			davisbaseTablesCatalog.close();

		} catch (Exception e) {
			out.println("Error for the meta_tables file creation");
			out.println(e);
		}

		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/hasanth_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);
			davisbaseColumnsCatalog.write(0x0D);
			davisbaseColumnsCatalog.write(0x00);
			
			davisbaseColumnsCatalog.write(0x00); // start of content
			davisbaseColumnsCatalog.write(0x00);
			

			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			
			davisbaseColumnsCatalog.close();

		} catch (Exception e) {
			out.println("Error for the meta_columns file creation");
			out.println(e);
		}

	}

	public void createDatabaseMetaTable() {
		
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			davisbaseTablesCatalog.setLength(pageSize);
			davisbaseTablesCatalog.seek(0);
			davisbaseTablesCatalog.write(0x0D);
			davisbaseTablesCatalog.write(0x00);
			
			davisbaseTablesCatalog.write(0x00); // start of content
			davisbaseTablesCatalog.write(0x00);
			

			davisbaseTablesCatalog.write(0xFF); // last page
			davisbaseTablesCatalog.write(0xFF); 
			davisbaseTablesCatalog.write(0xFF);
			davisbaseTablesCatalog.write(0xFF);
			
			davisbaseTablesCatalog.close();

		} catch (Exception e) {
			out.println("Error for the meta_tables file creation");
			out.println(e);
		}
	}

	public void createDatabaseMetaColumn() {

		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/hasanth_columns.tbl", "rw");
			davisbaseColumnsCatalog.setLength(pageSize);
			davisbaseColumnsCatalog.seek(0);
			
			davisbaseColumnsCatalog.write(0x0D);  // leaf page
			davisbaseColumnsCatalog.write(0x00); // no of records
			

			davisbaseColumnsCatalog.write(0x00); // start of content
			davisbaseColumnsCatalog.write(0x00);
			

			davisbaseColumnsCatalog.write(0xFF); // last page
			davisbaseColumnsCatalog.write(0xFF); 
			davisbaseColumnsCatalog.write(0xFF);
			davisbaseColumnsCatalog.write(0xFF);
			
			davisbaseColumnsCatalog.close();
			
			

		} catch (Exception e) {
			out.println("Error for the meta_columns file creation");
			out.println(e);
		}
	}
}
