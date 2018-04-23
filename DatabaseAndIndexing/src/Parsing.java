import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.swing.plaf.synth.SynthSpinnerUI;

public class Parsing {
	static long pageSize = 512;

	public static void parseDelete(String updateString) {
		System.out.println("STUB: This is the deleteTable method");
		System.out.println("Parsing the string:\"" + updateString + "\"");
		RandomAccessFile meta_tables, meta_columns;

		try {

			meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			meta_columns = new RandomAccessFile("data/hasanth_columns.tbl", "rw");

			meta_tables.seek(1);
			int count = meta_tables.read();

			int pos = 8;
			int noOfColumns = 0;
			int row_id = 0;
			boolean flag = false;
			String tableName = "";

			while (count > 0) {
				meta_tables.seek(pos);
				int position = meta_tables.readShort();
				meta_tables.seek(position);
				int lengthOfTable = meta_tables.read();
				noOfColumns = meta_tables.read();
				row_id = meta_tables.readInt();

				for (int i = 0; i < lengthOfTable; i++) {
					tableName += (char) meta_tables.read();
				}
				if (tableName.equals(updateString.split(" ")[2])) {
					flag = true;
					break;
				} else {
					count--;
					if (count == 0) {
						System.out.println("TABLE NOT FOUND IN DATABASE");
					}
					pos++;
				}
			}

			int lengthOfColumn = 0, row_id_of_table = 0, row_id_of_column = 0;

			String columnName = "", columnType = "", isNullable = "";

			List<String> tableDetails = new ArrayList<>();

			String tableNameInColumnTable = "";

			int pos1 = 8;
			int copyofNoOfColumns = noOfColumns;

			List<String> columnTypes = new ArrayList<>();
			if (flag) {

				while (noOfColumns > 0) {
					lengthOfColumn = 0;
					row_id_of_table = 0;
					row_id_of_column = 0;
					columnName = "";
					columnType = "";
					isNullable = "";
					tableNameInColumnTable = "";

					meta_columns.seek(pos1);
					int position = meta_columns.readShort();
					meta_columns.seek(position);
					lengthOfColumn = meta_columns.read();
					meta_columns.seek(position + 1);
					row_id_of_column = meta_columns.readInt();
					meta_columns.seek(position + 5);
					row_id_of_table = meta_columns.readInt();

					for (int i = 0; i < tableName.length(); i++) {
						tableNameInColumnTable += (char) meta_columns.read();
					}
					for (int j = 0; j < lengthOfColumn; j++) {
						columnName += (char) meta_columns.read();
					}

					for (int k = 0; k < 4; k++) {
						columnType += (char) meta_columns.read();
					}

					columnType = getTypeForBytes(columnType);

					for (int l = 0; l < 3; l++) {
						isNullable += (char) meta_columns.read();
						if (isNullable.equals("NO") || isNullable.equals("YES") || isNullable.equals("PRI")) {
							break;
						}
					}
					if (tableNameInColumnTable.equals(tableName)) {
						tableDetails.add(row_id_of_column + " " + columnName + " " + columnType + " " + isNullable);
						noOfColumns--;
					}

					if (row_id_of_column == copyofNoOfColumns) {
						break;
					}
					pos1 += 2;
					if (pos1 == 512) {
						meta_columns.close();
						meta_columns = new RandomAccessFile("data/hasanth_columns" + columnsSize + ".tbl", "rw");
						pos1 = 8;
					}
				}

			}
			// System.out.println(tableDetails);
			List<String> dataTypes = new ArrayList<>();

			List<String> columnHeaders = new ArrayList<>();
			for (String s : tableDetails) {
				String[] a = s.split(" ");
				columnHeaders.add(a[1]);
				dataTypes.add(a[2]);
			}

			int length = updateString.split(" ").length;
			List<String> displayRecords = new ArrayList<>();

			RandomAccessFile table = new RandomAccessFile("data/" + tableName + ".tbl", "rw");

			int pos5 = 8;
			int noOfRecords = 0;
			while (true) {
				table.seek(pos5);
				if (table.readShort() != 0) {
					noOfRecords++;
				} else {
					break;
				}
				pos5 += 2;
			}

			if (noOfRecords == 0) {
				System.out.println("TABLE DOES NOT CONTAIN ANY RECORDS TO DISPLAY");
			}

			pos5 = 8;
			while (noOfRecords > 0) {
				table.seek(pos5);
				int positionOfRecord = table.readShort();
				table.seek(positionOfRecord);
				int offSet = table.readShort();
				int rowId = table.readInt();
				int noOfColumnsInTable = table.read();

				String[] columnDataBytes = new String[noOfColumnsInTable];
				List<Integer> lengthOfColumnData = new ArrayList<>();
				for (int i = 0; i < noOfColumnsInTable; i++) {
					columnDataBytes[i] = String.valueOf(table.read());
				}

				for (int j = 0; j < columnDataBytes.length; j++) {
					int c = Integer.parseInt(columnDataBytes[j], 10);
					int d = Integer.parseInt("C", 16);
					// System.out.println(c);
					if (c > 12) {
						lengthOfColumnData.add(c - d);
					} else {
						lengthOfColumnData.add(c);
					}
				}

				// System.out.println(lengthOfColumnData);

				String rowData = "";
				for (int i = 0; i < lengthOfColumnData.size(); i++) {
					String columnData = "";
					if (dataTypes.get(i).equalsIgnoreCase("TEXT")) {
						for (int j = 0; j < lengthOfColumnData.get(i); j++) {
							columnData += (char) table.read();
						}
					} else if (dataTypes.get(i).equalsIgnoreCase("INT")) {
						columnData += table.readInt();
					} else if (dataTypes.get(i).equalsIgnoreCase("DOUBLE")) {
						columnData += table.readDouble();
					}
					rowData += columnData + " ";
				}
				displayRecords.add(rowData.trim());

				noOfRecords--;
				pos5 += 2;
			}
			// System.out.println(displayRecords);
			// System.out.println(columnHeaders);
			int count10 = 0;
			if (updateString.contains("where")) {

				String searchColumn = updateString.split(" ")[4];
				String searchOperator = updateString.split(" ")[5];
				String searchValue = null;

				if (updateString.split(" ")[6].contains("\"")) {
					searchValue = updateString.split(" ")[6].substring(1, updateString.split(" ")[6].length() - 1);
				} else {
					searchValue = updateString.split(" ")[6];
				}

				int columnIndex = 0;
				for (String s : columnHeaders) {
					if (s.equalsIgnoreCase(searchColumn)) {
						columnIndex = columnHeaders.indexOf(s);
						break;
					}
				}
				int columnTobeUpdated = 0;

				// System.out.println(columnIndex +" "+searchValue);
				List<Integer> updateIndex = new ArrayList<>();

				for (int j = 0; j < displayRecords.size(); j++) {
					String[] values = displayRecords.get(j).split(" ");
					// System.out.println(values[1]);
					for (int k = 0; k < values.length; k++) {
						if (columnIndex == k) {
							if (searchOperator.equals(">")) {
								if (Integer.valueOf(values[k]) > Integer.valueOf((searchValue))) {
									updateIndex.add(j);
									break;
								}
							} else if (searchOperator.equals("<")) {
								if (Integer.valueOf(values[k]) < Integer.valueOf((searchValue))) {
									updateIndex.add(j);
									break;
								}
							} else if (searchOperator.equals("=")) {
								if (values[k].equalsIgnoreCase(searchValue)) {
									updateIndex.add(j);
									break;
								}
							}

						}
					}
				}
				// System.out.println(displayRecords);
				// System.out.println(updateIndex);

				if (updateIndex.size() == 0) {
					System.out.println("No records match for the criteria given");
				} else {
					int pos6 = 8;
					while (true) {
						table.seek(pos6);
						int post = table.readShort();
						if (post != 0) {

							table.seek(post);
							int offset = table.readShort();
							int rowid = table.readInt();
							int noc = table.read();

							String[] columnDataBytes = new String[noc];
							List<Integer> lengthOfColumnData = new ArrayList<>();
							for (int i = 0; i < noc; i++) {
								columnDataBytes[i] = String.valueOf(table.read());
							}

							for (int j = 0; j < columnDataBytes.length; j++) {
								int c = Integer.parseInt(columnDataBytes[j], 10);

								if (c == 6) {
									lengthOfColumnData.add(4);
								} else if (c == 9) {
									lengthOfColumnData.add(8);
								} else if (c > 12) {
									int d = Integer.parseInt("C", 16);
									lengthOfColumnData.add(c - d);
								} else {
									lengthOfColumnData.add(c);
								}
							}

							String rowData = "";
							for (int i = 0; i < lengthOfColumnData.size(); i++) {
								String columnData = "";
								if (dataTypes.get(i).equalsIgnoreCase("TEXT")) {
									for (int j = 0; j < lengthOfColumnData.get(i); j++) {
										columnData += (char) table.read();
									}
								} else if (dataTypes.get(i).equalsIgnoreCase("INT")) {
									columnData += table.readInt();
								} else if (dataTypes.get(i).equalsIgnoreCase("DOUBLE")) {
									columnData += table.readDouble();
								}
								rowData += columnData + " ";
							}

							boolean flagup = false;
							for (int i = 0; i < displayRecords.size(); i++) {
								String updatingRecord = displayRecords.get(i);


								if (updateIndex.contains(i)) {
									if (rowData.trim().equalsIgnoreCase(updatingRecord)) {

										flagup = true;
									} else {
										flagup = false;
									}
								}

								if (flagup) {
									String val = rowData.trim().split(" ")[columnTobeUpdated];

									table.seek(1);
									int r = table.read();
									table.seek(1);
									table.write(r - 1);

									table.seek(2);
									table.writeShort(0);
									table.seek(post);

									table.writeShort(0);
									table.writeInt(0);
									table.write(0);

									for (int m = 0; m < noc; m++) {
										table.write(0);
									}

									for (int j = 0; j < lengthOfColumnData.size(); j++) {
										for (int k = 0; k < lengthOfColumnData.get(j); k++) {
											table.write(0);
										}
									}

									table.seek(pos6);
									table.writeShort(0);
									count10++;
									if (count10 == updateIndex.size()) {
										break;
									}
								}
								if (count10 == updateIndex.size()) {
									break;
								}
							}

						}
						if (count10 == updateIndex.size()) {
							break;
						}
						pos6 += 2;

					}
				}
			}

			List<Integer> pointers1 = new ArrayList<>();

			int countZeros = 0;
			int posi = 8;
			while (true) {
				table.seek(posi);
				int val = table.readShort();

				if (countZeros == 5) {
					break;
				}

				if (val == 0) {
					posi += 2;
					countZeros++;
				} else {
					table.seek(posi);
					int b = table.readShort();
					pointers1.add(b);
					countZeros = 0;
					posi += 2;
				}

			}

			if (pointers1.size() > 0) {
				posi = 8;
				for (int i = 0; i < pointers1.size(); i++) {
					table.seek(posi);
					table.writeShort(pointers1.get(i));
					posi += 2;
				}

			}
			table.seek(posi);
			table.writeShort(0);
			table.writeShort(0);

			if (pointers1.size() > 0) {
				table.seek(2);
				table.writeShort(pointers1.get(pointers1.size() - 1));
			}
			table.seek(1);
			table.write(pointers1.size());

			table.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void parseInsert(String userCommand) {
		System.out.println("STUB: This is the insertTable method.");
		System.out.println("\tParsing the string:\"" + userCommand + "\"");

		RandomAccessFile meta_tables, meta_columns;
		try {
			meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			meta_columns = new RandomAccessFile("data/hasanth_columns.tbl", "rw");

			// System.out.println(userCommand.split(" ")[2]);
			// System.out.println(userCommand.split(" ")[4].split(","));

			meta_tables.seek(1);
			int count = meta_tables.read();

			int pos = 8;
			int noOfColumns = 0;
			int row_id = 0;
			boolean flag = false;
			String tableName = "";
			// System.out.println(count+" yes");
			while (count > 0) {
				meta_tables.seek(pos);
				int position = meta_tables.readShort();
				meta_tables.seek(position);
				int lengthOfTable = meta_tables.read();
				// System.out.println(lengthOfTable);
				noOfColumns = meta_tables.read();
				// System.out.println(noOfColumns);
				row_id = meta_tables.readInt();
				// System.out.println(row_id);

				for (int i = 0; i < lengthOfTable; i++) {
					tableName += (char) meta_tables.read();
					// System.out.println(tableName);
				}
				// System.out.println(tableName);
				if (tableName.equals(userCommand.split(" ")[2])) {
					flag = true;
					break;
				} else {
					count--;
					if (count == 0) {
						System.out.println("TABLE NOT FOUND IN DATABASE");
					}
					pos++;
				}
			}

			int lengthOfColumn = 0, row_id_of_table = 0, row_id_of_column = 0;

			String columnName = "", columnType = "", isNullable = "";

			List<String> tableDetails = new ArrayList<>();

			String tableNameInColumnTable = "";

			int pos1 = 8;
			int copyofNoOfColumns = noOfColumns;
			String[] enteredValues = null;
			String[] enteredColumnNames = null;
			List<String> columnTypes = new ArrayList<>();
			if (flag) {

				while (noOfColumns > 0) {
					lengthOfColumn = 0;
					row_id_of_table = 0;
					row_id_of_column = 0;
					columnName = "";
					columnType = "";
					isNullable = "";
					tableNameInColumnTable = "";

					meta_columns.seek(pos1);
					int position = meta_columns.readShort();
					meta_columns.seek(position);
					lengthOfColumn = meta_columns.read();
					meta_columns.seek(position + 1);
					row_id_of_column = meta_columns.readInt();
					meta_columns.seek(position + 5);
					row_id_of_table = meta_columns.readInt();

					for (int i = 0; i < tableName.length(); i++) {
						tableNameInColumnTable += (char) meta_columns.read();
					}
					for (int j = 0; j < lengthOfColumn; j++) {
						columnName += (char) meta_columns.read();
					}

					for (int k = 0; k < 4; k++) {
						columnType += (char) meta_columns.read();
					}

					// System.out.println(columnName);
					columnType = getTypeForBytes(columnType);
					// System.out.println(columnType);

					for (int l = 0; l < 3; l++) {
						isNullable += (char) meta_columns.read();
						if (isNullable.equals("NO") || isNullable.equals("YES") || isNullable.equals("PRI")) {
							break;
						}
					}
					//
					if (tableNameInColumnTable.equals(tableName)) {
						tableDetails.add(row_id_of_column + " " + columnName + " " + columnType + " " + isNullable);
						noOfColumns--;
					}

					if (row_id_of_column == copyofNoOfColumns) {
						break;
					}
					pos1 += 2;
					if (pos1 == 512) {
						meta_columns.close();
						meta_columns = new RandomAccessFile("data/hasanth_columns" + columnsSize + ".tbl", "rw");
						pos1 = 8;
					}
				}
				// System.out.println(tableDetails);

			}
			// System.out.println(userCommand.split(" ")[4]);

			enteredColumnNames = userCommand.split(" ")[3].split(",");
			// System.out.println(enteredColumnNames[0]);
			// System.out.println(enteredColumnNames[1]);

			boolean flag2 = true;
			if (enteredColumnNames.length != tableDetails.size()) {
				System.out.println("NUMBER OF COLUMNS ARE MORE THAN ACTUALLY AVAILABLE");
				flag2 = false;
			}

			String[] enteredURL = userCommand.split(" ")[5].split(",");
			if (enteredURL.length != tableDetails.size()) {
				System.out.println("Number of columns are more than values entered");
				flag2 = false;
			}

			boolean flag1 = false;

			if (flag2) {

				String[] enteredColumns = new String[copyofNoOfColumns];
				for (int i = 0; i < enteredColumnNames.length; i++) {
					if (enteredColumnNames[i].contains("(")) {
						enteredColumns[i] = enteredColumnNames[i].substring(1);
					} else if (enteredColumnNames[i].contains(")")) {
						enteredColumns[i] = enteredColumnNames[i].substring(0, enteredColumnNames[i].length() - 1);
					} else {
						enteredColumns[i] = enteredColumnNames[i];
					}
				}

				enteredValues = new String[copyofNoOfColumns];
				for (int i = 0; i < enteredURL.length; i++) {
					if (enteredURL[i].contains("(")) {
						enteredValues[i] = enteredURL[i].substring(1);
					} else if (enteredURL[i].contains(")")) {
						enteredValues[i] = enteredURL[i].substring(0, enteredURL[i].length() - 1);
					} else {
						enteredValues[i] = enteredURL[i];
					}
				}

				for (int i = 0; i < tableDetails.size(); i++) {
					String[] rows = tableDetails.get(i).split(" ");
					String columnNameInTable = rows[1];
					String columnTypeInTable = rows[2];
					String isNullableInTable = rows[3];

					columnTypes.add(columnTypeInTable);

					if (!enteredColumns[i].toLowerCase().equalsIgnoreCase(columnNameInTable.toLowerCase())) {
						System.out.println("Column Names do not match");
						break;
					}
					if (columnTypeInTable.equals("TEXT") && (isNullableInTable == "NO" || isNullableInTable == "PRI")) {
						if (enteredValues[i].contains("\"")) {
							flag1 = true;
						} else {
							flag1 = false;
							break;
						}
					} else if (columnTypeInTable.equals("TEXT") && isNullableInTable.equals("YES")) {
						if (enteredValues[i].contains("\"") || enteredValues[i].equals("null")) {

							flag1 = true;
						} else {
							flag1 = false;
							break;
						}
					}

					if (columnTypeInTable.equals("INT")
							&& (isNullableInTable.equals("NO") || isNullableInTable.equals("PRI"))) {
						if (enteredValues[i].contains("\"") || enteredValues[i].equals("null")) {

							flag1 = false;
							break;
						} else if (enteredValues[i].equals(null)) {

							flag1 = true;
						}
					} else if (columnTypeInTable.equals("INT") && isNullableInTable.equals("YES")) {
						if (enteredValues[i].contains("\"")) {

							flag1 = false;
							break;
						} else if (enteredValues[i].equals("null")) {

							flag1 = true;
						} else {
							flag1 = true;
						}
					}

					if (columnTypeInTable.equals("DOUBLE")
							&& (isNullableInTable.equals("NO") || isNullableInTable.equals("PRI"))) {
						if (enteredValues[i].contains("\"") || enteredValues[i].equals("null")) {
							flag1 = false;
							break;
						} else if (enteredValues[i].equals(null)) {
							flag1 = true;
						}
					} else if (columnTypeInTable.equals("DOUBLE") && isNullableInTable.equals("YES")) {
						if (enteredValues[i].contains("\"")) {
							flag1 = false;
							break;
						} else if (enteredValues[i].equals("null")) {
							flag1 = true;
						} else {
							flag1 = true;
						}
					}

				}

			}

			if (flag1 == true) {

				RandomAccessFile table = new RandomAccessFile("data/" + tableName + ".tbl", "rw");

				table.setLength(pageSize);
				table.seek(0);
				table.write(0x0D);
				table.write(0x00);

				table.write(0x00); // start of content
				table.write(0x00);

				table.write(0xFF); // last page
				table.write(0xFF);
				table.write(0xFF);
				table.write(0xFF);

				meta_tables.seek(0x08);
				Long pointer1 = Long.decode("0x08");
				Long location = Long.decode("0x08");

				int loc = 0;

				while (true) {
					if (!table.readBoolean()) {
						if (location == 8) {
							break;
						} else {
							table.seek(location - 1);
							loc = Integer.valueOf(table.readShort());
							break;
						}
					} else {
						pointer1 = table.getFilePointer();
						location = pointer1;
						pointer1 += 1;
						table.seek(pointer1);
					}
				}

				int lengthOfRecordBefore = 0;

				if (loc == 0) {
					lengthOfRecordBefore = 0;
				} else {
					lengthOfRecordBefore = loc;
				}

				int lengthOfInsert = 0;
				for (int i = 0; i < columnTypes.size(); i++) {
					String[] out = getBytesForType(columnTypes.get(i));
					if (columnTypes.get(i).equalsIgnoreCase("TEXT")) {
						if (enteredValues[i].equals("null")) {
							lengthOfInsert += enteredValues[i].length();
						} else {
							lengthOfInsert += enteredValues[i].length() - 2; // just
																				// changed
						}
					} else if (columnTypes.get(i).equalsIgnoreCase("DOUBLE")) {
						lengthOfInsert += Integer.valueOf(out[0]);
					} else {
						lengthOfInsert += Integer.valueOf(out[0]);
					}
				}

				int recordLocation = 0;
				if (lengthOfRecordBefore == 0) {
					recordLocation = 512 - lengthOfInsert - enteredValues.length - lengthOfRecordBefore - 7;
				} else {
					recordLocation = lengthOfRecordBefore - lengthOfInsert - enteredValues.length - 7;
				}

				table.seek(recordLocation);

				// int pos2 = (int) table.getFilePointer();

				table.writeShort(lengthOfInsert + 7 + enteredValues.length);

				table.seek(0x01);
				int recordsSizeForRow_Id = table.read(); // for number of
															// records in
															// table
				table.seek(recordLocation + 2);
				table.writeInt(recordsSizeForRow_Id + 1);

				table.write(enteredValues.length);

				for (int i = 0; i < columnTypes.size(); i++) {
					String[] out = getBytesForType(columnTypes.get(i).toString());
					// System.out.println(out[1]);
					if (out[1].equals("0x0C")) {
						// System.out.println(enteredValues[i].length());
						int num = 0;
						if (enteredValues[i].equals("null")) {
							num = 12 + enteredValues[i].length();
						} else {
							num = 12 + enteredValues[i].length() - 2;
						}
						table.write(num);
					} else if (out[1].equals("0x06")) {
						// System.out.println(Integer.valueOf(out[1].substring(out[1].length()-2,
						// out[1].length())));
						table.write(Integer.valueOf(out[1].substring(out[1].length() - 2, out[1].length())));
					} else if (out[1].equals("0x09")) {
						table.write(Integer.valueOf(out[1].substring(out[1].length() - 2, out[1].length())));
					}
				}

				for (int i = 0; i < enteredValues.length; i++) {
					if (columnTypes.get(i).equalsIgnoreCase("TEXT")) {
						if (enteredValues[i].equals("null")) {
							for (int j = 0; j < enteredValues[i].length(); j++) {
								table.write(enteredValues[i].charAt(j));
							}
						} else {
							for (int j = 1; j < enteredValues[i].length(); j++) {
								if (j != enteredValues[i].length() - 1) {
									table.write(enteredValues[i].charAt(j));
								}
							}
						}
					} else if (columnTypes.get(i).equalsIgnoreCase("INT")) {
						table.writeInt(Integer.valueOf(enteredValues[i]));

					} else if (columnTypes.get(i).equalsIgnoreCase("DOUBLE")) {
						table.writeDouble(Double.valueOf(enteredValues[i]));
					}
				}

				// table.seek(1);
				// int o = table.read(); // for number of records
				// // in
				// // table
				// // System.out.println(recordsSize);
				// //System.out.println(o);
				// table.seek(0x01);
				//
				// int m = o+1;
				// //System.out.println(m);
				// table.write(m); // not updating
				table.seek(1);
				int r = table.read();
				table.seek(1);
				table.write(r + 1);
				table.writeShort(recordLocation);

				table.seek(8);
				Long pointer = Long.decode("0x08");

				int pos2 = recordLocation;

				while (true) {
					if (!table.readBoolean()) {
						table.seek(pointer);
						table.writeShort(pos2);
						break;
					}
					pointer = table.getFilePointer();
					pointer += 1;
					table.seek(pointer);
				}

				table.close();

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void dropTable(String dropTableString) {
		System.out.println("STUB: This is the dropTable method.");
		System.out.println("\tParsing the string:\"" + dropTableString + "\"");

		RandomAccessFile meta_tables, meta_columns;
		try {
			meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			meta_columns = new RandomAccessFile("data/hasanth_columns.tbl", "rw");

			int pos = 8;
			int columns = 0;
			while (true) {
				meta_tables.seek(pos);
				int positionOfTable = meta_tables.readShort();
				if (positionOfTable == 0) {
					System.out.println("TABLE DOESNT EXIST IN DATABASE");
					break;
				} else {

					int positionOfTableForDeletionStart = positionOfTable;
					meta_tables.seek(positionOfTable);

					int length = meta_tables.read();
					columns = meta_tables.read();

					positionOfTable = positionOfTable + 2;
					meta_tables.seek(positionOfTable);
					int row_id = meta_tables.readInt();
					String tableName = "";
					int i = 0;
					do {
						int a = meta_tables.read();
						tableName += String.valueOf((char) (a));
						i++;
					} while (i < length);

					if (tableName.equals(dropTableString.split(" ")[2])) {
						int totalLength = positionOfTableForDeletionStart + 1 + 4 + tableName.length();
						meta_tables.seek(positionOfTableForDeletionStart);
						for (int m = positionOfTableForDeletionStart; m <= totalLength; m++) {
							meta_tables.write(0);
						}
						meta_tables.seek(pos);
						meta_tables.write(0);
						meta_tables.write(0);

						meta_tables.seek(1);
						int recordsSize = meta_tables.read();
						meta_tables.seek(1);
						meta_tables.write(recordsSize - 1);

						meta_tables.seek(2);
						int position = meta_tables.readShort();
						if (positionOfTableForDeletionStart == position) {
							meta_tables.seek(2);
							meta_tables.writeShort(0);
						}

						File file = new File("data");
						String[] oldFiles = file.list();

						for (int k = 0; k < oldFiles.length; k++) {
							if (oldFiles[k].equals(tableName + ".tbl")) {
								File oldfiles = new File(file, oldFiles[k]);
								oldfiles.delete();
							}
						}

						int pos1 = 8;
						while (true) {
							meta_columns.seek(pos1);
							int position1 = meta_columns.readShort();
							if (position1 == 0) {
								break;
							}
							meta_columns.seek(position1);
							meta_columns.seek(position1 + 9);
							String nameOfTable = "";
							for (int a = 0; a < tableName.length(); a++) {
								char b = (char) meta_columns.read();
								nameOfTable += String.valueOf(b);
								a++;
							}
							if (tableName.contains(nameOfTable)) {

								meta_columns.seek(pos1);
								meta_columns.writeShort(0);

								meta_columns.seek(1);
								int count = meta_columns.read();
								meta_columns.seek(1);
								meta_columns.write(count - 1);

								meta_columns.seek(2);
								int shortPos = meta_columns.readShort();
								if (shortPos == position1) {
									meta_columns.seek(pos + 2);
									int replacePlace = meta_columns.readShort();
									meta_columns.seek(2);
									meta_columns.writeShort(replacePlace);
								}

								meta_columns.seek(position1);
								// System.out.println(position1);// 454

								int a = meta_columns.read();

								meta_columns.seek(position1);
								meta_columns.write(0);
								meta_columns.writeInt(0);
								meta_columns.writeInt(0);

								int currentPos = position1 + 9;
								meta_columns.seek(currentPos);
								while (true) {
									int j = meta_columns.read();
									if (j > 9) {
										meta_columns.seek(currentPos);
										meta_columns.write(0);
										currentPos++;
										meta_columns.seek(currentPos);
									} else {
										break;
									}
								}

							}

							pos1 += 2;
						}

						System.out.println("TABLE SUCCESSFULLY DELETED");
						break;

					}

				}
				pos += 2;

			}
			meta_columns.seek(1);
			columns = meta_columns.read();
			meta_columns.close();
			meta_tables.close();

			rearrangeTable(columns);
		} catch (

		Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void rearrangeTable(int columns) {
		try {
			// System.out.println("inside rearrange");
			RandomAccessFile meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			meta_tables.seek(1);
			int a = meta_tables.read();
			int count = 0;
			List<Integer> pointers1 = new ArrayList<>();
			if (a != 0) {
				int pos = 8;
				while (a > 0) {
					meta_tables.seek(pos);
					int val = meta_tables.read();
					if (val == 0) {
						pos += 2;
					} else {
						meta_tables.seek(pos);
						pointers1.add((int) meta_tables.readShort());
						count++;
						a = a - 1;
					}
				}

			}
			if (pointers1.size() > 0) {
				int pos = 8;
				for (int i = 0; i < pointers1.size(); i++) {
					meta_tables.seek(pos);
					meta_tables.writeShort(pointers1.get(i));
					pos += 2;
				}
				meta_tables.seek(2);
				meta_tables.writeShort(pointers1.get(pointers1.size() - 1));

			}
			meta_tables.close();

			RandomAccessFile meta_columns = new RandomAccessFile("data/hasanth_columns.tbl", "rw");
			// System.out.println(columns);
			List<Integer> pointers = new ArrayList<>();
			int a1 = columns;
			// System.out.println(a1);
			if (a1 != 0) {
				int pos1 = 8;
				while (a1 > 0) {
					meta_columns.seek(pos1);
					int val1 = meta_columns.read();
					if (val1 == 0) {
						pos1 += 2;
					} else {

						meta_columns.seek(pos1);
						pointers.add((int) meta_columns.readShort());
						// after deletion rearranging columns incomplete
						a1 = a1 - 1;
						pos1 += 2;
					}
				}
				// System.out.println(pointers);

				meta_columns.seek(8);
				for (int i = 0; i < pointers.size(); i++) {
					meta_columns.writeShort(pointers.get(i));
				}
				meta_columns.seek(2);
				meta_columns.writeShort(pointers.get(pointers.size() - 1));

			}
			meta_columns.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void parseQuery(String queryString) {
		System.out.println("STUB: This is the parseQuery method");
		System.out.println("\tParsing the string:\"" + queryString + "\"");

		RandomAccessFile meta_tables, meta_columns;

		try {

			meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			meta_columns = new RandomAccessFile("data/hasanth_columns.tbl", "rw");

			meta_tables.seek(1);
			int count = meta_tables.read();

			int pos = 8;
			int noOfColumns = 0;
			int row_id = 0;
			boolean flag = false;
			String tableName = "";

			while (count > 0) {
				meta_tables.seek(pos);
				int position = meta_tables.readShort();
				meta_tables.seek(position);
				int lengthOfTable = meta_tables.read();
				noOfColumns = meta_tables.read();
				row_id = meta_tables.readInt();

				for (int i = 0; i < lengthOfTable; i++) {
					tableName += (char) meta_tables.read();
				}
				if (tableName.equals(queryString.split(" ")[3])) {
					flag = true;
					break;
				} else {
					count--;
					if (count == 0) {
						System.out.println("TABLE NOT FOUND IN DATABASE");
					}
					pos++;
				}
			}

			int lengthOfColumn = 0, row_id_of_table = 0, row_id_of_column = 0;

			String columnName = "", columnType = "", isNullable = "";

			List<String> tableDetails = new ArrayList<>();

			String tableNameInColumnTable = "";

			int pos1 = 8;
			int copyofNoOfColumns = noOfColumns;

			List<String> columnTypes = new ArrayList<>();
			if (flag) {

				while (noOfColumns > 0) {
					lengthOfColumn = 0;
					row_id_of_table = 0;
					row_id_of_column = 0;
					columnName = "";
					columnType = "";
					isNullable = "";
					tableNameInColumnTable = "";

					meta_columns.seek(pos1);
					int position = meta_columns.readShort();
					meta_columns.seek(position);
					lengthOfColumn = meta_columns.read();
					meta_columns.seek(position + 1);
					row_id_of_column = meta_columns.readInt();
					meta_columns.seek(position + 5);
					row_id_of_table = meta_columns.readInt();

					for (int i = 0; i < tableName.length(); i++) {
						tableNameInColumnTable += (char) meta_columns.read();
					}
					for (int j = 0; j < lengthOfColumn; j++) {
						columnName += (char) meta_columns.read();
					}

					for (int k = 0; k < 4; k++) {
						columnType += (char) meta_columns.read();
					}

					columnType = getTypeForBytes(columnType);

					for (int l = 0; l < 3; l++) {
						isNullable += (char) meta_columns.read();
						if (isNullable.equals("NO") || isNullable.equals("YES") || isNullable.equals("PRI")) {
							break;
						}
					}
					if (tableNameInColumnTable.equals(tableName)) {
						tableDetails.add(row_id_of_column + " " + columnName + " " + columnType + " " + isNullable);
						noOfColumns--;
					}

					if (row_id_of_column == copyofNoOfColumns) {
						break;
					}
					pos1 += 2;
					if (pos1 == 512) {
						meta_columns.close();
						meta_columns = new RandomAccessFile("data/hasanth_columns" + columnsSize + ".tbl", "rw");
						pos1 = 8;
					}
				}

			}
			// System.out.println(tableDetails);
			List<String> dataTypes = new ArrayList<>();

			List<String> columnHeaders = new ArrayList<>();
			for (String s : tableDetails) {
				String[] a = s.split(" ");
				columnHeaders.add(a[1]);
				dataTypes.add(a[2]);
			}

			int length = queryString.split(" ").length;
			List<String> displayRecords = new ArrayList<>();

			RandomAccessFile table = new RandomAccessFile("data/" + tableName + ".tbl", "rw");

			int pos5 = 8;
			int noOfRecords = 0;
			while (true) {
				table.seek(pos5);
				if (table.readShort() != 0) {
					noOfRecords++;
				} else {
					break;
				}
				pos5 += 2;
			}

			if (noOfRecords == 0) {
				System.out.println("TABLE DOES NOT CONTAIN ANY RECORDS TO DISPLAY");
			}

			pos5 = 8;
			while (noOfRecords > 0) {
				table.seek(pos5);
				int positionOfRecord = table.readShort();
				table.seek(positionOfRecord);
				int offSet = table.readShort();
				int rowId = table.readInt();
				int noOfColumnsInTable = table.read();

				String[] columnDataBytes = new String[noOfColumnsInTable];
				List<Integer> lengthOfColumnData = new ArrayList<>();
				for (int i = 0; i < noOfColumnsInTable; i++) {
					columnDataBytes[i] = String.valueOf(table.read());
				}

				for (int j = 0; j < columnDataBytes.length; j++) {
					int c = Integer.parseInt(columnDataBytes[j], 10);
					int d = Integer.parseInt("C", 16);
					// System.out.println(c);
					if (c > 12) {
						lengthOfColumnData.add(c - d);
					} else {
						lengthOfColumnData.add(c);
					}
				}

				// System.out.println(lengthOfColumnData);

				String rowData = "";
				for (int i = 0; i < lengthOfColumnData.size(); i++) {
					String columnData = "";
					if (dataTypes.get(i).equalsIgnoreCase("TEXT")) {
						for (int j = 0; j < lengthOfColumnData.get(i); j++) {
							columnData += (char) table.read();
						}
					} else if (dataTypes.get(i).equalsIgnoreCase("INT")) {
						columnData += table.readInt();
					} else if (dataTypes.get(i).equalsIgnoreCase("DOUBLE")) {
						columnData += table.readDouble();
					}
					rowData += columnData + " ";
				}
				displayRecords.add(rowData.trim());

				noOfRecords--;
				pos5 += 2;
			}
			// System.out.println(displayRecords);
			// System.out.println(columnHeaders);
			if (queryString.contains("where")) {

				String searchColumn = queryString.split(" ")[5];
				String searchOperator = queryString.split(" ")[6];
				String searchValue = null;
				if (queryString.split(" ")[7].contains("\"")) {
					searchValue = queryString.split(" ")[7].substring(1, queryString.split(" ")[7].length() - 1);
				} else {
					searchValue = queryString.split(" ")[7];
				}

				int columnIndex = 0;
				for (String s : columnHeaders) {
					if (s.equalsIgnoreCase(searchColumn)) {
						columnIndex = columnHeaders.indexOf(s);
						break;
					}
				}

				List<Integer> indexesToPrint = new ArrayList<>();
				if (searchOperator.equals(">")) {
					for (int j = 0; j < displayRecords.size(); j++) {
						String[] values = displayRecords.get(j).split(" ");
						for (int k = 0; k < values.length; k++) {
							if (columnIndex == k) {
								if (Integer.valueOf(values[k]) > Integer.valueOf(searchValue)) {
									indexesToPrint.add(j);
								}
							}
						}
					}

				} else if (searchOperator.equals("<")) {
					for (int j = 0; j < displayRecords.size(); j++) {
						String[] values = displayRecords.get(j).split(" ");
						for (int k = 0; k < values.length; k++) {
							if (columnIndex == k) {
								if (Integer.valueOf(values[k]) < Integer.valueOf(searchValue)) {
									indexesToPrint.add(j);
								}
							}
						}
					}
				} else if (searchOperator.equals("=")) {
					for (int j = 0; j < displayRecords.size(); j++) {
						String[] values = displayRecords.get(j).split(" ");
						for (int k = 0; k < values.length; k++) {
							if (columnIndex == k) {
								if (searchValue.equalsIgnoreCase(values[k])) {
									indexesToPrint.add(j);
								}
							}
						}
					}

				}

				if (indexesToPrint.size() == 0) {
					System.out.println("Search Criteria did not match any records");
				} else {

					System.out.println(line("-", 80));
					System.out.print(line(" ", 10));
					for (int i = 0; i < columnHeaders.size(); i++) {
						System.out.print(columnHeaders.get(i) + line(" ", 15));
					}
					System.out.println();
					System.out.println(line("-", 80));
					for (int j = 0; j < displayRecords.size(); j++) {
						String[] values = displayRecords.get(j).split(" ");
						if (indexesToPrint.contains(j)) {
							System.out.print(line(" ", 10));
							for (int k = 0; k < values.length; k++) {
								System.out.print(values[k] + line(" ", 15));
							}
							System.out.println();
						}

					}
					System.out.println(line("-", 80));

				}

			} else {
				System.out.println(line("-", 80));
				System.out.print(line(" ", 10));
				for (int i = 0; i < columnHeaders.size(); i++) {
					System.out.print(columnHeaders.get(i) + line(" ", 15));
				}
				System.out.println();
				System.out.println(line("-", 80));
				for (int j = 0; j < displayRecords.size(); j++) {
					String[] values = displayRecords.get(j).split(" ");
					System.out.print(line(" ", 10));
					for (int k = 0; k < values.length; k++) {
						System.out.print(values[k] + line(" ", 15));
					}
					System.out.println();
				}
				System.out.println(line("-", 80));

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void parseUpdate(String updateString) {
		System.out.println("STUB: This is the updateTable method");
		System.out.println("Parsing the string:\"" + updateString + "\"");
		RandomAccessFile meta_tables, meta_columns;

		try {

			meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");
			meta_columns = new RandomAccessFile("data/hasanth_columns.tbl", "rw");

			meta_tables.seek(1);
			int count = meta_tables.read();

			int pos = 8;
			int noOfColumns = 0;
			int row_id = 0;
			boolean flag = false;
			String tableName = "";

			while (count > 0) {
				meta_tables.seek(pos);
				int position = meta_tables.readShort();
				meta_tables.seek(position);
				int lengthOfTable = meta_tables.read();
				noOfColumns = meta_tables.read();
				row_id = meta_tables.readInt();

				for (int i = 0; i < lengthOfTable; i++) {
					tableName += (char) meta_tables.read();
				}
				if (tableName.equals(updateString.split(" ")[1])) {
					flag = true;
					break;
				} else {
					count--;
					if (count == 0) {
						System.out.println("TABLE NOT FOUND IN DATABASE");
					}
					pos++;
				}
			}

			int lengthOfColumn = 0, row_id_of_table = 0, row_id_of_column = 0;

			String columnName = "", columnType = "", isNullable = "";

			List<String> tableDetails = new ArrayList<>();

			String tableNameInColumnTable = "";

			int pos1 = 8;
			int copyofNoOfColumns = noOfColumns;

			List<String> columnTypes = new ArrayList<>();
			if (flag) {

				while (noOfColumns > 0) {
					lengthOfColumn = 0;
					row_id_of_table = 0;
					row_id_of_column = 0;
					columnName = "";
					columnType = "";
					isNullable = "";
					tableNameInColumnTable = "";

					meta_columns.seek(pos1);
					int position = meta_columns.readShort();
					meta_columns.seek(position);
					lengthOfColumn = meta_columns.read();
					meta_columns.seek(position + 1);
					row_id_of_column = meta_columns.readInt();
					meta_columns.seek(position + 5);
					row_id_of_table = meta_columns.readInt();

					for (int i = 0; i < tableName.length(); i++) {
						tableNameInColumnTable += (char) meta_columns.read();
					}
					for (int j = 0; j < lengthOfColumn; j++) {
						columnName += (char) meta_columns.read();
					}

					for (int k = 0; k < 4; k++) {
						columnType += (char) meta_columns.read();
					}

					columnType = getTypeForBytes(columnType);

					for (int l = 0; l < 3; l++) {
						isNullable += (char) meta_columns.read();
						if (isNullable.equals("NO") || isNullable.equals("YES") || isNullable.equals("PRI")) {
							break;
						}
					}
					if (tableNameInColumnTable.equals(tableName)) {
						tableDetails.add(row_id_of_column + " " + columnName + " " + columnType + " " + isNullable);
						noOfColumns--;
					}

					if (row_id_of_column == copyofNoOfColumns) {
						break;
					}
					pos1 += 2;
					if (pos1 == 512) {
						meta_columns.close();
						meta_columns = new RandomAccessFile("data/hasanth_columns" + columnsSize + ".tbl", "rw");
						pos1 = 8;
					}
				}

			}
			// System.out.println(tableDetails);
			List<String> dataTypes = new ArrayList<>();

			List<String> columnHeaders = new ArrayList<>();
			for (String s : tableDetails) {
				String[] a = s.split(" ");
				columnHeaders.add(a[1]);
				dataTypes.add(a[2]);
			}

			int length = updateString.split(" ").length;
			List<String> displayRecords = new ArrayList<>();

			RandomAccessFile table = new RandomAccessFile("data/" + tableName + ".tbl", "rw");

			int pos5 = 8;
			int noOfRecords = 0;
			while (true) {
				table.seek(pos5);
				if (table.readShort() != 0) {
					noOfRecords++;
				} else {
					break;
				}
				pos5 += 2;
			}

			if (noOfRecords == 0) {
				System.out.println("TABLE DOES NOT CONTAIN ANY RECORDS TO DISPLAY");
			}

			pos5 = 8;
			while (noOfRecords > 0) {
				table.seek(pos5);
				int positionOfRecord = table.readShort();
				table.seek(positionOfRecord);
				int offSet = table.readShort();
				int rowId = table.readInt();
				int noOfColumnsInTable = table.read();

				String[] columnDataBytes = new String[noOfColumnsInTable];
				List<Integer> lengthOfColumnData = new ArrayList<>();
				for (int i = 0; i < noOfColumnsInTable; i++) {
					columnDataBytes[i] = String.valueOf(table.read());
				}

				for (int j = 0; j < columnDataBytes.length; j++) {
					int c = Integer.parseInt(columnDataBytes[j], 10);
					int d = Integer.parseInt("C", 16);

					if (c > 12) {
						lengthOfColumnData.add(c - d);
					} else {
						lengthOfColumnData.add(c);
					}
				}

				// System.out.println(lengthOfColumnData);

				String rowData = "";
				for (int i = 0; i < lengthOfColumnData.size(); i++) {
					String columnData = "";
					if (dataTypes.get(i).equalsIgnoreCase("TEXT")) {
						for (int j = 0; j < lengthOfColumnData.get(i); j++) {
							columnData += (char) table.read();
						}
					} else if (dataTypes.get(i).equalsIgnoreCase("INT")) {
						columnData += table.readInt();
					} else if (dataTypes.get(i).equalsIgnoreCase("DOUBLE")) {
						columnData += table.readDouble();
					}
					rowData += columnData + " ";
				}
				displayRecords.add(rowData.trim());

				noOfRecords--;
				pos5 += 2;
			}
			// System.out.println(displayRecords);
			// System.out.println(columnHeaders);

			int count10 = 0;
			if (updateString.contains("where")) {

				String setColumn = updateString.split(" ")[3];
				String setValue = updateString.split(" ")[5];

				String searchColumn = updateString.split(" ")[7];
				String searchOperator = updateString.split(" ")[8];
				String searchValue = null;

				if (updateString.split(" ")[9].contains("\"")) {
					searchValue = updateString.split(" ")[9].substring(1, updateString.split(" ")[9].length() - 1);
				} else {
					searchValue = updateString.split(" ")[9];
				}

				int columnIndex = 0;
				for (String s : columnHeaders) {
					if (s.equalsIgnoreCase(searchColumn)) {
						columnIndex = columnHeaders.indexOf(s);
						break;
					}
				}
				int columnTobeUpdated = 0;

				for (String s : columnHeaders) {
					if (s.equals(setColumn)) {
						columnTobeUpdated = columnHeaders.indexOf(s);
						break;
					}
				}

				List<Integer> updateIndex = new ArrayList<>();

				for (int j = 0; j < displayRecords.size(); j++) {
					String[] values = displayRecords.get(j).split(" ");
					for (int k = 0; k < values.length; k++) {
						if (columnIndex == k) {
							if (searchOperator.equals(">")) {
								if (Integer.valueOf(values[k]) > Integer.valueOf((searchValue))) {
									updateIndex.add(j);
									break;
								}
							} else if (searchOperator.equals("<")) {
								if (Integer.valueOf(values[k]) < Integer.valueOf((searchValue))) {
									updateIndex.add(j);
									break;
								}
							} else if (searchOperator.equals("=")) {
								if (values[k].equalsIgnoreCase(searchValue)) {
									updateIndex.add(j);
									break;
								}
							}

						}
					}
				}
				// System.out.println(displayRecords);
				// System.out.println(updateIndex);
				// System.out.println(columnTobeUpdated);

				if (updateIndex.size() == 0) {
					System.out.println("No records match for the criteria given");
				} else {
					int pos6 = 8;
					while (true) {
						table.seek(pos6);
						int post = table.readShort();

						table.seek(post);
						int offset = table.readShort();
						int rowid = table.readInt();
						int noc = table.read();

						String[] columnDataBytes = new String[noc];
						List<Integer> lengthOfColumnData = new ArrayList<>();
						for (int i = 0; i < noc; i++) {
							columnDataBytes[i] = String.valueOf(table.read());
						}

						for (int j = 0; j < columnDataBytes.length; j++) {
							int c = Integer.parseInt(columnDataBytes[j], 10);

							if (c == 6) {
								lengthOfColumnData.add(4);
							} else if (c == 9) {
								lengthOfColumnData.add(8);
							} else if (c > 12) {
								int d = Integer.parseInt("C", 16);
								lengthOfColumnData.add(c - d);
							} else {
								lengthOfColumnData.add(c);
							}
						}

						String rowData = "";
						for (int i = 0; i < dataTypes.size(); i++) {
							String columnData = "";
							if (dataTypes.get(i).equalsIgnoreCase("TEXT")) {
								for (int j = 0; j < lengthOfColumnData.get(i); j++) {
									columnData += (char) table.read();
								}
							} else if (dataTypes.get(i).equalsIgnoreCase("INT")) {
								columnData += table.readInt();
							} else if (dataTypes.get(i).equalsIgnoreCase("DOUBLE")) {
								columnData += table.readDouble();
							}
							rowData += columnData + " ";
						}

						boolean flagup = false;
						for (int i = 0; i < displayRecords.size(); i++) {

							String updatingRecord = displayRecords.get(i);

							if (updateIndex.contains(i)) {
								if (rowData.trim().equalsIgnoreCase(updatingRecord)) {

									flagup = true;
								} else {
									flagup = false;
								}
							}

							if (flagup) {
								String val = rowData.trim().split(" ")[columnTobeUpdated];

								table.seek(2);
								if (post == table.readShort()) {
									table.writeShort(0);
								}

								table.seek(post);

								table.writeShort(0);
								table.writeInt(0);
								table.write(0);

								for (int m = 0; m < noc; m++) {
									table.write(0);
								}

								for (int j = 0; j < lengthOfColumnData.size(); j++) {
									for (int k = 0; k < lengthOfColumnData.get(j); k++) {
										table.write(0);
									}
								}

								String updatedValues = "";
								for (int e = 0; e < rowData.trim().split(" ").length; e++) {
									if (e == columnTobeUpdated) {
										updatedValues += setValue.substring(1, setValue.length() - 1) + " ";
									} else {
										updatedValues += rowData.trim().split(" ")[e] + " ";
									}
								}
								// System.out.println(rowData);
								// System.out.println(updatedValues.trim());

								String string = "insert into " + updateString.split(" ")[1] + " (";

								for (int t = 0; t < columnHeaders.size(); t++) {
									if (t == columnHeaders.size() - 1) {
										string += columnHeaders.get(t) + ")";
									} else {
										string += columnHeaders.get(t) + ",";
									}

								}
								string += " values (";
								for (int y = 0; y < updatedValues.trim().split(" ").length; y++) {

									if (tableDetails.get(y).split(" ")[2].equalsIgnoreCase("TEXT")) {
										if (y == updatedValues.trim().split(" ").length - 1) {
											string += "\"" + updatedValues.trim().split(" ")[y] + "\")";
										} else {
											string += "\"" + updatedValues.trim().split(" ")[y] + "\",";
										}
									} else {

										if (y == updatedValues.trim().split(" ").length - 1) {
											string += updatedValues.trim().split(" ")[y] + ")";
										} else {
											string += updatedValues.trim().split(" ")[y] + ",";
										}
									}
								}
								Parsing.parseInsert(string);
								count10++;
								if (updateIndex.size() == count10) {
									break;
								}

							}

						}
						if (updateIndex.size() == count10) {
							break;
						}

						pos6 += 2;
					}
				}

			}

			int byt = 8;
			int count11 = count10;
			while (count11 > 0) {

				table.seek(byt);
				int res = table.readShort();
				table.seek(res);
				if (table.readShort() == 0) {
					table.seek(byt);
					table.writeShort(0);
					count11--;
				} else {
					byt += 2;
				}
			}

			List<Integer> pointers1 = new ArrayList<>();

			int countZeros = 0;
			int posi = 8;
			while (true) {
				table.seek(posi);
				int val = table.readShort();

				if (countZeros == 5) {
					break;
				}

				if (val == 0) {
					posi += 2;
					countZeros++;
				} else {
					table.seek(posi);
					int b = table.readShort();
					pointers1.add(b);
					countZeros = 0;
					posi += 2;
				}

			}

			if (pointers1.size() > 0) {
				posi = 8;
				for (int i = 0; i < pointers1.size(); i++) {
					table.seek(posi);
					table.writeShort(pointers1.get(i));
					posi += 2;
				}

			}
			table.seek(posi);
			table.writeShort(0);
			table.writeShort(0);

			table.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void showTables() {
		System.out.println("STUB: This is the Show Tables method");
		try {
			RandomAccessFile meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");

			int pos = 8;
			HashMap<Integer, String> hashMap = new HashMap<>();

			while (true) {
				meta_tables.seek(pos);
				int positionOfTable = meta_tables.readShort();
				if (positionOfTable == 0) {
					break;
				} else {
					meta_tables.seek(positionOfTable);

					int length = meta_tables.read();
					positionOfTable = positionOfTable + 2;
					meta_tables.seek(positionOfTable);
					int row_id = meta_tables.readInt();
					String tableName = "";
					int i = 0;
					do {
						int a = meta_tables.read();
						tableName += String.valueOf((char) (a));
						i++;
					} while (i < length);

					hashMap.put(row_id, tableName);
					pos += 2;
				}
			}

			if (hashMap.size() == 0) {
				System.out.println(line("-", 80));
				System.out.println(line(" ", 10) + "row_id" + line(" ", 20) + "table name");
				System.out.println(line("-", 80));

				System.out.println(line(" ", 10) + "1" + line(" ", 25) + "hasanth_tables");
				System.out.println(line(" ", 10) + "2" + line(" ", 25) + "hasanth_columns");

				System.out.println(line("-", 80));

			} else {
				System.out.println(line("-", 80));
				System.out.println(line(" ", 10) + "row_id" + line(" ", 20) + "table name");
				System.out.println(line("-", 80));

				System.out.println(line(" ", 10) + "1" + line(" ", 25) + "hasanth_tables");
				System.out.println(line(" ", 10) + "2" + line(" ", 25) + "hasanth_columns");
				for (Integer s : hashMap.keySet()) {
					System.out.println(line(" ", 10) + s + line(" ", 25) + hashMap.get(s));
				}
				System.out.println(line("-", 80));

			}
			meta_tables.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void parseCreateTable(String createTableString) {

		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		// System.out.println(createTableTokens);
		Boolean flag = false;
		String tableFileName = createTableTokens.get(2) + ".tbl";

		try {

			File file = new File("data");
			String[] oldFiles = file.list();
			for (int i = 0; i < oldFiles.length; i++) {
				if (oldFiles[i].equals(tableFileName + ".tbl")) {
					System.out.println("Table already exists");
					flag = true;
				}
			}

			if (flag == false) {
				RandomAccessFile tableFile = new RandomAccessFile("data/" + tableFileName, "rw");
				tableFile.setLength(pageSize);
				tableFile.seek(0);

				tableFile.write(0x0D); // leaf page

				tableFile.write(0); // number of records

				tableFile.write(0); // start of content row name
				tableFile.write(0); // col name

				tableFile.write(0x0F); // next page pointer
				tableFile.write(0x0F);
				tableFile.write(0x0F);
				tableFile.write(0x0F);

				// record 1 pointer
				tableFile.close();

				String tableName = createTableTokens.get(2);

				String[] splitTable = createTableString.split(",");

				List<String> columnNames = new ArrayList<>();
				List<String> columnTypes = new ArrayList<>();
				List<String> columnNullable = new ArrayList<>();

				// for (int i = 0; i < splitTable.length; i++) {
				// System.out.print(splitTable[i].split(" ").length);
				// for (int j = 0; j < splitTable[i].split(" ").length; j++) {
				// System.out.print(splitTable[i].split(" ")[j] + ",");
				// }
				// System.out.println();
				// }

				for (int i = 0; i < splitTable.length; i++) {
					if (i == 0) {
						columnNames.add(splitTable[i].split(" ")[4]);
						columnTypes.add(splitTable[i].split(" ")[5]);

						if (splitTable[i].split(" ")[7].toUpperCase().contains("PRIMARY")
								|| splitTable[i].split(" ")[7].toUpperCase().contains("KEY")) {
							columnNullable.add("PRI");
						} else if (splitTable[i].split(" ")[7].toUpperCase() == "NOT") {
							columnNullable.add("NO");
						} else {
							columnNullable.add("YES");
						}

					} else {
						columnNames.add(splitTable[i].split(" ")[2]);
						columnTypes.add(splitTable[i].split(" ")[3]);

						if (splitTable[i].split(" ").length == 4) {
							columnNullable.add("YES");
						} else if (splitTable[i].split(" ").length > 4) {
							if (splitTable[i].split(" ")[3].toUpperCase().contains("NOT")
									|| splitTable[i].split(" ")[4].toUpperCase().contains("NOT")) {
								columnNullable.add("NO");
							} else {
								columnNullable.add("YES");
							}
						}
					}
				}

				RandomAccessFile meta_tables = new RandomAccessFile("data/hasanth_tables.tbl", "rw");

				meta_tables.seek(0x08);
				Long pointer1 = Long.decode("0x08");
				Long location = Long.decode("0x08");

				int loc = 0;

				while (true) {
					if (!meta_tables.readBoolean()) {
						if (location == 8) {
							break;
						} else {
							meta_tables.seek(location - 1);
							loc = Integer.valueOf(meta_tables.readShort());
							break;
						}
					} else {
						pointer1 = meta_tables.getFilePointer();
						location = pointer1;
						pointer1 += 1;
						meta_tables.seek(pointer1);
					}
				}

				int lengthOfRecordBefore = 0;

				if (loc == 0) {
					lengthOfRecordBefore = 0;
				} else {
					lengthOfRecordBefore = loc;
				}

				int recordLocation = 0;
				if (lengthOfRecordBefore == 0) {
					recordLocation = 512 - tableName.length() - 6 - lengthOfRecordBefore;
				} else {
					recordLocation = lengthOfRecordBefore - tableName.length() - 6;
				}

				meta_tables.seek(recordLocation);

				int pos = (int) meta_tables.getFilePointer();

				meta_tables.write(tableName.length());
				meta_tables.write(columnNames.size());

				meta_tables.seek(0x01);
				int recordsSizeForRow_Id = meta_tables.read(); // for number of
																// records in
																// table

				meta_tables.seek(recordLocation + 2);
				meta_tables.writeInt(recordsSizeForRow_Id + 1);

				meta_tables.seek(recordLocation + 6);
				for (int i = 0; i < tableName.length(); i++) {
					meta_tables.write(tableName.charAt(i));
				}

				meta_tables.seek(0x01);
				int recordsSize = meta_tables.read(); // for number of records
														// in
														// table
				meta_tables.seek(0x01);
				meta_tables.write(recordsSize + 1);
				meta_tables.writeShort(pos);

				meta_tables.seek(0x08);
				Long pointer = Long.decode("0x08");

				while (true) {
					if (!meta_tables.readBoolean()) {
						meta_tables.seek(pointer);
						meta_tables.writeShort(pos);
						break;
					}
					pointer = meta_tables.getFilePointer();
					pointer += 1;
					meta_tables.seek(pointer);
				}
				//
				// System.out.println(columnNames);
				// System.out.println(columnTypes);
				// System.out.println(columnNullable);

				meta_tables.close();
				for (int i = 0; i < columnNames.size(); i++) {
					MetaColumnsTable(columnNames.get(i), columnTypes.get(i), i, recordsSizeForRow_Id + 1, tableName,
							columnNullable.get(i));
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}

	}

	private static int columnsSize = 0;
	private static String columnTable = "data/hasanth_columns.tbl";

	public static void MetaColumnsTable(String columnName, String type, int position, int row_id, String tableName,
			String isNullable) {

		RandomAccessFile meta_columns, meta_tables;
		try {
			meta_columns = new RandomAccessFile(columnTable, "rw");

			// meta_tables = new RandomAccessFile("data/hasanth_tables.tbl",
			// "rw");
			// meta_tables.seek(1);
			// meta_tables.close();

			meta_columns.seek(1);
			int count = meta_columns.read();

			if (count == 10) {
				columnsSize++;
				meta_columns.seek(4);
				meta_columns.writeInt(columnsSize);
				meta_columns.close();
				columnTable = "data/hasanth_columns" + columnsSize + ".tbl";
				meta_columns = new RandomAccessFile(columnTable, "rw");
				meta_columns.setLength(pageSize);
				meta_columns.seek(0);
				meta_columns.write(0x0D);
				meta_columns.write(0x00);

				meta_columns.write(0x00); // start of content
				meta_columns.write(0x00);

				meta_columns.write(0xFF); // last page
				meta_columns.write(0xFF);
				meta_columns.write(0xFF);
				meta_columns.write(0xFF);

			}

			meta_columns.seek(0x08);
			Long pointer2 = Long.decode("0x08");
			Long location1 = Long.decode("0x08");
			int loc1 = 0;

			while (true) {
				if (!meta_columns.readBoolean()) {
					if (location1 == 8) {
						break;
					} else {
						// System.out.println(location1);
						meta_columns.seek(location1 - 1);
						loc1 = Integer.valueOf(meta_columns.readShort());
						break;
					}
				} else {
					pointer2 = meta_columns.getFilePointer();
					location1 = pointer2;
					pointer2 += 1;
					meta_columns.seek(pointer2);
				}
			}

			int lengthOfRecordBefore1 = 0;

			if (loc1 == 0) {
				lengthOfRecordBefore1 = 0;
			} else {
				lengthOfRecordBefore1 = loc1;
			}
			// System.out.println(lengthOfRecordBefore1);
			int recordLocation1 = 0;
			if (lengthOfRecordBefore1 == 0) {
				recordLocation1 = 512 - row_id - tableName.length() - 7 - columnName.length() - type.length() - position
						- 3 - lengthOfRecordBefore1;
			} else {
				recordLocation1 = lengthOfRecordBefore1 - row_id - tableName.length() - 7 - columnName.length()
						- type.length() - position - 3 - 1;
			}

			// System.out.println(recordLocation1);

			meta_columns.seek(recordLocation1);

			int pos1 = (int) meta_columns.getFilePointer();

			meta_columns.write(columnName.length());

			meta_columns.seek(0x01);
			int recordsSizeForRow_Id1 = meta_columns.read(); // for number of
																// records in
			meta_columns.seek(recordLocation1 + 1);
			meta_columns.writeInt(recordsSizeForRow_Id1 + 1);
			meta_columns.writeInt(row_id);
			for (int i = 0; i < tableName.length(); i++) {
				meta_columns.write(tableName.charAt(i));
			}
			for (int i = 0; i < columnName.length(); i++) {
				meta_columns.write(columnName.charAt(i));
			}
			String[] datatypes = getBytesForType(type);
			for (int i = 0; i < datatypes[1].length(); i++) {
				meta_columns.write(datatypes[1].charAt(i));
			}

			if (position == 0) {

				for (int i = 0; i < isNullable.length(); i++) {
					meta_columns.write(isNullable.charAt(i));
				}

			}
			if (position != 0) {
				for (int i = 0; i < isNullable.length(); i++) {
					meta_columns.write(isNullable.charAt(i));
				}
			}

			meta_columns.seek(0x01);
			int recordsSize1 = meta_columns.read(); // for number of records in
													// table
			meta_columns.seek(0x01);
			meta_columns.write(recordsSize1 + 1);
			meta_columns.writeShort(pos1);

			meta_columns.seek(0x08);
			Long pointer3 = Long.decode("0x08");

			while (true) {
				if (!meta_columns.readBoolean()) {
					meta_columns.seek(pointer3);
					meta_columns.writeShort(pos1);
					break;
				}
				pointer3 = meta_columns.getFilePointer();
				pointer3 += 1;
				meta_columns.seek(pointer3);
			}

			meta_columns.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static String[] getBytesForType(String type) {

		int a = 0;
		String serial_type_code = "0x00";

		switch (type.toUpperCase()) {

		case "TINYINT":
			a = 1;
			serial_type_code = "0x04";
			break;
		case "SMALLINT":
			a = 2;
			serial_type_code = "0x05";
			break;
		case "INT":
			a = 4;
			serial_type_code = "0x06";
			break;
		case "BIGINT":
			a = 8;
			serial_type_code = "0x07";
			break;
		case "REAL":
			a = 4;
			serial_type_code = "0x08";
			break;
		case "DOUBLE":
			a = 8;
			serial_type_code = "0x09";
			break;
		case "DATETIME":
			a = 8;
			serial_type_code = "0x0A";
			break;
		case "DATE":
			a = 8;
			serial_type_code = "0x0B";
			break;
		case "TEXT":
			serial_type_code = "0x0C";
			break;

		}

		String[] result = new String[2];
		result[0] = String.valueOf(a);
		result[1] = serial_type_code;

		return result;
	}

	public static String getTypeForBytes(String type) {

		String returntype = null;

		switch (type) {

		case "0x04":
			returntype = "TINYINT";
			break;
		case "0x05":
			returntype = "SMALLINT";
			break;
		case "0x06":

			returntype = "INT";
			break;
		case "0x07":
			returntype = "BIGINT";
			break;
		case "0x08":
			returntype = "REAL";
			break;
		case "0x09":

			returntype = "DOUBLE";
			break;
		case "0x0A":
			returntype = "DATETIME";
			break;
		case "0x0B":
			returntype = "DATE";
			break;
		case "0x0C":
			returntype = "TEXT";
			break;

		}

		return returntype;
	}

}
