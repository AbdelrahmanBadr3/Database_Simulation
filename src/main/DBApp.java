package main;

import java.io.*;
import java.util.*;

public class DBApp {
    private static ArrayList<String> tableNames;
    private static ArrayList<Integer> numberofColunms;
    private static ArrayList<ArrayList<String>> columnNames;
    private static ArrayList<ArrayList<String>> columnTypes;
    private static ArrayList<ArrayList<Boolean>> isKey;
    private static ArrayList<ArrayList<Boolean>> isIndexed;
    private static Hashtable<String, Table> myDB;
    private static Hashtable<String, Integer> myDB1;
    private static ArrayList<String> types;
    private final static int N = 3;

    private static String delSpace(String s) {
        String safter = "";
        for (int i = 0; i < s.length(); i++)
            if ((s.charAt(i) - ' ') != 0)
                safter += s.charAt(i);
        return safter;
    }

    public void init() {
        tableNames = new ArrayList<String>();
        numberofColunms = new ArrayList<Integer>();
        columnNames = new ArrayList<ArrayList<String>>();
        columnTypes = new ArrayList<ArrayList<String>>();
        isKey = new ArrayList<ArrayList<Boolean>>();
        isIndexed = new ArrayList<ArrayList<Boolean>>();
        myDB = new Hashtable<String, Table>();
        types = new ArrayList<String>();
        types.add("java.lang.Integer");
        types.add("java.lang.String");
        types.add("java.lang.Double");
        types.add("java.lang.Boolean");
        types.add("java.util.Date");
    }

    public void readFile1(String path) throws IOException {
        //read the meta data file
        myDB1 = new Hashtable<String, Integer>();

        FileReader fileReader = new FileReader(path);

        BufferedReader br = new BufferedReader(fileReader);
        String currentLine = "";
        while ((currentLine = br.readLine()) != null && currentLine.length() != 0) {
            // System.out.println(currentLine.length());
            String[] Line = currentLine.split(",");
            myDB1.put(Line[0], Integer.parseInt(Line[1]));
        }
        fileReader.close();
        br.close();
    }

    public void updatatablefile(String path,String name,int lastPage) throws IOException {
        //read the meta data file
       // myDB1 = new Hashtable<String, Integer>();
        FileReader fileReader = new FileReader(path);
        //create stringBuilder to insert into the metadata file

        StringBuilder sbuilder1= new StringBuilder();

        //sbuilder1.append(tableData );


        BufferedReader br = new BufferedReader( (fileReader));
        String currentLine = "";
        while ((currentLine = br.readLine()) != null && currentLine.length() != 0) {
            // trim newline when comparing with lineToRemove
           // String trimmedLine = currentLine.trim();
            if(currentLine.split(",")[0].equals(name)){
                sbuilder1.append(name+","+(lastPage+1)+ "\n");
                System.out.println("entered");

            }
            else
                sbuilder1.append(currentLine+ "\n" );

        }
        File f=new File("table.csv");
        f.delete();
        PrintWriter pw1 = null;
        try {
            pw1 = new PrintWriter(new File("table.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(sbuilder1.toString());
        pw1.write(sbuilder1.toString());
        pw1.close();
        //fileReader.close();
        br.close();
    }

    public void readFile(String path) throws IOException {
        //read the meta data file
        FileReader fileReader = new FileReader(path);
        BufferedReader br = new BufferedReader(fileReader);
        //intialize the values of the arraylist
        init();
        //fill  the arrays with data info.,
        String currentLine = "";
        while ((currentLine = br.readLine()) != null && currentLine.length() != 0) {
            // System.out.println(currentLine.length());
            String[] Line = currentLine.split(",");
            if (tableNames.contains(Line[0])) {
                // System.out.println("e");
                numberofColunms.add(tableNames.indexOf(Line[0]), numberofColunms.remove(tableNames.indexOf(Line[0])) + 1);
                columnNames.get(tableNames.indexOf(Line[0])).add(Line[1]);
                columnTypes.get(tableNames.indexOf(Line[0])).add(Line[2]);
                isKey.get(tableNames.indexOf(Line[0])).add(Boolean.parseBoolean(Line[3]));
                isIndexed.get(tableNames.indexOf(Line[0])).add(Boolean.parseBoolean(Line[4]));
                //System.out.println(numberofColunms.get(tableNames.indexOf(Line[0])));
            } else {
                if (numberofColunms.size() > 0)
                    System.out.println(numberofColunms.get(numberofColunms.size() - 1));
                tableNames.add(Line[0]);
                numberofColunms.add(tableNames.indexOf(Line[0]), 1);
                columnNames.add(new ArrayList<String>());
                columnNames.get(tableNames.indexOf(Line[0])).add(Line[1]);
                columnTypes.add(new ArrayList<String>());
                columnTypes.get(tableNames.indexOf(Line[0])).add(Line[2]);
                isKey.add(new ArrayList<Boolean>());
                isKey.get(tableNames.indexOf(Line[0])).add(Boolean.parseBoolean(Line[3]));
                isIndexed.add(new ArrayList<Boolean>());
                isIndexed.get(tableNames.indexOf(Line[0])).add(Boolean.parseBoolean(Line[4]));
            }
            // System.out.println(currentLine);

            // System.out.println(Line[0] + " " + Line[1] + " " + Line[2] + " " + Line[3] + " " + Line[4]);
        }
        //   System.out.println(numberofColunms.size());
        //   System.out.println(numberofColunms.get(numberofColunms.size() - 1));
    }

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException, IOException {
        //create newdir to the table if not exists if exists throw the Exception
        File newFolder = new File("DBTables\\" + strTableName);
        if (newFolder.exists())
            throw new DBAppException("This table is already exist");
        newFolder.mkdir();

        //read current data
        readFile("metadata.csv");
        readFile1("table.csv");


        //create stringBuilder to insert into the metadata file
        PrintWriter pw1 = null;
        try {
            pw1 = new PrintWriter(new File("table.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sbuilder1 = new StringBuilder();
        String tableData = strTableName + ",1";
        sbuilder1.append(tableData + "\n");
        pw1.write(sbuilder1.toString());
        pw1.close();

        //create stringBuilder to insert into the metadata file
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(new File("metadata.csv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        StringBuilder sbuilder = new StringBuilder();
        String headerOfTable = "TableName,ColumnName,ColumnType,Key,Indexed";
        sbuilder.append(headerOfTable + "\n");
        Set<String> keys = htblColNameType.keySet();
        String tableinfo = "";
        for (String key : keys) {
            if (types.contains(htblColNameType.get(key)))
                if (key.equalsIgnoreCase(strClusteringKeyColumn))
                    sbuilder.append(strTableName + "," + key + "," + htblColNameType.get(key) + "," + "True,False" + "\n");
                else
                    sbuilder.append(strTableName + "," + key + "," + htblColNameType.get(key) + "," + "False,False" + "\n");
            else
                throw new DBAppException("Your dataType is not valid");
            tableinfo += key + ":" + htblColNameType.get(key) + ",";

        }
        pw.write(sbuilder.toString());
        pw.close();

        myDB.put(strTableName, new Table(tableinfo, strTableName, N, strClusteringKeyColumn));

    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        String defultPath = "DBTables\\" + strTableName+"\\";
        File newFolder = new File("DBTables\\" + strTableName);
        if (!newFolder.exists())
            throw new DBAppException(strTableName + "is not Exist !");
        readFile1("table.csv");

        int LastPageid = myDB1.get(strTableName);

        int nOfNotFound = 0;
        int maxlength = 0;
        ArrayList<Object> pageData = null;
        String pageKey = "";
        ArrayList<Object> insertedRow=null;
        boolean inserted = false;

        l:
        for (int i = 0; i < LastPageid|| insertedRow!=null; i++) {

            System.out.println(i);
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(defultPath + i);
            } catch (FileNotFoundException e) {
                System.out.println("ewfew");
                new Page(maxlength, i, (String) pageData.get(0), pageKey, strTableName);
                updatatablefile("table.csv",strTableName,i);
                i--;

                continue l;

            }
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Vector<ArrayList<Object>> tempVector = (Vector<ArrayList<Object>>) in.readObject();
            System.out.println(tempVector);
            pageData = tempVector.get(0);
            String[] dataInfo = ((String) (pageData.get(0))).split(",");
            String[] setOfKeys = new String[dataInfo.length];
            for (int j = 0; j < dataInfo.length; j++) {
                setOfKeys[j] = dataInfo[j].split(":")[0];
            }
            if (!inserted) {
                insertedRow = new ArrayList<Object>();
                for (int j = 0; j < setOfKeys.length; j++)
                    insertedRow.add(htblColNameValue.get(setOfKeys[j]));
                insertedRow.add(Calendar.getInstance().getTime());
            }
            pageKey = (String) pageData.get(1);
            int indexofKey = getIndex(pageKey, dataInfo);
            int currlength = (Integer) pageData.get(2);
            maxlength = (Integer) pageData.get(3);
           // Object minValue = pageData.get(4);
            Object maxValue = pageData.get(5);

            if (maxValue==null||greaterThan(htblColNameValue.get(pageKey), maxValue)) {
                if(maxValue==null){
                    tempVector.add(insertedRow);
                    ArrayList<Object> firstRow1 = tempVector.get(1);
                    ArrayList<Object> lastRow1 = tempVector.get(tempVector.size() - 1);
                    pageData.remove(2);
                    pageData.add(2, tempVector.size() - 1);
                    pageData.remove(4);
                    pageData.add(4, firstRow1.get(indexofKey));
                    pageData.remove(5);
                    pageData.add(5, lastRow1.get(indexofKey));
                    tempVector.remove(0);
                    tempVector.add(0, pageData);
                    // (currTable.pages.get(currTable.idofCurrPage())).svector = tempVector;
                    System.out.println(tempVector);
                    FileOutputStream fileOut1 = new FileOutputStream(defultPath + i);
                    ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
                    out1.writeObject(tempVector);
                    out1.close();
                    fileOut1.close();
                    break l;
                }
                for (int k = 1; k < tempVector.size(); k++) {
                    ArrayList<Object> dataRow = (ArrayList<Object>) tempVector.get(k);
                    if (greaterThan(htblColNameValue.get(pageKey), dataRow.get(indexofKey))) {
                        inserted = true;
                        tempVector.add(k, insertedRow);
                        if (currlength + 1 > maxlength) {

                            insertedRow = tempVector.get(tempVector.size() - 1);
                            tempVector.remove(tempVector.size() - 1);
                            ArrayList<Object> firstRow1 = tempVector.get(1);
                            ArrayList<Object> lastRow1 = tempVector.get(tempVector.size() - 1);
                            pageData.remove(2);
                            pageData.add(2, tempVector.size() - 1);
                            pageData.remove(4);
                            pageData.add(4, firstRow1.get(indexofKey));
                            pageData.remove(5);
                            pageData.add(5, lastRow1.get(indexofKey));
                            tempVector.remove(0);
                            tempVector.add(0, pageData);
                            // (currTable.pages.get(currTable.idofCurrPage())).svector = tempVector;
                            System.out.println(tempVector);
                            FileOutputStream fileOut1 = new FileOutputStream(defultPath + i);
                            ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
                            out1.writeObject(tempVector);
                            out1.close();
                            fileOut1.close();
                            continue l;
                        } else {
                            System.out.println((currlength + 2)+"entered"+maxlength);
                            ArrayList<Object> firstRow1 = tempVector.get(1);
                            ArrayList<Object> lastRow1 = tempVector.get(tempVector.size() - 1);
                            pageData.remove(2);
                            pageData.add(2, tempVector.size() - 1);
                            pageData.remove(4);
                            pageData.add(4, firstRow1.get(indexofKey));
                            pageData.remove(5);
                            pageData.add(5, lastRow1.get(indexofKey));
                            tempVector.remove(0);
                            tempVector.add(0, pageData);
                            // (currTable.pages.get(currTable.idofCurrPage())).svector = tempVector;
                            System.out.println(tempVector);
                            FileOutputStream fileOut1 = new FileOutputStream(defultPath + i);
                            ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
                            out1.writeObject(tempVector);
                            out1.close();
                            fileOut1.close();
                            break l;
                        }
                    }


                }
            } else {
                if (currlength + 1 <= maxlength) {
                    tempVector.add(currlength + 1, insertedRow);
                    ArrayList<Object> firstRow1 = tempVector.get(1);
                    ArrayList<Object> lastRow1 = tempVector.get(tempVector.size() - 1);
                    pageData.remove(2);
                    pageData.add(2, tempVector.size() - 1);
                    pageData.remove(4);
                    pageData.add(4, firstRow1.get(indexofKey));
                    pageData.remove(5);
                    pageData.add(5, lastRow1.get(indexofKey));
                    tempVector.remove(0);
                    tempVector.add(0, pageData);
                    // (currTable.pages.get(currTable.idofCurrPage())).svector = tempVector;
                    System.out.println(tempVector);
                    FileOutputStream fileOut1 = new FileOutputStream(defultPath + i);
                    ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
                    out1.writeObject(tempVector);
                    out1.close();
                    fileOut1.close();
                    break l;
                }
            }
            continue;


        }
    }


    public boolean greaterThan(Object o0, Object o1) {
        if (o0 instanceof Integer) {
            if ((Integer) o0 < (Integer) o1)
                return true;

            return false;
        } else {
            if (o0 instanceof Double) {
                if ((Double) o0 < (Double) o1)
                    return true;
                return false;

            } else {
                if (o0 instanceof String) {
                    if ((((String) o0).compareTo((String) o1) == -1))
                        return true;
                    return false;
                } else {
                    if (o0 instanceof Date) {
                        if ((((Date) o0).compareTo((Date) o1) == -1))
                            return true;
                        return false;
                    }
                    return false;
                }
            }
        }
    }

    public boolean equals(Object o0, Object o1) {
        if (o0 instanceof Integer) {
            if ((((Integer) o0).intValue()) == ((Integer) o1).intValue())
                return true;
            return false;
        } else {
            if (o0 instanceof Double) {
                if (((Double) o0).doubleValue() == ((Double) o1).doubleValue())
                    return true;
                return false;

            } else {
                if (o0 instanceof String) {
                    if ((((String) o0).compareTo((String) o1) == 0))
                        return true;
                    return false;
                } else {
                    if (o0 instanceof Date) {
                        if ((((Date) o0).compareTo((Date) o1) == 0))
                            return true;
                        return false;
                    }
                    return false;
                }
            }
        }
    }

    public int getIndex(String key, String[] dataInfo) {
        for (int i = 0; i < dataInfo.length; i++)
            if (dataInfo[i].contains(key))
                return i;
        return -1;
    }


    public void updateTable(String strTableName, Object strKey, Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {
        //  Table currTable = myDB.get(strTableName);

        String defultPath = "DBTables\\" + strTableName + "\\";
        int nOfNotFound = 0;
        l:
        for (int i = 0; ; i++) {

            System.out.println(i);
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(defultPath + i);
            } catch (FileNotFoundException e) {
                nOfNotFound++;
                if (nOfNotFound == 30) {
                    throw new DBAppException("I Can not Found ur tuple");
                }
                continue;

            }
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Vector<ArrayList<Object>> tempVector = (Vector<ArrayList<Object>>) in.readObject();
            System.out.println(tempVector);
            ArrayList<Object> pageData = tempVector.get(0);
            String[] dataInfo = ((String) (pageData.get(0))).split(",");
            //String[] setOfKeys = new String[dataInfo.length];
//                for (int j = 0; j < dataInfo.length; j++) {
//                    setOfKeys[j] = dataInfo[j].split(":")[0];
//                }
            String pageKey = (String) pageData.get(1);
            int indexofKey = getIndex(pageKey, dataInfo);
//                int currlength = (Integer) pageData.get(2);
//                int maxlength = (Integer) pageData.get(3);
            //   Object minValue = pageData.get(4);
            Object maxValue = pageData.get(5);
            if ((greaterThan(strKey, maxValue) || equals(strKey, maxValue))) {
                for (int k = 1; k < tempVector.size(); k++) {

                    ArrayList<Object> dataRow = (ArrayList<Object>) tempVector.get(k);
                    if (equals(strKey, dataRow.get(indexofKey))) {

                        Set<String> keys = htblColNameValue.keySet();
                        for (String key : keys) {
                            int indexofupKey = getIndex(key, dataInfo);
                            dataRow.remove(indexofupKey);
                            dataRow.add(indexofupKey, htblColNameValue.get(key));

                        }
                        dataRow.remove(dataRow.size() - 1);
                        dataRow.add(Calendar.getInstance().getTime());
                        tempVector.remove(k);
                        tempVector.add(k, dataRow);
                        ArrayList<Object> firstRow1 = tempVector.get(1);
                        ArrayList<Object> lastRow1 = tempVector.get(tempVector.size() - 1);
                        pageData.remove(2);
                        pageData.add(2, tempVector.size() - 1);
                        pageData.remove(4);
                        pageData.add(4, firstRow1.get(indexofKey));
                        pageData.remove(5);
                        pageData.add(5, lastRow1.get(indexofKey));
                        tempVector.remove(0);
                        tempVector.add(0, pageData);
                        // (currTable.pages.get(currTable.idofCurrPage())).svector = tempVector;
                        System.out.println(tempVector);
                        FileOutputStream fileOut1 = new FileOutputStream(defultPath + i);
                        ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
                        out1.writeObject(tempVector);
                        out1.close();
                        fileOut1.close();
                        break l;
                    }


                }
            }
            continue;


        }

    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, ClassNotFoundException, IOException {

        String defultPath = "DBTables\\" + strTableName + "\\";
        int nOfNotFound = 0;
        l:
        for (int i = 0; ; i++) {
            //System.out.println(i);
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(defultPath + i);
            } catch (FileNotFoundException e) {
                nOfNotFound++;
                if (nOfNotFound == 30) {
                    System.out.println("delete is done ");
                    break l;
                }
                continue;

            }
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Vector<ArrayList<Object>> tempVector = (Vector<ArrayList<Object>>) in.readObject();
            System.out.println(tempVector);
            ArrayList<Object> pageData = tempVector.get(0);
            String[] dataInfo = ((String) (pageData.get(0))).split(",");
            String pageKey = (String) pageData.get(1);
            int indexofKey = getIndex(pageKey, dataInfo);
            for (int k = 1; k < tempVector.size(); k++) {
                ArrayList<Object> dataRow = (ArrayList<Object>) tempVector.get(k);
                Set<String> keys = htblColNameValue.keySet();
                boolean matches = true;
                for (String key : keys) {
                    int indexofupKey = getIndex(key, dataInfo);
                    if (!equals(dataRow.get(indexofupKey), htblColNameValue.get(key))) {
                        matches = false;
                        break;
                    }
                }
                if (matches && keys.size() != 0)
                    tempVector.remove(k);
                // tempVector.add(k, dataRow);
            }

            if (tempVector.size() - 1 == 0) {
                fileIn.close();
                in.close();
                System.out.println(tempVector);
                File file = new File(defultPath + i);
                System.out.println(file.delete() + " " + i);
            } else {
                // (currTable.pages.get(currTable.idofCurrPage())).svector = tempVector;
                ArrayList<Object> firstRow1 = tempVector.get(1);
                ArrayList<Object> lastRow1 = tempVector.get(tempVector.size() - 1);
                pageData.remove(2);
                pageData.add(2, tempVector.size() - 1);
                pageData.remove(4);
                pageData.add(4, firstRow1.get(indexofKey));
                pageData.remove(5);
                pageData.add(5, lastRow1.get(indexofKey));
                tempVector.remove(0);
                tempVector.add(0, pageData);
                System.out.println(tempVector);
                FileOutputStream fileOut1 = new FileOutputStream(defultPath + i);
                //  Files.delete(defultPath + i);
                ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
                out1.writeObject(tempVector);
                out1.close();
                fileOut1.close();
            }

        }
    }

    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
        System.out.println(Calendar.getInstance().getTime());
        File newFolder = new File("DBTables");
        boolean c = newFolder.mkdir();
        System.out.println(newFolder.exists());
////
        DBApp myDB = new DBApp();
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        //System.out.println(htblColNameType.get("id"));
        myDB.createTable(strTableName, "id", htblColNameType);
////
        Hashtable htblColNameValue = new Hashtable();
        htblColNameValue.put("id", new Integer(2343432));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));
        myDB.insertIntoTable(strTableName, htblColNameValue);
        htblColNameValue.clear();
//
        htblColNameValue.put("id", new Integer(453455));
        htblColNameValue.put("name", new String("Ahmed Noor"));
        htblColNameValue.put("gpa", new Double(0.95));
        myDB.insertIntoTable(strTableName, htblColNameValue);
////
        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(5674567));
        htblColNameValue.put("name", new String("Dalia Noor"));
        htblColNameValue.put("gpa", new Double(1.25));
        myDB.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(23498));
        htblColNameValue.put("name", new String("John Noor"));
        htblColNameValue.put("gpa", new Double(1.5));
        myDB.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
                htblColNameValue.put("id", new Integer(78452));
        htblColNameValue.put("name", new String("Zaky Noor"));
        htblColNameValue.put("gpa", new Double(0.88));
        myDB.insertIntoTable(strTableName, htblColNameValue);

        htblColNameValue.clear();
        htblColNameValue.put("id", new Integer(4534555));
        htblColNameValue.put("name", new String("ali1"));
        htblColNameValue.put("gpa", new Double(0.88));
        myDB.insertIntoTable(strTableName, htblColNameValue);
//
        String defultPath = "DBTables\\" + strTableName + "\\";

        FileInputStream fileIn = null;

        fileIn = new FileInputStream(defultPath + 0);


        ObjectInputStream in = new ObjectInputStream(fileIn);
        Vector<ArrayList<Object>> tempVector = (Vector<ArrayList<Object>>) in.readObject();
        System.out.println(tempVector);
         defultPath = "DBTables\\" + strTableName + "\\";

         fileIn = null;

        fileIn = new FileInputStream(defultPath +1);


         in = new ObjectInputStream(fileIn);
         tempVector = (Vector<ArrayList<Object>>) in.readObject();
        System.out.println(tempVector);
    }


}

