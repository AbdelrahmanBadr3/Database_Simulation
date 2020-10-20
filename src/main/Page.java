package main;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Page implements Serializable {
    public int       maxlength;
    public int       currlength;
    public Object    minvalue;
    public Object    maxvalue;
    public int       id;
    public String    data;
    FileOutputStream fos;
    Vector<ArrayList<Object>> svector;
    public  String clustaerKey;
//    (maxRaw, pages.size(), tableInfo,clustaerKey);

    public Page(int maxlength, int id, String data, String clustaerKey, String strTableName) throws IOException {
        this.fos=new FileOutputStream("DBTables\\" + strTableName + "\\" + id);
        this.svector = new Vector<ArrayList<Object>>();
        this.id=id;
        this.data=data;
        this.currlength = 0;
        this.maxlength=maxlength;
        this.clustaerKey=clustaerKey;
        ArrayList<Object> firstLine= new ArrayList<Object>();
        firstLine.add(data);
        firstLine.add(clustaerKey);
        firstLine.add(currlength);
        firstLine.add(maxlength);
        firstLine.add(minvalue);
        firstLine.add(maxvalue);
        svector.add(firstLine);
        ObjectOutputStream out1 = new ObjectOutputStream(fos);
        out1.writeObject(svector);
        out1.close();
        fos.close();
    }


//    public void addtopage(ArrayList<Object> s) throws IOException {
//        System.out.println(s);
//        svector.add(s);
//        os.writeObject(svector);
//        //os.close();
//
//    }


}
