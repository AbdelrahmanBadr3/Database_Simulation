package main;

import java.io.IOException;
import java.util.ArrayList;

public class Table {
    transient   ArrayList<Page> pages ;
    String      tableInfo ;
    String      name ;
    int         maxRaw;
    Page        currPage;
    Page 		firstPage;
    int lastPage=0;
    String clustaerKey;
    public Table(String tableInfo, String name, int maxRaw,String clustaerKey) throws IOException {
        this.tableInfo = tableInfo;
        this.name = name;
        this.maxRaw = maxRaw;
        this.clustaerKey=clustaerKey;
        this.pages= new ArrayList<Page>();
        Page p = new Page(maxRaw,0, tableInfo,clustaerKey,name);
        currPage = p;
        firstPage=p;
        pages.add(p);
    }
    public  String getcurrPage(){
        return "DBTables\\" +name+"\\"+currPage.id;
    }
    public  String beInNextPage()throws IOException{
        if((pages.indexOf(currPage)+1)-pages.size()==0){
            Page p = new Page(maxRaw,lastPage++ , tableInfo,clustaerKey,name);
            pages.add(p);
            currPage = p;
            return "DBTables\\" +name+"\\"+currPage.id;
        }
        currPage = pages.get((pages.indexOf(currPage)+1));
        return "DBTables\\" +name+"\\"+currPage.id;
        //pages.add(p);

    }
    public int idofCurrPage(){
        return currPage.id;
    }


}
