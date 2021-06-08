package RECORDMANAGER;

import RECORDMANAGER.TableRow;

import java.io.Serializable;
import java.util.Vector;

public class ReturnData implements Serializable {
    public boolean isSuccess;
    public String info;
    public Vector<TableRow> returnData;

    public ReturnData(boolean isSuccess, String info){
        this.isSuccess = isSuccess;
        this.info = info;
        this.returnData = new Vector<>();
    }

    public ReturnData(boolean isSuccess, String info, Vector<TableRow> returnData){
        this.isSuccess = isSuccess;
        this.info = info;
        this.returnData = returnData;
    }

    public void setInfo(String info){
        this.info = info;
    }

    public void setReturnData(Vector<TableRow> returnData) {
        this.returnData = returnData;
    }

    public void setSuccess(boolean isSuccess){
        this.isSuccess = isSuccess;
    }
}
