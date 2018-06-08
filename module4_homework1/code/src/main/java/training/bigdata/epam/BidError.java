package training.bigdata.epam;

import java.io.Serializable;

public class BidError implements Serializable {

    String date;
    String errorMessage;
    Integer count;

    public BidError(String date, String errorMessage, Integer count){
        this.date = date;
        this.errorMessage = errorMessage;
        this.count = count;
    }

    public String getDate() {
        return date;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Integer getCount() {
        return count;
    }

}


