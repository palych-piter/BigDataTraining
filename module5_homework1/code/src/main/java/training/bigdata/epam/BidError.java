package training.bigdata.epam;

import java.io.Serializable;

public class BidError implements Serializable {

    String date;
    String errorMessage;
    Integer count;

    public void setDate(String date) {
        this.date = date;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setCount(Integer count) {
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


