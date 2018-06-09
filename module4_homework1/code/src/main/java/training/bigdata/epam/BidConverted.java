package training.bigdata.epam;

import java.io.Serializable;

public class BidConverted implements Serializable {


    String date;
    Integer motelId;
    String loSa;
    Double price;


    public BidConverted(String date, Integer motelId, String loSa, Double price){
        this.date = date;
        this.motelId = motelId;
        this.loSa = loSa;
        this.price = price;
    }


    public String getDate() {
        return date;
    }

    public Integer getMotelId() {
        return motelId;
    }

    public String getLoSa() {
        return loSa;
    }

    public Double getPrice() {
        return price;
    }

}
