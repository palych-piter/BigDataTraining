package training.bigdata.epam;

import java.io.Serializable;

public class EnrichedItem implements Serializable {

    String date;
    String motelId;
    String loSa;
    Double price;


    public EnrichedItem(String date, String motelId, String loSa, Double price){
        this.date = date;
        this.motelId = motelId;
        this.loSa = loSa;
        this.price = price;
    }


    public String getDate() {
        return date;
    }

    public String getMotelId() {
        return motelId;
    }

    public String getLoSa() {
        return loSa;
    }

    public Double getPrice() {
        return price;
    }

}
