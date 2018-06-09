package training.bigdata.epam;

import java.io.Serializable;

public class EnrichedItem implements Serializable {

    String date;
    String motelName;
    String loSa;
    Double price;
    String hotelName;


    public EnrichedItem(String date, String motelId, String loSa, Double price, String hotelName){
        this.date = date;
        this.motelName = motelId;
        this.loSa = loSa;
        this.price = price;
        this.hotelName = hotelName;
    }


    public String getDate() {
        return date;
    }

    public String getMotelId() {
        return motelName;
    }

    public String getLoSa() {
        return loSa;
    }

    public Double getPrice() {
        return price;
    }

    public String getHotelName() {
        return hotelName;
    }


}
