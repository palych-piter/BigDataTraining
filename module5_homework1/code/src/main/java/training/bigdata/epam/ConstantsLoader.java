package training.bigdata.epam;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class ConstantsLoader {
    public static final class Constants {
        public static String dateFormatOutput = "yyyy-MM-dd HH:mm:ss.S";
        public static String dateFormatInput = "HH-dd-MM-yyyy";

        public static String bidSchema = "MotelID BidDate HU UK NL US MX AU CA CN KR BE I JP IN HN GY DE";
        public static String hotelSchema = "MotelID MotelName Country URL Comment";
        public static String rateSchema = "ValidFrom CurrencyName CurrencyCode ExchangeRate";
    }
}
