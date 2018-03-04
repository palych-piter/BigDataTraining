package training.bigdata.epam;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MapperOutputWritable implements Writable {

        private Text operatingSystem; //Stores a word
        private IntWritable biddingPrice; //stores the count

        //null arguments YOU NEED THIS so hadoop can init the writable
        public MapperOutputWritable(){
            setOperatingSystem("");
            setBiddingPrice(0);
        }

        public MapperOutputWritable(String operatingSystem, String biddingPrice){
            setOperatingSystem(operatingSystem);
            setBiddingPrice(Integer.parseInt(biddingPrice));

        }

        public MapperOutputWritable(String operatingSystem, int biddingPrice) {
            setOperatingSystem(operatingSystem);
            setBiddingPrice(biddingPrice);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            operatingSystem.readFields(in);
            biddingPrice.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            operatingSystem.write(out);
            biddingPrice.write(out);
        }

        public int getBiddingPrice() {
            return biddingPrice.get();
        }
        public void setBiddingPrice(int v1) {
            this.biddingPrice = new IntWritable(v1);
        }
        public String getOperatingSystem() {
            return operatingSystem.toString();
        }
        public void setOperatingSystem(String operatingSystem) {
            this.operatingSystem = new Text(operatingSystem);
        }


}







