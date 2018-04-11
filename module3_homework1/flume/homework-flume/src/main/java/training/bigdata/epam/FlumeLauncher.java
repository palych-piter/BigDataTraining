package training.bigdata.epam;

import org.apache.flume.node.Application;

public class FlumeLauncher {

    public static void main(String[] args) {

        //System.setProperty("log4j.configuration", "file:/flume/config/log4j.properties");

        Application.main(new String[]{
                "-f", "/root/bgtraining/aux/flume/flume.conf",
                "-n", "flume-agent"
        });

    }
}