package water;

import water.util.Log;


public class HangApp extends H2OStarter {
    public static void main(String[] args) {

        if (H2O.checkUnsupportedJava())
            System.exit(1);

        start(args, System.getProperty("user.dir"));

        // wait for cluster of size 2
        H2O.waitForCloudSize(2, 10000);

        System.out.println("Before");
        // Renumber to handle dup names
        if (DKV.get("prostate.hex") == null) {
            Log.info("All ok");
        }else{
            throw new RuntimeException("Should not be in:");
        }
        System.out.println("After");

    }

}
