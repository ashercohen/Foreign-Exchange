package hw1_data_prep;

/**
 * Created by Asher Cohen asherc@andrew
 */

//enum representing the currencies pairs
public enum Currencies {
    AUDJPY,
    AUDNZD,
    AUDUSD,
    CADJPY,
    CHFJPY,
    EURCHF,
    EURGBP,
    EURJPY,
//    EURUSD,
    GBPJPY,
    GBPUSD,
    NZDUSD,
    USDCAD,
    USDCHF,
    USDJPY,
    DUMMY;

    public static Currencies safeValueOf(String s) {
        try {
            return Currencies.valueOf(s);
        }
        catch(IllegalArgumentException iae) {
            System.err.println(iae.getMessage());
        }

        return DUMMY;
    }
}
